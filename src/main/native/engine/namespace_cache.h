#pragma once

#include "allocator.h"
#include "hot_cache.h"

#include <array>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <mutex>
#include <unordered_set>
#include <vector>

namespace forl0 {

struct NamespaceCacheFull : std::bad_alloc {
    NamespaceCacheFull(size_t requested_bytes, size_t requested_alignment)
        : bytes(requested_bytes), alignment(requested_alignment) {}
    size_t bytes;
    size_t alignment;
};

// StateMap can keep small payloads contiguous in either L0 resource.
class L0PayloadAllocator : public Allocator {};

// Each state keeps a stable primary span and can lease additional empty spans
// from the process pool. A state owns its tables and all their allocations.
class NamespaceCacheArena final : public L0PayloadAllocator {
    struct Span;
public:
    NamespaceCacheArena(std::shared_ptr<HotCacheManager> manager, size_t requested_bytes,
                        Allocator* fallback = &DefaultAllocator::instance())
        : manager_(std::move(manager)), fallback_(fallback) {
        const size_t requested = std::min(requested_bytes, manager_->capacity_bytes()) / sizeof(HotSet);
        // Many Flink state instances register before a single table is populated.
        // On a shared pool, obtain the balance of the configured share on actual growth.
        const size_t minimum_sets = (64 * 1024 + sizeof(HotSet) - 1) / sizeof(HotSet);
        const bool shared = requested && manager_->total_sets() / requested >= 4;
        const size_t initial = shared
                ? std::min(requested, std::max(minimum_sets, (requested + 3) / 4)) : requested;
        install(spans_.front(), manager_->acquire_namespace_span(static_cast<uint32_t>(initial)));
        target_capacity_ = initial < requested ? requested * sizeof(HotSet) : primary_capacity();
    }

    ~NamespaceCacheArena() override {
        release_payload_slack();
        for (auto& span : spans_) manager_->release_ll(span.lease);
    }

    NamespaceCacheArena(const NamespaceCacheArena&) = delete;
    NamespaceCacheArena& operator=(const NamespaceCacheArena&) = delete;

    const void* pool_base() const { return manager_->data(); }
    const void* data() const { return spans_.front().base; }
    size_t primary_capacity() const { return spans_.front().capacity; }
    bool contains_primary(const void* ptr) const { return spans_.front().contains(ptr); }
    size_t capacity() const { return capacity_; }
    bool has_backing_space(size_t bytes, size_t alignment = 64) {
        return has_gap(allocation_bytes(bytes), alignment);
    }

    // Optional metadata may only take genuinely free pool bytes. Do not
    // borrow, spill to DRAM, or displace an authoritative table for it.
    void* try_allocate_metadata(size_t bytes, size_t alignment) {
        return alignment <= 64 ? try_allocate(bytes, alignment) : nullptr;
    }

    void* try_allocate_primary_payload(size_t size, size_t alignment) {
        if (alignment > 64) return nullptr;
        return try_allocate_in_span(spans_.front(), allocation_bytes(size), std::max(alignment, quantum));
    }

    using ReclaimIndexes = bool (*)(void*, size_t, size_t);
    void set_index_reclaimer(void* owner, ReclaimIndexes reclaim) noexcept {
        index_owner_ = owner;
        reclaim_indexes_ = reclaim;
    }

    size_t free_bytes() const { return free_bytes_; }
    bool in_mutation() const { return spill_ != nullptr; }
    bool mutating(const bool& owner) const { return spill_ == &owner; }
    void* mutation_owner() const noexcept { return mutation_owner_; }
    size_t payload_run_slack() const { return run_slack_; }
    uint64_t payload_run_allocations() const { return run_allocations_; }
    uint64_t payload_runs_reserved() const { return runs_reserved_; }
    uint64_t recovered_resizes() const { return recovered_resizes_; }
    uint64_t recycled_allocations() const { return recycled_allocations_; }

    void forget_payload_owner(const bool* owner) noexcept {
        const auto id = reinterpret_cast<uintptr_t>(owner);
        auto& run = payload_run(id);
        if (run.owner == id) {
            release_run(run);
            run = {};
        } else if (run.challenger == id) {
            run.challenger = 0;
        }
    }

    void release_payload_slack() noexcept {
        if (run_slack_) for (auto& run : runs_) release_run(run);
    }

    HotCacheLL* backing() const { return spans_.front().lease; }

    size_t maximum_capacity() const {
        return std::max(target_capacity_,
            std::min(4 * target_capacity_, manager_->capacity_bytes() / 4));
    }

    size_t maximum_table_capacity() const {
        return std::max(maximum_capacity(),
                std::min(16 * target_capacity_, manager_->capacity_bytes() / 2));
    }

    bool has_table_loan() const { return table_loans_ != 0; }
    size_t peak_table_loans() const { return peak_table_loans_; }

    bool try_borrow_whole_table(size_t bytes) {
        if (!bytes || bytes > maximum_table_capacity()) return false;
        if (bytes <= maximum_capacity()) {
            const size_t ordinary_room = maximum_capacity() - std::min(capacity_, maximum_capacity());
            if (bytes < primary_capacity() || bytes <= free_bytes_ + ordinary_room) return false;
        }
        const size_t sets = 1 + (bytes - 1) / sizeof(HotSet);
        if (sets > maximum_table_capacity() / sizeof(HotSet)) return false;
        // Fill the dedicated large-table span first, then reuse empty lease
        // descriptors. A failed relocation must not acquire a second empty span.
        Span* available = nullptr;
        for (size_t i = spans_.size() - 1; i > 0; --i) {
            auto& span = spans_[i];
            if (span.table_loan && span.available > span.capacity / 2) return false;
            if (!span.lease && !available) available = &span;
        }
        if (!available) return false;
        const auto protected_sets = std::max(backing()->num_sets(), manager_->total_sets() / 4);
        if (!install(*available, manager_->borrow_namespace_span(static_cast<uint32_t>(sets), protected_sets))) return false;
        available->table_loan = true;
        peak_table_loans_ = std::max(peak_table_loans_, ++table_loans_);
        return true;
    }

    bool contains(const void* ptr) const {
        for (const auto& span : spans_) {
            if (span.contains(ptr)) return true;
        }
        return false;
    }

    static constexpr size_t largest_cached_block() { return bin_count * quantum; }

    static size_t allocation_bytes(size_t size) {
        return align_up(std::max(size, size_t(1)), quantum);
    }

    bool has_unclaimed_share() const { return capacity_ < target_capacity_; }

    // The initial span stays at a stable address. A table that exhausts it can
    // finish its original share in one additional span, subject to the process reserve.
    bool try_complete_share(size_t bytes, size_t backing_bytes) {
        if (!has_unclaimed_share() || spans_[1].lease) return false;
        const size_t balance = target_capacity_ - capacity_;
        if (bytes > free_bytes_ + balance || backing_bytes > balance) return false;
        const uint32_t sets = static_cast<uint32_t>(balance / sizeof(HotSet));
        const uint32_t reserved = std::max(backing()->num_sets(), manager_->total_sets() / 4);
        return install(spans_[1], manager_->borrow_namespace_span(sets, reserved));
    }

    bool try_borrow(size_t bytes, size_t backing_bytes, size_t alignment = 64) {
        if (capacity_ >= maximum_capacity()) return false;
        const size_t room = maximum_capacity() - capacity_;
        const size_t deficit = bytes > free_bytes_ ? bytes - free_bytes_ : 0;
        const size_t contiguous = has_gap(allocation_bytes(backing_bytes), alignment) ? 0 : backing_bytes;
        const size_t needed = std::max(deficit, contiguous);
        if (!needed) return false;
        const size_t sets = 1 + (needed - 1) / sizeof(HotSet);
        if (sets > room / sizeof(HotSet)) return false;
        const auto half_primary = primary_capacity() / (2 * sizeof(HotSet));
        const size_t room_sets = room / sizeof(HotSet);
        const auto protected_sets = std::max(backing()->num_sets(), manager_->total_sets() / 4);
        auto try_lease = [&](Span& span, size_t count) {
            return install(span, manager_->borrow_namespace_span(static_cast<uint32_t>(count), protected_sets));
        };
        // Keep the last lease available for a proven, larger whole table.
        for (auto it = spans_.begin() + 1; it != spans_.end() - 1; ++it) {
            if (it->lease) continue;
            const size_t standard = half_primary <= room_sets ? std::max(sets, half_primary) : sets;
            if (it != spans_.begin() + 1) {
                // The first loan is already occupied. Let the last span
                // cover sustained demand up to this arena's existing cap.
                size_t expanded = standard;
                while (expanded <= room_sets / 2) expanded *= 2;
                if (expanded > standard && try_lease(*it, expanded)) return true;
            }
            if (try_lease(*it, standard)) return true;
            return standard > sets && try_lease(*it, sets);
        }
        return false;
    }

    void release_empty_loans() {
        for (auto it = spans_.begin() + 1; it != spans_.end(); ++it) {
            if (!it->lease || it->available != it->capacity) continue;
            capacity_ -= it->capacity;
            free_bytes_ -= it->capacity;
            if (it->table_loan) --table_loans_;
            manager_->release_ll(it->lease);
            *it = Span{};
        }
    }

    // Let a resident mutation finish once, even when a new allocation
    // falls back to DRAM. The table is moved at the mutation boundary.
    // Retiring a complete table can release thousands of independently owned rows.
    // Accumulate their exact bitmap changes before the next aligned gap search.
    class ReleaseBatch {
    public:
        explicit ReleaseBatch(NamespaceCacheArena& arena) noexcept
            : arena_(arena), previous_(arena.batch_releases_) { arena_.batch_releases_ = true; }
        ~ReleaseBatch() { arena_.batch_releases_ = previous_; }
        ReleaseBatch(const ReleaseBatch&) = delete;
        ReleaseBatch& operator=(const ReleaseBatch&) = delete;
    private:
        NamespaceCacheArena& arena_;
        bool previous_;
    };

    class Mutation {
    public:
        Mutation(NamespaceCacheArena& arena, bool& spill, void* owner = nullptr)
            : arena_(arena), previous_(arena.spill_), previous_owner_(arena.mutation_owner_) {
            arena_.spill_ = &spill;
            arena_.mutation_owner_ = owner;
        }
        ~Mutation() {
            arena_.mutation_owner_ = previous_owner_;
            arena_.spill_ = previous_;
        }
        Mutation(const Mutation&) = delete;
        Mutation& operator=(const Mutation&) = delete;
    private:
        NamespaceCacheArena& arena_;
        bool* previous_;
        void* previous_owner_;
    };

    // Copy a table linearly into one free range when possible. Individual
    // allocations still use the normal arena ownership and can be freed or
    // resized independently after the copy.
    class Packing {
    public:
        Packing(NamespaceCacheArena& arena, size_t bytes, size_t alignment = 64)
            : arena_(arena), previous_(arena.packing_) {
            if (!bytes) return;
            capacity_ = allocation_bytes(bytes);
            base_ = static_cast<char*>(arena_.try_allocate(capacity_, alignment));
            if (base_) arena_.packing_ = this;
        }
        ~Packing() {
            if (!base_) return;
            arena_.packing_ = previous_;
            if (payload_) arena_.deallocate(payload_, payload_size_);
            if (used_ < capacity_) arena_.deallocate(base_ + used_, capacity_ - used_);
        }
        Packing(const Packing&) = delete;
        Packing& operator=(const Packing&) = delete;

        bool active() const { return base_ != nullptr; }

        // Protect a single known allocation in a fragmented arena. Other
        // rows cannot consume its gap before the copy reaches this payload.
        bool reserve_payload(size_t bytes, size_t alignment) {
            assert(base_ && !payload_size_ && bytes);
            payload_size_ = allocation_bytes(bytes);
            payload_ = arena_.try_allocate(payload_size_, alignment);
            return payload_ != nullptr;
        }

        bool used_payload() const { return payload_size_ && !payload_; }

    private:
        friend class NamespaceCacheArena;

        void* take(size_t bytes, size_t alignment) {
            const uintptr_t address = reinterpret_cast<uintptr_t>(base_) + used_;
            const size_t start = used_ + ((-address) & (alignment - 1));
            if (start > capacity_ || bytes > capacity_ - start) {
                if (payload_ && bytes == payload_size_
                        && (reinterpret_cast<uintptr_t>(payload_) & (alignment - 1)) == 0) {
                    void* result = payload_;
                    payload_ = nullptr;
                    return result;
                }
                return nullptr;
            }
            if (start != used_) arena_.deallocate(base_ + used_, start - used_);
            used_ = start + bytes;
            return base_ + start;
        }

        NamespaceCacheArena& arena_;
        Packing* previous_;
        char* base_ = nullptr;
        size_t capacity_ = 0;
        size_t used_ = 0;
        void* payload_ = nullptr;
        size_t payload_size_ = 0;
    };

    void* allocate(size_t size, size_t alignment) override {
        if (alignment > 64 || (spill_ && *spill_)) return spill(size, alignment);
        if (packing_) {
            if (auto* block = packing_->take(allocation_bytes(size), std::max(alignment, quantum))) return block;
        } else if (spill_ && alignment <= quantum && size <= 256 && primary_capacity() >= 16384) {
            const size_t bytes = allocation_bytes(size);
            // The primary free list can hand off an exact block and preserve the namespace run.
            if (auto* block = take_recycled(spans_.front(), bytes, quantum)) {
                if (recycle_backoff_) --recycle_backoff_;
                return block;
            }
            if (auto* block = take_payload(bytes)) return block;
        }
        if (auto* block = try_allocate(size, alignment)) return block;
        if (run_slack_) {
            release_payload_slack();
            if (auto* block = try_allocate(size, alignment)) return block;
        }
        if (spill_ && reclaim_indexes_ && reclaim_indexes_(index_owner_, allocation_bytes(size), alignment)) {
            if (auto* block = try_allocate(size, alignment)) return block;
        }
        return spill(size, alignment);
    }

private:
    // A short run belongs to the stable namespace table header. Its consumed
    // bytes are ordinary individually owned PMR allocations. Only the unused
    // suffix is kept here, and any table can reclaim it under pressure.
    struct PayloadRun {
        uintptr_t owner = 0, challenger = 0;
        char* next = nullptr;
        size_t remaining = 0;
        uint8_t requests = 0;
    };

    PayloadRun& payload_run(uintptr_t owner) noexcept {
        return runs_[((owner >> 4) ^ (owner >> 11)) & (runs_.size() - 1)];
    }

    void release_run(PayloadRun& run) noexcept {
        if (!run.remaining) return;
        deallocate(run.next, run.remaining);
        run_slack_ -= run.remaining;
        run.remaining = 0;
        run.next = nullptr;
    }

    void* take_payload(size_t bytes) {
        const auto owner = reinterpret_cast<uintptr_t>(spill_);
        auto& run = payload_run(owner);
        if (run.owner != owner) {
            if (run.owner && run.challenger != owner) {
                run.challenger = owner;
                return nullptr;
            }
            release_run(run);
            run = {};
            run.owner = owner;
        }
        run.challenger = 0;
        if (run.requests < 7) {
            ++run.requests;
            return nullptr;
        }
        if (run.remaining < bytes) {
            const size_t chunk = allocation_bytes(std::min(size_t{1024}, primary_capacity() / 128));
            const size_t extent = std::max(chunk, bytes * 2);
            if (free_bytes_ < 8 * extent ||
                    run_slack_ - run.remaining + extent > primary_capacity() / 32) return nullptr;
            auto* block = static_cast<char*>(try_allocate(extent, quantum));
            if (!block) return nullptr;
            release_run(run);
            run.next = block;
            run.remaining = extent;
            run_slack_ += extent;
            ++runs_reserved_;
        }
        auto* block = run.next;
        run.next += bytes;
        run.remaining -= bytes;
        run_slack_ -= bytes;
        ++run_allocations_;
        return block;
    }

    void* take_recycled(Span& span, size_t bytes, size_t alignment) noexcept {
        if (!span.reusable(bytes, alignment)) return nullptr;
        auto* block = span.pop(bytes / quantum - 1);
        span.available -= bytes;
        free_bytes_ -= bytes;
        ++recycled_allocations_;
        return block;
    }

    void* try_allocate(size_t size, size_t alignment) {
        const size_t bytes = allocation_bytes(size);
        if (recycle_backoff_) --recycle_backoff_;
        if (bytes > free_bytes_) return nullptr;
        const size_t align = std::max(alignment, quantum);
        for (auto& span : spans_) {
            if (auto* block = try_allocate_in_span(span, bytes, align)) return block;
        }
        return nullptr;
    }

    void* try_allocate_in_span(Span& span, size_t bytes, size_t align) {
        if (bytes > span.available) return nullptr;
        if (auto* block = take_recycled(span, bytes, align)) return block;
        const size_t start = span.find_gap(bytes, align);
        if (start == span.space.units()) return nullptr;
        span.space.take(start, bytes / quantum);
        span.available -= bytes;
        free_bytes_ -= bytes;
        return span.base + start * quantum;
    }

public:
    void deallocate(void* ptr, size_t size) noexcept override {
        if (!ptr) return;
        for (auto& span : spans_) {
            if (!span.contains(ptr)) continue;
            const size_t begin = static_cast<char*>(ptr) - span.base;
            const size_t bytes = allocation_bytes(size);
            if (batch_releases_ || (recycle_backoff_ && span.available < span.capacity / 4)
                    || !span.recycle(static_cast<char*>(ptr), bytes)) {
                span.space.give(size_t(begin) / quantum, bytes / quantum, batch_releases_);
            }
            span.available += bytes;
            free_bytes_ += bytes;
            return;
        }
        if (recoverable_backing_ == ptr) recoverable_backing_ = nullptr;
        fallback_->deallocate(ptr, size);
    }

    SplitResult allocate_split(size_t ctrl_size, size_t ctrl_align,
                               size_t slots_size, size_t slots_align) override {
        const size_t offset = align_up(ctrl_size, slots_align);
        const bool clean = spill_ && !*spill_;
        auto* block = static_cast<char*>(allocate(offset + slots_size, std::max(ctrl_align, slots_align)));
        const bool temporary = clean && *spill_;
        if (temporary) {
            recoverable_backing_ = block;
            recoverable_owner_ = spill_;
        }
        return {block, block + offset, false, temporary};
    }

    SplitResult recover_split(const SplitResult& previous, size_t ctrl_size, size_t ctrl_align,
                              size_t slots_size, size_t slots_align) override {
        if (!previous.temporary || previous.ctrl_ptr != recoverable_backing_ || !spill_ || spill_ != recoverable_owner_
                || !*spill_ || std::max(ctrl_align, slots_align) > 64) return {nullptr, nullptr, false};
        const size_t offset = align_up(ctrl_size, slots_align);
        auto* block = static_cast<char*>(try_allocate(offset + slots_size, std::max(ctrl_align, slots_align)));
        if (!block) return {nullptr, nullptr, false};
        // This backing was the only fallback allocation in the mutation.
        // A second spilled payload invalidates the token in spill().
        recoverable_backing_ = nullptr;
        *spill_ = false;
        ++recovered_resizes_;
        return {block, block + offset, false};
    }

    void deallocate_split(const SplitResult& allocation, size_t, size_t slots_size) noexcept override {
        const size_t offset = static_cast<char*>(allocation.slots_ptr) - static_cast<char*>(allocation.ctrl_ptr);
        deallocate(allocation.ctrl_ptr, offset + slots_size);
    }

private:
    static constexpr size_t quantum = 16;
    static constexpr size_t bin_count = 32;
    // One leaf describes 64 allocation quanta. Each branch records the largest
    // free run and both boundary runs, so a fragmented span can reject an
    // impossible request before visiting any individual holes.
    class FreeQuanta {
        struct Runs {
            size_t prefix = 0, suffix = 0, longest = 0;

            bool operator==(const Runs& other) const noexcept {
                return prefix == other.prefix && suffix == other.suffix && longest == other.longest;
            }
        };

    public:
        void reset(size_t units) {
            units_ = units;
            cursor_ = 0;
            recent_.fill(0);
            free_.assign((units + 63) / 64, ~uint64_t{0});
            if (units % 64) free_.back() = low_bits(units % 64);
            leaves_ = 1;
            while (leaves_ < free_.size()) leaves_ *= 2;
            runs_.assign(2 * leaves_, {});
            for (size_t i = 0; i < free_.size(); ++i) runs_[leaves_ + i] = summarize(free_[i]);
            for (size_t level = leaves_ / 2, width = 64; level; level /= 2, width *= 2) {
                for (size_t i = level; i < 2 * level; ++i) join(i, width);
            }
        }

        size_t units() const noexcept { return units_; }

        size_t find(size_t count, size_t align) noexcept {
            if (!count || count > units_) return units_;
            if (runs_[1].longest == dirty) refresh(1, leaves_ * 64);
            if (count > runs_[1].longest) return units_;
            if (count <= bin_count && recent_[count - 1]) {
                const size_t pos = recent_[count - 1] - 1;
                if (!(pos & (align - 1)) && is_free(pos, count)) return pos;
            }
            const size_t next = align_up(cursor_, align);
            if (next <= units_ - count && is_free(next, count)) return next;
            return find_in(1, 0, leaves_ * 64, count, align);
        }

        void take(size_t pos, size_t count) noexcept {
            if (count <= bin_count && recent_[count - 1] == pos + 1) {
                recent_[count - 1] = 0;
            } else {
                cursor_ = pos + count == units_ ? 0 : pos + count;
            }
            mark<false>(pos, count);
        }

        void give(size_t pos, size_t count, bool deferred = false) noexcept {
            if (deferred || runs_[1].longest == dirty) mark<true, true>(pos, count);
            else mark<true>(pos, count);
            if (count <= bin_count) recent_[count - 1] = pos + 1;
        }

    private:
        static uint64_t low_bits(size_t count) noexcept {
            return count == 64 ? ~uint64_t{0} : (uint64_t{1} << count) - 1;
        }

        static Runs summarize(uint64_t bits) noexcept {
            if (!bits) return {};
            if (!~bits) return {64, 64, 64};
            const size_t prefix = __builtin_ctzll(~bits);
            const size_t suffix = __builtin_clzll(~bits);
            size_t longest = std::max(prefix, suffix);
            // Boundary runs are already accounted for. Inspect only the
            // remaining internal runs, skipping a complete run in one step.
            bits &= ~low_bits(prefix);
            bits &= low_bits(64 - suffix);
            while (bits) {
                const size_t start = __builtin_ctzll(bits);
                const size_t length = __builtin_ctzll(~(bits >> start));
                longest = std::max(longest, length);
                bits &= ~low_bits(start + length);
            }
            return {prefix, suffix, longest};
        }

        bool join(size_t at, size_t half) noexcept {
            const auto& a = runs_[2 * at];
            const auto& b = runs_[2 * at + 1];
            Runs result{a.prefix == half ? half + b.prefix : a.prefix,
                        b.suffix == half ? half + a.suffix : b.suffix,
                        std::max({a.longest, b.longest, a.suffix + b.prefix})};
            if (runs_[at] == result) return false;
            runs_[at] = result;
            return true;
        }

        bool is_free(size_t pos, size_t count) const noexcept {
            while (count) {
                const size_t offset = pos % 64;
                const size_t length = std::min(count, 64 - offset);
                const uint64_t mask = low_bits(length) << offset;
                if ((free_[pos / 64] & mask) != mask) return false;
                pos += length;
                count -= length;
            }
            return true;
        }

        size_t find_in(size_t node, size_t base, size_t width, size_t count, size_t align) const noexcept {
            const auto& here = runs_[node];
            if (here.longest < count) return units_;
            const size_t aligned = align_up(base, align);
            if (aligned - base + count <= here.prefix) return aligned;
            if (width == 64) {
                uint64_t starts = free_[base / 64];
                for (size_t length = 1; length < count;) {
                    const size_t step = std::min(length, count - length);
                    starts &= starts >> step;
                    length += step;
                }
                while (starts) {
                    const size_t pos = align_up(base + __builtin_ctzll(starts), align);
                    if (pos >= base + 64) break;
                    const uint64_t bit = uint64_t{1} << (pos - base);
                    if (starts & bit) return pos;
                    starts &= ~(bit - 1);
                }
                return units_;
            }
            const size_t half = width / 2, middle = base + half;
            const size_t left = find_in(2 * node, base, half, count, align);
            if (left != units_) return left;
            const size_t across = align_up(middle - runs_[2 * node].suffix, align);
            if (across + count <= middle + runs_[2 * node + 1].prefix) return across;
            return find_in(2 * node + 1, middle, half, count, align);
        }

        // The bitmap always describes exact ownership. Shared ancestors are marked
        // once for an entire batch. No side allocation is needed to record dirty leaves.
        static constexpr size_t dirty = std::numeric_limits<size_t>::max();

        void refresh(size_t node, size_t width) noexcept {
            if (runs_[node].longest != dirty) return;
            if (node >= leaves_) {
                runs_[node] = summarize(free_[node - leaves_]);
                return;
            }
            const size_t half = width / 2;
            refresh(2 * node, half);
            refresh(2 * node + 1, half);
            join(node, half);
        }

        template<bool release, bool deferred = false>
        void mark(size_t pos, size_t count) noexcept {
            size_t first = leaves_ + pos / 64;
            size_t last = leaves_ + (pos + count - 1) / 64;
            bool changed = false;
            while (count) {
                const size_t word = pos / 64, offset = pos % 64;
                const size_t length = std::min(count, 64 - offset);
                const uint64_t mask = low_bits(length) << offset;
                auto& bits = free_[word];
                if constexpr (release) {
                    assert(!(bits & mask));
                    bits |= mask;
                } else {
                    assert((bits & mask) == mask);
                    bits &= ~mask;
                }
                if constexpr (deferred) {
                    for (size_t node = leaves_ + word; node && runs_[node].longest != dirty; node /= 2)
                        runs_[node].longest = dirty;
                } else {
                    const Runs leaf = summarize(bits);
                    if (!(runs_[leaves_ + word] == leaf)) {
                        runs_[leaves_ + word] = leaf;
                        changed = true;
                    }
                }
                pos += length;
                count -= length;
            }
            for (size_t half = 64; changed && first > 1; half *= 2) {
                first /= 2;
                last /= 2;
                changed = false;
                for (size_t i = first; i <= last; ++i) changed |= join(i, half);
            }
        }

        size_t units_ = 0, cursor_ = 0, leaves_ = 0;
        std::vector<uint64_t> free_;
        std::vector<Runs> runs_;
        std::array<size_t, bin_count> recent_{};
    };

    struct Span {
        HotCacheLL* lease = nullptr;
        bool table_loan = false;
        char* base = nullptr;
        size_t capacity = 0;
        size_t available = 0;
        FreeQuanta space;
        // A released, frequently sized block can pass straight to its next
        // owner without changing the bitmap or its interval summaries twice.
        // Only freed storage holds links. A gap search can coalesce it all.
        static constexpr size_t small_bins = 16;
        std::array<char*, small_bins> heads{};
        std::array<uint8_t, small_bins> depths{};
        size_t recycled_bytes = 0;

        bool reusable(size_t bytes, size_t align) const noexcept {
            if (!recycled_bytes || bytes > small_bins * quantum) return false;
            const auto* head = heads[bytes / quantum - 1];
            return head && !(reinterpret_cast<uintptr_t>(head) & (align - 1));
        }

        bool recycle(char* block, size_t bytes) noexcept {
            if (bytes > small_bins * quantum || recycled_bytes + bytes > capacity / 32) return false;
            const size_t bin = bytes / quantum - 1;
            if (depths[bin] == 8) return false;
            std::memcpy(block, &heads[bin], sizeof(char*));
            heads[bin] = block;
            ++depths[bin];
            recycled_bytes += bytes;
            return true;
        }

        char* pop(size_t bin) noexcept {
            char* head = heads[bin];
            std::memcpy(&heads[bin], head, sizeof(char*));
            --depths[bin];
            recycled_bytes -= (bin + 1) * quantum;
            return head;
        }

        size_t find_gap(size_t bytes, size_t align) noexcept {
            const size_t count = bytes / quantum;
            const size_t pos = space.find(count, align / quantum);
            if (pos != space.units() || !recycled_bytes) return pos;
            for (size_t bin = 0; bin < small_bins; ++bin) {
                while (heads[bin]) space.give(size_t(pop(bin) - base) / quantum, bin + 1);
            }
            return space.find(count, align / quantum);
        }

        bool contains(const void* ptr) const {
            const auto address = reinterpret_cast<uintptr_t>(ptr);
            const auto start = reinterpret_cast<uintptr_t>(base);
            return address >= start && address - start < capacity;
        }
    };

    bool install(Span& span, std::unique_ptr<HotCacheLL> lease) {
        if (!lease) return false;
        const size_t bytes = size_t(lease->num_sets()) * sizeof(HotSet);
        try {
            span.space.reset(bytes / quantum);
        } catch (...) {
            manager_->release_ll(lease.release());
            throw;
        }
        span.lease = lease.release();
        span.base = reinterpret_cast<char*>(span.lease->sets());
        span.capacity = span.available = bytes;
        capacity_ += bytes;
        free_bytes_ += bytes;
        return true;
    }

    bool has_gap(size_t bytes, size_t alignment) noexcept {
        const size_t align = std::max(alignment, quantum);
        for (auto& span : spans_) {
            if (bytes <= span.available && (span.reusable(bytes, align)
                    || span.find_gap(bytes, align) != span.space.units())) return true;
        }
        return false;
    }

    void* spill(size_t size, size_t alignment) {
        if (!spill_) throw NamespaceCacheFull(size, alignment);
        auto* result = fallback_->allocate(size, alignment);
        // When a live mutation outgrows the arena, leave contiguous space
        // visible to growing tables until allocation pressure subsides.
        if (alignment <= 64) recycle_backoff_ = 2048;
        recoverable_backing_ = nullptr;
        *spill_ = true;
        return result;
    }

    static size_t align_up(size_t size, size_t alignment) {
        return (size + alignment - 1) & ~(alignment - 1);
    }

    std::shared_ptr<HotCacheManager> manager_;
    Allocator* fallback_;
    bool* spill_ = nullptr;
    void* mutation_owner_ = nullptr;
    bool* recoverable_owner_ = nullptr;
    void* recoverable_backing_ = nullptr;
    uint64_t recovered_resizes_ = 0;
    uint64_t recycled_allocations_ = 0;
    uint16_t recycle_backoff_ = 0;
    bool batch_releases_ = false;
    void* index_owner_ = nullptr;
    ReclaimIndexes reclaim_indexes_ = nullptr;
    Packing* packing_ = nullptr;
    std::array<PayloadRun, 4> runs_{};
    size_t run_slack_ = 0;
    uint64_t run_allocations_ = 0, runs_reserved_ = 0;
    std::array<Span, 4> spans_;
    size_t table_loans_ = 0, peak_table_loans_ = 0;
    size_t capacity_ = 0, target_capacity_ = 0;
    size_t free_bytes_ = 0;
};

// Separately owned MapState entries can use L0 even when their VoidNamespace
// root does not fit. Growth can fall back to that root's ordinary allocator.
class RootMapPayloadAllocator final : public L0PayloadAllocator {
public:
    RootMapPayloadAllocator(NamespaceCacheArena& arena, Allocator& dram, size_t& resident_bytes)
        : arena_(arena), dram_(dram), resident_bytes_(resident_bytes) {}

    class Placement {
    public:
        explicit Placement(RootMapPayloadAllocator& resource) : resource_(resource) { resource_.placing_ = true; }
        ~Placement() { resource_.placing_ = false; }
        Placement(const Placement&) = delete;
        Placement& operator=(const Placement&) = delete;
    private:
        RootMapPayloadAllocator& resource_;
    };

    void* allocate(size_t size, size_t alignment) override {
        const size_t bytes = NamespaceCacheArena::allocation_bytes(size);
        if (bytes <= arena_.primary_capacity() / 16 - resident_bytes_) {
            if (auto* storage = arena_.try_allocate_primary_payload(size, alignment)) {
                resident_bytes_ += bytes;
                return storage;
            }
        }
        if (placing_) throw std::bad_alloc();
        return dram_.allocate(size, alignment);
    }

    void deallocate(void* ptr, size_t size) noexcept override {
        if (arena_.contains_primary(ptr)) {
            resident_bytes_ -= NamespaceCacheArena::allocation_bytes(size);
            arena_.deallocate(ptr, size);
        } else {
            dram_.deallocate(ptr, size);
        }
    }

private:
    NamespaceCacheArena& arena_;
    Allocator& dram_;
    size_t& resident_bytes_;
    bool placing_ = false;
};

// Published table descriptors never move while the Java task can read them.
// Use spare pool space for this small directory and keep the full table quota
// available on very small arenas or when a large directory would crowd out data.
class NamespaceControlResource final : public std::pmr::memory_resource {
public:
    void bind(NamespaceCacheArena* arena) noexcept { arena_ = arena; }
    size_t pooled_bytes() const noexcept { return pooled_bytes_; }

private:
    void* do_allocate(size_t bytes, size_t alignment) override {
        if (arena_ && bytes <= arena_->primary_capacity() / 32 && bytes <= arena_->free_bytes() / 8) {
            if (auto* ptr = arena_->try_allocate_metadata(bytes, alignment)) {
                pooled_bytes_ += NamespaceCacheArena::allocation_bytes(bytes);
                return ptr;
            }
        }
        return std::pmr::new_delete_resource()->allocate(bytes, alignment);
    }

    void do_deallocate(void* ptr, size_t bytes, size_t alignment) override {
        if (arena_ && arena_->contains(ptr)) {
            pooled_bytes_ -= NamespaceCacheArena::allocation_bytes(bytes);
            arena_->deallocate(ptr, bytes);
        } else {
            std::pmr::new_delete_resource()->deallocate(ptr, bytes, alignment);
        }
    }

    bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override { return this == &other; }
    NamespaceCacheArena* arena_ = nullptr;
    size_t pooled_bytes_ = 0;
};

// Track the size of one DRAM table in arena allocation units. This lets
// admission reject a table before traversing its rows or nested containers.
class NamespaceDramAllocator final : public Allocator {
    // A whole-table copy constructs adjacent slots in order. Pack their small,
    // separately owned payloads together in ordinary memory. A page can be
    // returned independently as keys expire or their values are replaced.
    struct CopyPages final : Allocator {
        static constexpr size_t page_bytes = 4096;
        static constexpr size_t max_payload = page_bytes / 4;
        struct Page { size_t used, live; };

        explicit CopyPages(NamespaceDramAllocator& owner)
                : owner(owner), upstream(owner.backing_), pages(upstream) {}

        void* allocate(size_t size, size_t alignment) override {
            if (copying) {
                if (void* packed = try_allocate(size, alignment)) return packed;
            }
            return upstream->allocate(size, alignment);
        }

        void deallocate(void* block, size_t size) override {
            if (!release(block, size)) upstream->deallocate(block, size);
        }

        void* try_allocate(size_t size, size_t alignment) {
            if (size > max_payload || alignment > 64) return nullptr;
            size = std::max(size, size_t{1});
            auto aligned = [alignment](size_t n) { return (n + alignment - 1) & ~(alignment - 1); };
            size_t offset = current ? aligned(current->used) : 0;
            if (!current || offset + size > page_bytes) {
                if (!can_grow) return nullptr;
                void* storage = nullptr;
                try {
                    storage = upstream->allocate(page_bytes, page_bytes);
                    pages.insert(reinterpret_cast<uintptr_t>(storage) / page_bytes);
                } catch (const std::bad_alloc&) {
                    if (storage) upstream->deallocate(storage, page_bytes);
                    // Remaining memory can still cover individual allocations.
                    can_grow = false;
                    return nullptr;
                }
                current = new (storage) Page{sizeof(Page), 0};
                offset = aligned(sizeof(Page));
            }
            ++current->live;
            current->used = offset + size;
            return reinterpret_cast<char*>(current) + offset;
        }

        bool release(void* ptr, size_t size) {
            if (size > max_payload) return false;
            const uintptr_t number = reinterpret_cast<uintptr_t>(ptr) / page_bytes;
            auto found = pages.find(number);
            if (found == pages.end()) return false;
            auto* page = reinterpret_cast<Page*>(number * page_bytes);
            if (--page->live == 0) {
                if (current == page) current = nullptr;
                pages.erase(found);
                upstream->deallocate(page, page_bytes);
                if (pages.empty() && !copying) {
                    // Ordinary allocations made alongside the pages belong to
                    // the upstream as well. Future releases can go directly there.
                    owner.backing_ = upstream;
                    std::pmr::unordered_set<uintptr_t> empty(upstream);
                    empty.swap(pages);
                }
            }
            return true;
        }

        NamespaceDramAllocator& owner;
        Allocator* upstream;
        std::pmr::unordered_set<uintptr_t> pages;
        Page* current = nullptr;
        bool copying = false;
        bool can_grow = true;
    };

public:
    explicit NamespaceDramAllocator(Allocator* backing) : backing_(backing) {}

    void* allocate(size_t size, size_t alignment) override {
        void* block = backing_->allocate(size, alignment);
        bytes_ += NamespaceCacheArena::allocation_bytes(size);
        return block;
    }

    void deallocate(void* block, size_t size) override {
        backing_->deallocate(block, size);
        bytes_ -= NamespaceCacheArena::allocation_bytes(size);
    }

    template<class Copy>
    void pack_copy(Copy&& copy) {
        if (!pages_) pages_.reset(new (std::nothrow) CopyPages(*this));
        if (!pages_) { copy(); return; }
        backing_ = pages_.get();
        pages_->copying = pages_->can_grow = true;
        try { copy(); }
        catch (...) { finish_copy(); throw; }
        finish_copy();
    }

    size_t bytes() const { return bytes_; }

private:
    void finish_copy() {
        pages_->copying = false;
        pages_->current = nullptr;
        if (pages_->pages.empty()) {
            backing_ = pages_->upstream;
            pages_.reset();
        }
    }
    Allocator* backing_;
    size_t bytes_ = 0;
    std::unique_ptr<CopyPages> pages_;
};

// A checkpoint visits one authoritative namespace table at a time. Mark the
// current slot of each changed key while its key-group lock is held. Groups
// of slots with no changes can then stream straight to the checkpoint writer.
class SnapshotSlotMarks {
public:
    size_t reserve(NamespaceCacheArena& arena) {
        std::unique_lock<SnapshotSlotMarks> guard(*this, std::try_to_lock);
        if (!guard) return 0;
        if (words_) return words_->size() * sizeof(uint64_t);
        const size_t limit = std::min({size_t{32768}, arena.primary_capacity() / 128,
                                       arena.free_bytes() / 8});
        if (limit < 64) return 0;
        size_t count = 8;
        while (count <= limit / (2 * sizeof(uint64_t))) count *= 2;
        try {
            words_ = std::make_unique<std::pmr::vector<uint64_t>>(count, uint64_t{}, &arena);
        } catch (const std::bad_alloc&) { return 0; }
        return words_->size() * sizeof(uint64_t);
    }

    bool release_if_idle() {
        std::unique_lock<SnapshotSlotMarks> guard(*this, std::try_to_lock);
        if (!guard || !words_) return false;
        words_.reset();
        return true;
    }

    template<class Table, class Changes, class Current, class Changed>
    bool visit(const Table& table, const Changes& changes, Current& current, Changed& changed) {
        if (table.size() < 128 || changes.size() > table.size() / 8) return false;
        std::unique_lock<SnapshotSlotMarks> guard(*this, std::try_to_lock);
        if (!guard || !words_ || changes.size() > words_->size() * 8) return false;
        const size_t count = std::min(words_->size(), (table.capacity() + 63) / 64);
        std::fill_n(words_->data(), count, uint64_t{0});
        for (const auto& entry : changes) {
            if (const auto* value = table.find(entry.first)) {
                const size_t slot = table.position_of(*value);
                (*words_)[(slot / 64) & (count - 1)] |= uint64_t{1} << (slot % 64);
            }
        }
        const size_t checked = table.for_each_marked(words_->data(), count - 1, current, changed);
        scans_.fetch_add(1, std::memory_order_relaxed);
        skips_.fetch_add(table.size() - checked, std::memory_order_relaxed);
        return true;
    }

    uint64_t scans() const { return scans_.load(std::memory_order_relaxed); }
    uint64_t skips() const { return skips_.load(std::memory_order_relaxed); }

    // A writer in another group, or a nested visitor, uses ordinary COW
    // lookup while the buffer is in use. Admission never waits for a scan.
    bool try_lock() noexcept { return !busy_.test_and_set(std::memory_order_acquire); }
    void unlock() noexcept { busy_.clear(std::memory_order_release); }

private:
    std::unique_ptr<std::pmr::vector<uint64_t>> words_;
    std::atomic_flag busy_ = ATOMIC_FLAG_INIT;
    std::atomic<uint64_t> scans_{0};
    std::atomic<uint64_t> skips_{0};
};

struct NamespaceCacheStats {
    uint64_t growth_loans = 0;
    size_t charged_root_copy_bytes = 0, dram_reuse_credit_spent = 0;
    size_t initial_share_expansions = 0, control_view_bytes = 0;
    uint64_t interleaved_growth_loans = 0, negative_growth_loans = 0;
    uint64_t resident_control_skips = 0;
    uint64_t repeated_root_reads = 0, repeated_root_writes = 0;
    uint64_t missing_namespace_reads = 0, missing_namespace_skips = 0, missing_namespace_busy = 0;
    uint64_t resident_slot_reads = 0, resident_slot_writes = 0;
    uint64_t paired_slot_updates = 0, paired_slot_inserts = 0;
    size_t map_edit_bytes = 0, map_edit_reuses = 0, map_edit_pauses = 0;
    size_t root_edit_hint_bytes = 0, root_edit_hint_hits = 0;
    size_t probe_index_bytes = 0, probe_indexes_built = 0, probe_indexes_released = 0;
    size_t initial_root_tables = 0;
    size_t initial_miss_root_deferrals = 0;
    size_t root_map_payload_bytes = 0;
    size_t root_map_payload_promotions = 0;
    size_t root_map_payload_evictions = 0;
    uint64_t recovered_resizes = 0;
    size_t payload_run_slack = 0;
    uint64_t payload_run_allocations = 0;
    uint64_t payload_runs_reserved = 0;
    size_t snapshot_index_bytes = 0;
    uint64_t snapshot_index_scans = 0;
    uint64_t snapshot_lookup_skips = 0;
    size_t idle_window_bytes = 0;
    size_t reused_windows = 0, idle_successors = 0, directory_node_reuses = 0;
    size_t learned_window_sizes = 0;
    size_t shared_window_reuses = 0;
    size_t batched_window_reuses = 0, singleton_windows = 0;
    size_t peak_table_loans = 0;
    size_t overlap_compactions = 0;
    size_t released_idle_windows = 0;
    size_t whole_table_loans = 0;
    size_t empty_root_returns = 0;
    size_t blocked_backings = 0;
    size_t blocked_payloads = 0;
    size_t reserved_payload_promotions = 0;
    size_t failed_relocations = 0;
    size_t resident_root_updates = 0;
    size_t dram_root_updates = 0;
    size_t membership_write_skips = 0;
    size_t prepared_writes = 0;
    size_t read_position_bytes = 0;
    size_t read_position_writes = 0;
    size_t read_position_pauses = 0;
    uint64_t shared_negative_reads = 0;
    size_t directory_bytes = 0;
    size_t directories_built = 0;
    size_t directories_released = 0;
    size_t memberships_built = 0;
    size_t memberships_released = 0;
    size_t membership_bytes = 0;
    size_t membership_bypassed = 0;
    size_t membership_probes = 0;
    size_t membership_skips = 0;
    uint64_t promotions = 0;
    uint64_t evictions = 0;
    uint64_t growth_demotions = 0, growth_reentry_deferrals = 0, growing_map_updates = 0;
    uint64_t rejected = 0;
    uint64_t admission_deferrals = 0;
    uint64_t resident_lookups = 0;
    uint64_t loans_granted = 0;
    uint64_t packed_promotions = 0;
    size_t resident_tables = 0;
    size_t resident_bytes = 0;
};

}  // namespace forl0
