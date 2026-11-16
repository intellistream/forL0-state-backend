#pragma once
#include "state_cache_access.h"
#include "flink_binary_format.h"

namespace forl0 {

// Each element has its own cache slot. Append never serializes or recopies the
// existing list. Read admission happens after consuming all cached elements:
// filling an old prefix during the scan must not evict its still-readable tail.
template<class Consumer>
void read_cached_list(MapCallCache& cache, const ElementList& elements,
                      size_t footprint, Consumer&& consume) {
    bool admit = cache.admit_list_read(footprint);
    if (!admit) {
        // The prefix directory already links every resident element. Walk it
        // once instead of hashing every cold element of an oversized list.
        // Borrowed views stay valid: this read performs no cache mutations.
        auto resident = cache.list_entries(elements.size());
        cache.record_hits(resident.size());
        cache.record_misses(elements.size() - resident.size());
        size_t next = 0;
        for (size_t i = 0; i < elements.size(); ++i) {
            if (next < resident.size() && resident[next].first == i) consume(i, resident[next++].second);
            else consume(i, std::string_view(elements[i]));
        }
        return;
    }
    bool present = cache.has_entries();
    std::vector<size_t> missed;
    if (!present) cache.record_misses(elements.size());
    for (size_t i = 0; i < elements.size(); ++i) {
        std::string_view bytes;
        if (!present || !cache.get(uint64_t(i), &bytes)) {
            bytes = elements[i];
            if (present && admit) {
                if (missed.empty()) missed.reserve(elements.size());
                missed.push_back(i);
            }
        }
        consume(i, bytes);
    }
    // Large lists cannot reside together; refilling them just churns the ring.
    if (admit) {
        if (!present) {
            for (size_t i = 0; i < elements.size(); ++i) cache.put(uint64_t(i), elements[i]);
        } else {
            for (size_t i : missed) cache.put(uint64_t(i), elements[i]);
        }
    }
}

template<class K, class... Args>
jbyteArray cached_list_get(JNIEnv* env, StateHandle* h,
                          StateTable<K, ElementList>* table, Args&&... args) {
    auto* elements = table->get(args...);
    if (!elements || elements->empty()) return nullptr;
    MapCallCache cache(h, args...);
    size_t bytes = 4, footprint = 0;
    for (const auto& element : *elements) {
        bytes += element.size(); footprint += cache.list_entry_bytes(element.size());
    }
    WriteBuffer buffer(bytes);
    buffer.write_int(static_cast<int32_t>(elements->size()));
    read_cached_list(cache, *elements, footprint, [&](size_t, std::string_view element) {
        buffer.write_raw(reinterpret_cast<const uint8_t*>(element.data()), element.size());
    });
    return buffer_to_jbytearray(env, buffer.data(), buffer.size());
}

template<class K, class... Args>
void cached_list_append(StateHandle* h, StateTable<K, ElementList>* table,
                        ElementList&& added, Args&&... args) {
    if (added.empty()) return;
    MapCallCache cache(h, args...);
    auto append = [&](ElementList& elements) {
        size_t start = elements.size();
        elements.insert(elements.end(), std::make_move_iterator(added.begin()),
                        std::make_move_iterator(added.end()));
        for (size_t i = start; i < elements.size(); ++i) cache.put(uint64_t(i), elements[i]);
    };
    try {
        if (!table->modify_in_place(args..., append)) {
            auto* stored = table->put(args..., std::move(added));
            for (size_t i = 0; i < stored->size(); ++i) cache.put(uint64_t(i), (*stored)[i]);
        }
    } catch (...) { cache.invalidate(); throw; }
}

template<class K, class... Args>
void cached_list_add(StateHandle* h, StateTable<K, ElementList>* table,
                     std::string&& element, Args&&... args) {
    MapCallCache cache(h, args...);
    try {
        if (!table->modify_in_place(args..., [&](ElementList& elements) {
            elements.push_back(std::move(element));
            cache.put(uint64_t(elements.size() - 1), elements.back());
        })) {
            ElementList elements;
            elements.push_back(std::move(element));
            auto* stored = table->put(args..., std::move(elements));
            cache.put(uint64_t(0), stored->front());
        }
    } catch (...) { cache.invalidate(); throw; }
}

template<class K, class... Args>
void cached_list_update(StateHandle* h, StateTable<K, ElementList>* table,
                        ElementList&& elements, Args&&... args) {
    MapCallCache cache(h, args...);
    cache.invalidate();
    auto* stored = table->put(args..., std::move(elements));
    try {
        for (size_t i = 0; i < stored->size(); ++i) cache.put(uint64_t(i), (*stored)[i]);
    } catch (...) { cache.invalidate(); throw; }
}

template<class K, class... Args>
void cached_list_clear(StateHandle* h, StateTable<K, ElementList>* table, Args&&... args) {
    MapCallCache cache(h, args...);
    cache.invalidate();
    table->remove(args...);
}
} // namespace forl0
