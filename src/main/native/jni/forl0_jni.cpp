// forl0_jni.cpp — JNI entry points for StateEngine lifecycle and state registration.
// Maps to NativeEngine.java native method declarations.

#include <jni.h>
#include <cstdio>
#include <unistd.h>
#include "jni_utils.h"
#include "state_engine.h"
#include "hot_cache.h"
#include "type_layout.h"
#include "flink_binary_format.h"

using namespace forl0;
#ifdef FORL0_TEST_CACHE_STATS
extern void test_cache_record(HotCacheLL*);
#endif

// Emit a lifecycle record in one write so Java console output cannot split
// the numeric telemetry fields when stdout and stderr share a pipe.
template<class... Args>
static void native_lifecycle_log(const char* format, Args... args) {
    char line[1024];
    int length = std::snprintf(line, sizeof(line), format, args...);
    if (length <= 0) return;
    if (size_t(length) < sizeof(line)) {
        (void)::write(STDERR_FILENO, line, size_t(length));
    } else {
        std::string full(size_t(length) + 1, '\0');
        std::snprintf(full.data(), full.size(), format, args...);
        (void)::write(STDERR_FILENO, full.data(), size_t(length));
    }
}

// ============================================================================
//  JNI class: org.apache.flink.state.forl0.NativeEngine
// ============================================================================

static StateHandle::StateKind to_state_kind(int stateType) {
    switch (stateType) {
        case 0: return StateHandle::StateKind::VALUE;
        case 1: return StateHandle::StateKind::LIST;
        case 2: return StateHandle::StateKind::MAP_;
        case 3: return StateHandle::StateKind::REDUCING;
        case 4: return StateHandle::StateKind::AGGREGATING;
        default: return StateHandle::StateKind::VALUE;
    }
}

extern "C" {

// --- StateEngine lifecycle ---

JNIEXPORT jlong JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_createEngine(
        JNIEnv* env, jclass, jint startKeyGroup, jint numKeyGroups, jint totalKeyGroups,
        jboolean l0Enabled, jlong l0Capacity, jboolean l0StrictAllocation,
        jlong l0StateCapacity, jlong l0WriteBypassThreshold,
        jint initialTableCapacity, jint maxTableCapacity,
        jdouble mainTableLoadFactor, jlong nativeMemoryMaxBytes) {
    JNI_ENTRY_RETURN(jlong, 0, {
        std::shared_ptr<HotCacheManager> mgr;
        if (l0Enabled) {
            bool reused = false;
            mgr = HotCacheManager::acquire_process_shared(
                static_cast<size_t>(l0Capacity), nullptr,
                l0StrictAllocation == JNI_TRUE,
                static_cast<uint64_t>(l0WriteBypassThreshold), &reused);
            if (mgr->is_active()) {
                native_lifecycle_log(
                        "[ForL0-HotCache] %s active=1 memory=%s key_groups=%d:%d requested_bytes=%zu actual_bytes=%zu total_sets=%u strict=%d\n",
                        reused ? "engine_reuse" : "engine_start",
                        mgr->is_l0_backed() ? "L0" : "DRAM",
                        static_cast<int>(startKeyGroup),
                        static_cast<int>(startKeyGroup + numKeyGroups),
                        mgr->requested_capacity_bytes(), mgr->capacity_bytes(),
                        mgr->total_sets(), l0StrictAllocation == JNI_TRUE ? 1 : 0);
                if (!reused && !mgr->is_l0_backed()) {
                    native_lifecycle_log("[ForL0-HotCache] L0 unavailable, using DRAM: %s\n",
                                         mgr->failure_reason().c_str());
                }
            } else {
                if (l0StrictAllocation == JNI_TRUE) {
                    throw std::runtime_error(
                        std::string("strict HotCache allocation failed: ") + mgr->failure_reason());
                }
                native_lifecycle_log(
                        "[ForL0-HotCache] WARN: allocation failed (reason: %s); "
                        "cache forcibly disabled.\n",
                        mgr->failure_reason().c_str());
                mgr.reset();  // Drop manager so cache_ pointers stay null.
            }
        } else {
            native_lifecycle_log( "[ForL0-HotCache] L0 cache disabled by config.\n");
        }
        auto* engine = new StateEngine(startKeyGroup, numKeyGroups, totalKeyGroups,
                                       std::move(mgr),
                                       static_cast<size_t>(initialTableCapacity),
                                       static_cast<size_t>(maxTableCapacity),
                                       static_cast<double>(mainTableLoadFactor),
                                       static_cast<size_t>(nativeMemoryMaxBytes),
                                       static_cast<size_t>(l0StateCapacity));
        return to_handle(engine);
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_destroyEngine(
        JNIEnv* env, jclass, jlong engineHandle) {
    JNI_ENTRY_VOID({
        auto* engine = from_handle<StateEngine>(engineHandle);
        auto* mgr = engine->hot_cache_manager();
        for (const auto& entry : engine->registered_state_handles<StateHandle>()) {
            StateHandle* state = entry.second;
            if (!state || !state->cache_backing()) continue;
            state->table_handle->flush_shared_reads();
#ifdef FORL0_TEST_CACHE_STATS
            test_cache_record(state->cache_backing());
#endif
            native_lifecycle_log(
                    "[ForL0-HotCache] state_summary name=%s sets=%u lookups=%llu hits=%llu invalidations=%llu writes=%llu bypass_events=%llu bypassed=%d\n",
                    state->state_name.c_str(), state->cache_backing()->num_sets(),
                    static_cast<unsigned long long>(state->cache_backing()->lookups()),
                    static_cast<unsigned long long>(state->cache_backing()->hits()),
                    static_cast<unsigned long long>(state->cache_backing()->invalidations()),
                    static_cast<unsigned long long>(state->cache_backing()->writes()),
                    static_cast<unsigned long long>(state->cache_backing()->bypass_events()),
                    state->cache_backing()->write_bypassed() ? 1 : 0);
            {
                const auto stats = state->table_handle->namespace_cache_stats();
                native_lifecycle_log(
                        "[ForL0-HotCache] namespace_cache_summary name=%s resident_tables=%zu resident_bytes=%zu promotions=%llu "
                        "evictions=%llu growth_demotions=%llu rejected=%llu admission_deferrals=%llu resident_lookups=%llu "
                        "loans_granted=%llu packed_promotions=%llu memberships_built=%zu membership_probes=%zu "
                        "membership_skips=%zu membership_bypassed=%zu membership_bytes=%zu memberships_released=%zu "
                        "resident_root_updates=%zu dram_root_updates=%zu blocked_backings=%zu failed_relocations=%zu "
                        "whole_table_loans=%zu empty_root_returns=%zu membership_write_skips=%zu directory_bytes=%zu "
                        "directories_built=%zu directories_released=%zu idle_window_bytes=%zu reused_windows=%zu "
                        "released_idle_windows=%zu overlap_compactions=%zu prepared_writes=%zu shared_negative_reads=%llu "
                        "blocked_payloads=%zu reserved_payload_promotions=%zu read_position_bytes=%zu read_position_writes=%zu "
                        "read_position_pauses=%zu snapshot_index_bytes=%zu snapshot_index_scans=%llu snapshot_lookup_skips=%llu "
                        "payload_run_slack=%zu payload_run_allocations=%llu payload_runs_reserved=%llu recovered_resizes=%llu "
                        "initial_root_tables=%zu resident_control_skips=%llu growth_loans=%llu resident_slot_reads=%llu "
                        "probe_index_bytes=%zu probe_indexes_built=%zu probe_indexes_released=%zu paired_slot_updates=%llu "
                        "paired_slot_inserts=%llu map_edit_bytes=%zu map_edit_reuses=%zu map_edit_pauses=%zu "
                        "learned_window_sizes=%zu shared_window_reuses=%zu peak_table_loans=%zu interleaved_growth_loans=%llu "
                        "negative_growth_loans=%llu resident_slot_writes=%llu charged_root_copy_bytes=%zu "
                        "dram_reuse_credit_spent=%zu initial_share_expansions=%zu control_view_bytes=%zu "
                        "missing_namespace_reads=%llu missing_namespace_skips=%llu missing_namespace_busy=%llu "
                        "idle_successors=%zu directory_node_reuses=%zu repeated_root_reads=%llu batched_window_reuses=%zu "
                        "singleton_windows=%zu repeated_root_writes=%llu growth_reentry_deferrals=%llu growing_map_updates=%llu "
                        "root_edit_hint_bytes=%zu root_edit_hint_hits=%zu initial_miss_root_deferrals=%zu "
                        "root_map_payload_bytes=%zu root_map_payload_promotions=%zu root_map_payload_evictions=%zu\n",
                        state->state_name.c_str(), stats.resident_tables, stats.resident_bytes,
                        static_cast<unsigned long long>(stats.promotions),
                        static_cast<unsigned long long>(stats.evictions),
                        static_cast<unsigned long long>(stats.growth_demotions),
                        static_cast<unsigned long long>(stats.rejected),
                        static_cast<unsigned long long>(stats.admission_deferrals),
                        static_cast<unsigned long long>(stats.resident_lookups),
                        static_cast<unsigned long long>(stats.loans_granted),
                        static_cast<unsigned long long>(stats.packed_promotions), stats.memberships_built,
                        stats.membership_probes, stats.membership_skips, stats.membership_bypassed,
                        stats.membership_bytes, stats.memberships_released,
                        stats.resident_root_updates, stats.dram_root_updates,
                        stats.blocked_backings, stats.failed_relocations, stats.whole_table_loans, stats.empty_root_returns,
                        stats.membership_write_skips, stats.directory_bytes,
                        stats.directories_built, stats.directories_released, stats.idle_window_bytes,
                        stats.reused_windows, stats.released_idle_windows, stats.overlap_compactions, stats.prepared_writes,
                        static_cast<unsigned long long>(stats.shared_negative_reads),
                        stats.blocked_payloads, stats.reserved_payload_promotions,
                        stats.read_position_bytes, stats.read_position_writes, stats.read_position_pauses,
                        stats.snapshot_index_bytes,
                        (unsigned long long)stats.snapshot_index_scans, (unsigned long long)stats.snapshot_lookup_skips,
                        stats.payload_run_slack, (unsigned long long)stats.payload_run_allocations,
                        (unsigned long long)stats.payload_runs_reserved, (unsigned long long)stats.recovered_resizes,
                        stats.initial_root_tables, static_cast<unsigned long long>(stats.resident_control_skips),
                        static_cast<unsigned long long>(stats.growth_loans), static_cast<unsigned long long>(stats.resident_slot_reads),
                        stats.probe_index_bytes, stats.probe_indexes_built, stats.probe_indexes_released,
                        static_cast<unsigned long long>(stats.paired_slot_updates),
                        static_cast<unsigned long long>(stats.paired_slot_inserts),
                        stats.map_edit_bytes, stats.map_edit_reuses, stats.map_edit_pauses,
                        stats.learned_window_sizes, stats.shared_window_reuses, stats.peak_table_loans,
                        static_cast<unsigned long long>(stats.interleaved_growth_loans),
                        static_cast<unsigned long long>(stats.negative_growth_loans),
                        static_cast<unsigned long long>(stats.resident_slot_writes),
                        stats.charged_root_copy_bytes, stats.dram_reuse_credit_spent, stats.initial_share_expansions, stats.control_view_bytes,
                        static_cast<unsigned long long>(stats.missing_namespace_reads),
                        static_cast<unsigned long long>(stats.missing_namespace_skips),
                        static_cast<unsigned long long>(stats.missing_namespace_busy),
                        stats.idle_successors, stats.directory_node_reuses,
                        static_cast<unsigned long long>(stats.repeated_root_reads), stats.batched_window_reuses, stats.singleton_windows,
                        static_cast<unsigned long long>(stats.repeated_root_writes),
                        static_cast<unsigned long long>(stats.growth_reentry_deferrals),
                        static_cast<unsigned long long>(stats.growing_map_updates),
                        stats.root_edit_hint_bytes, stats.root_edit_hint_hits, stats.initial_miss_root_deferrals,
                        stats.root_map_payload_bytes, stats.root_map_payload_promotions, stats.root_map_payload_evictions);
            }
        }
        if (mgr) {
            native_lifecycle_log(
                    "[ForL0-HotCache] engine_summary requested_bytes=%zu actual_bytes=%zu used_bytes=%zu total_sets=%u free_sets=%u caches=%u lookups=%llu hits=%llu invalidations=%llu writes=%llu bypass_events=%llu\n",
                    mgr->requested_capacity_bytes(), mgr->capacity_bytes(), mgr->used_bytes(),
                    mgr->total_sets(), mgr->free_sets(), mgr->cache_count(),
                    static_cast<unsigned long long>(mgr->total_lookups()),
                    static_cast<unsigned long long>(mgr->total_hits()),
                    static_cast<unsigned long long>(mgr->total_invalidations()),
                    static_cast<unsigned long long>(mgr->total_writes()),
                    static_cast<unsigned long long>(mgr->total_bypass_events()));
        }
        if (auto* allocator = engine->counting_allocator()) {
            native_lifecycle_log(
                    "[ForL0-Memory] engine_summary used_bytes=%zu peak_bytes=%zu max_bytes=%zu\n",
                    allocator->used_bytes(), allocator->peak_bytes(), allocator->max_bytes());
        }
        delete engine;
    })
}

// --- Hot-cache metrics (design §8) ---
// Layout of the returned long[12]:
//   [0] active              (1 = L0 cache active, 0 = disabled / hw unavailable)
//   [1] capacity_bytes      (actual usable HotCache bytes)
//   [2] used_bytes          (sets currently acquired × sizeof(HotSet))
//   [3] total_sets
//   [4] free_sets
//   [5] total_lookups       (sum across all attached caches)
//   [6] total_hits          (sum across all attached caches)
//   [7] total_invalidations (sum across all attached caches)
//   [8] requested_capacity_bytes
//   [9] total writes
//   [10] write-only bypass events
//   [11] attached cache count
// Java callers should pass an array of length >= 12. Older 6-slot callers get
// the first 6 fields populated exactly as before (forward-compatible).

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_getHotCacheManagerStats(
        JNIEnv* env, jclass, jlong engineHandle, jlongArray out) {
    JNI_ENTRY_VOID({
        auto* engine = from_handle<StateEngine>(engineHandle);
        jsize n = env->GetArrayLength(out);
        jlong buf[12] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        if (auto* mgr = engine->hot_cache_manager()) {
            buf[0] = mgr->is_active() ? 1 : 0;
            buf[1] = static_cast<jlong>(mgr->capacity_bytes());
            buf[2] = static_cast<jlong>(mgr->used_bytes());
            buf[3] = static_cast<jlong>(mgr->total_sets());
            buf[4] = static_cast<jlong>(mgr->free_sets());
            buf[5] = static_cast<jlong>(mgr->total_lookups());
            buf[6] = static_cast<jlong>(mgr->total_hits());
            buf[7] = static_cast<jlong>(mgr->total_invalidations());
            buf[8] = static_cast<jlong>(mgr->requested_capacity_bytes());
            buf[9] = static_cast<jlong>(mgr->total_writes());
            buf[10] = static_cast<jlong>(mgr->total_bypass_events());
            buf[11] = static_cast<jlong>(mgr->cache_count());
        }
        jsize toWrite = (n < 12) ? n : 12;
        env->SetLongArrayRegion(out, 0, toWrite, buf);
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_getEngineMemoryStats(
        JNIEnv* env, jclass, jlong engineHandle, jlongArray out) {
    JNI_ENTRY_VOID({
        auto* engine = from_handle<StateEngine>(engineHandle);
        jlong buf[3] = {0, 0, 0};
        if (auto* allocator = engine->counting_allocator()) {
            buf[0] = static_cast<jlong>(allocator->used_bytes());
            buf[1] = static_cast<jlong>(allocator->peak_bytes());
            buf[2] = static_cast<jlong>(allocator->max_bytes());
        }
        jsize n = env->GetArrayLength(out);
        env->SetLongArrayRegion(out, 0, n < 3 ? n : 3, buf);
    })
}

// Layout of the returned long[7] for a single state's attached scalar or serialized cache:
//   [0] attached (1 = cache attached, 0 = not attached)
//   [1] lookups  (hits + misses)
//   [2] hits
//   [3] invalidations
//   [4] writes
//   [5] bypass events
//   [6] sets

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_getHotCacheStats(
        JNIEnv* env, jclass, jlong stateHandle, jlongArray out) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        jlong buf[7] = {0, 0, 0, 0, 0, 0, 0};
        if (handle->cache_backing()) {
            buf[0] = 1;
            buf[1] = static_cast<jlong>(handle->cache_backing()->lookups() + handle->table_handle->pending_shared_reads());
            buf[2] = static_cast<jlong>(handle->cache_backing()->hits() + handle->table_handle->pending_shared_hits());
            buf[3] = static_cast<jlong>(handle->cache_backing()->invalidations());
            buf[4] = static_cast<jlong>(handle->cache_backing()->writes() + handle->table_handle->pending_shared_writes());
            buf[5] = static_cast<jlong>(handle->cache_backing()->bypass_events());
            buf[6] = static_cast<jlong>(handle->cache_backing()->num_sets());
        }
        jsize n = env->GetArrayLength(out);
        env->SetLongArrayRegion(out, 0, n < 7 ? n : 7, buf);
    })
}

// --- Hot-cache adaptive rebalance (design §6.3) ---
// Triggers a best-effort rebalance pass across all caches. Returns the
// number of caches whose state was reset. Callers typically invoke this
// from a low-frequency timer (e.g., once per checkpoint) or a benchmark.

JNIEXPORT jint JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_rebalanceHotCache(
        JNIEnv* env, jclass, jlong engineHandle,
        jlong intervalOps, jdouble missRateThreshold) {
    JNI_ENTRY_RETURN(jint, 0, {
        auto* engine = from_handle<StateEngine>(engineHandle);
        jint rebalanced = 0;
        for (const auto& entry : engine->registered_state_handles<StateHandle>()) {
            StateHandle* state = entry.second;
            if (state && state->cache_backing()
                    && state->cache_backing()->rebalance_if_needed(
                        static_cast<uint64_t>(intervalOps),
                        static_cast<double>(missRateThreshold))) {
                ++rebalanced;
            }
        }
        return rebalanced;
    })
}

// --- State registration ---

JNIEXPORT jlong JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_registerState(
        JNIEnv* env, jclass,
        jlong engineHandle, jstring stateName,
        jint stateType, jint keyTypeId, jint valueTypeId, jint nsTypeId,
        jbyteArray typeDescriptor) {
    JNI_ENTRY_RETURN(jlong, 0, {
        auto* engine = from_handle<StateEngine>(engineHandle);
        std::string name = jstring_to_string(env, stateName);
        bool void_ns = (nsTypeId == 20);

        // Register the appropriate SwissTable specialization based on type IDs.
        // IMPORTANT: The C++ table type MUST match what Java accesses via JNI.
        //
        // Java ForL0ValueState fast paths:
        //   - int64 key + int64 value  → typed JNI (valuePutLongLong etc.)
        //   - int64 key + double value → typed JNI (valuePutLongDouble etc.)
        //   - all others               → generic JNI (<std::string, std::string>)
        //
        // Java LIST/MAP/REDUCING/AGGREGATING states:
        //   - int64 key → typed key JNI, value always std::string (serialized)
        //   - all others → generic (<std::string, std::string>)
        int64_t table_id = -1;

        auto kind = to_state_kind(stateType);
        bool is_value_state = (kind == StateHandle::StateKind::VALUE);

        // Pre-parse type descriptor to extract layout info needed for registration.
        // MapState needs UK/UV types from parsed value_layout to select InnerMap specialization.
        bool row_data_key = false, row_data_value = false, long_namespace_serializer = false;
        std::unique_ptr<TypeLayout> parsed_key_layout;
        std::unique_ptr<TypeLayout> parsed_ns_layout;
        std::unique_ptr<TypeLayout> parsed_val_layout;
        if (typeDescriptor) {
            jsize descLen = env->GetArrayLength(typeDescriptor);
            if (descLen > 0) {
                std::vector<uint8_t> desc(descLen);
                env->GetByteArrayRegion(typeDescriptor, 0, descLen,
                                        reinterpret_cast<jbyte*>(desc.data()));
                try {
                    size_t offset = 0;
                    parsed_key_layout = TypeLayoutParser::parse_at(desc.data(), descLen, offset);
                    parsed_ns_layout = TypeLayoutParser::parse_at(desc.data(), descLen, offset);
                    parsed_val_layout = TypeLayoutParser::parse_at(desc.data(), descLen, offset);
                    if (offset + 2 <= static_cast<size_t>(descLen) && desc[offset] == 0xC7) {
                        row_data_key = (desc[offset + 1] & 1) != 0;
                        row_data_value = (desc[offset + 1] & 2) != 0;
                        long_namespace_serializer = (desc[offset + 1] & 4) != 0;
                        offset += 2;
                    }
                    if (offset < static_cast<size_t>(descLen) && desc[offset] == 0xC8) {
                        ++offset;
                        auto read_physical = [&](auto&& self, TypeLayout& layout) -> void {
                            if (offset + 5 > desc.size()) throw std::runtime_error("Truncated physical type layout");
                            uint8_t flags = desc[offset++];
                            layout.serialized_row = (flags & 1) != 0;
                            layout.wide_serialized_row = (flags & 4) != 0;
                            layout.fixed_window_row_bytes = layout.serialized_row ? (flags & (8 | 16)) : 0;
                            layout.serialized_container = (flags & 2) != 0;
                            uint32_t length = 0;
                            for (int i = 0; i < 4; ++i) length = (length << 8) | desc[offset++];
                            layout.serialized_fixed_size = static_cast<int32_t>(length);
                            for (auto& child : layout.children) self(self, *child);
                        };
                        read_physical(read_physical, *parsed_val_layout);
                    }
                } catch (const std::exception&) {}
            }
        }

        // Determine MapState InnerMap kind from parsed value layout UK/UV children.
        StateHandle::MapInnerKind map_inner_kind = StateHandle::MapInnerKind::STRING_STRING;
        if (kind == StateHandle::StateKind::MAP_ && parsed_val_layout
            && parsed_val_layout->type_id == TypeId::MAP
            && parsed_val_layout->children.size() >= 2) {
            TypeId uk_id = parsed_val_layout->children[0]->type_id;
            TypeId uv_id = parsed_val_layout->children[1]->type_id;
            if (void_ns && keyTypeId == 2 && uk_id == TypeId::TIME_WINDOW && uv_id == TypeId::TIME_WINDOW) {
                map_inner_kind = StateHandle::MapInnerKind::WINDOW_WINDOW;
            } else if (uk_id == TypeId::INT64 && uv_id == TypeId::INT64) {
                map_inner_kind = StateHandle::MapInnerKind::LONG_LONG;
            } else if (uk_id == TypeId::INT64) {
                map_inner_kind = StateHandle::MapInnerKind::LONG_STRING;
            } else if (uv_id == TypeId::INT64) {
                map_inner_kind = StateHandle::MapInnerKind::STRING_LONG;
            } else if (void_ns && keyTypeId == 2) {
                const auto& value = *parsed_val_layout->children[1];
                if (!value.serialized_row && value.serialized_fixed_size > 0
                        && value.serialized_fixed_size <= static_cast<int32_t>(InlineMapValue::capacity)) {
                    const auto& key = *parsed_val_layout->children[0];
                    map_inner_kind = StateHandle::MapInnerKind::STRING_INLINE;
                    if (key.serialized_row && value.serialized_fixed_size == 4) {
                        map_inner_kind = key.wide_serialized_row ? StateHandle::MapInnerKind::WIDE_ROW_FOUR
                                                               : StateHandle::MapInnerKind::ROW_FOUR;
                    }
                }
            }
        }

        // Determine actual storage types (must match Java JNI access patterns)
        StateHandle::KeyType stored_key_type;
        StateHandle::ValueType stored_value_type;

        // Namespace selection must preserve the value container used by Java.
        // A generic namespace changes the outer key to serialized bytes; it
        // does not turn MapState/ListState payloads into std::string values.
        const bool is_time_window_ns = (nsTypeId == 21);
        const bool typed_time_window = is_time_window_ns && keyTypeId == 2;
        const bool typed_long_namespace = nsTypeId == 2 && keyTypeId == 2 && is_value_state;
        const bool sql_window_table = is_value_state && !is_time_window_ns
                && parsed_ns_layout && parsed_ns_layout->type_id == TypeId::TIME_WINDOW;
        const bool long_namespace_table = is_value_state && nsTypeId == 2;
        const bool namespace_value_tables = long_namespace_table || sql_window_table;
        const bool numeric_window_lists = kind == StateHandle::StateKind::LIST && nsTypeId == 2 && long_namespace_serializer;
        const bool namespace_serialized_keys = (namespace_value_tables || numeric_window_lists) && keyTypeId != 2;
        const uint8_t width = namespace_value_tables && row_data_value && parsed_val_layout
                ? parsed_val_layout->fixed_window_row_bytes : 0;
        auto register_window_rows = [&](auto key) {
            return StateHandle::dispatch_window_row(width, [&](auto tag) {
                return engine->register_namespace_state<decltype(key), typename decltype(tag)::type>(name);
            });
        };
        if (numeric_window_lists) {
            if (namespace_serialized_keys) {
                table_id = engine->register_namespace_state<NamespaceBlob, ElementList>(name);
                stored_key_type = StateHandle::KeyType::BYTES;
            } else {
                table_id = engine->register_namespace_state<WindowStateKey, ElementList>(name);
                stored_key_type = StateHandle::KeyType::INT64;
            }
            stored_value_type = StateHandle::ValueType::LIST;
        } else if (namespace_serialized_keys) {
            table_id = register_window_rows(NamespaceBlob{});
            stored_key_type = StateHandle::KeyType::BYTES;
            stored_value_type = StateHandle::ValueType::BYTES;
        } else if (namespace_value_tables) {
            stored_key_type = StateHandle::KeyType::INT64;
            if (typed_long_namespace && !row_data_value && (valueTypeId == 1 || valueTypeId == 2)) {
                table_id = engine->register_namespace_state<WindowStateKey, int64_t>(name);
                stored_value_type = StateHandle::ValueType::INT64;
            } else if (typed_long_namespace && !row_data_value && valueTypeId == 4) {
                table_id = engine->register_namespace_state<WindowStateKey, double>(name);
                stored_value_type = StateHandle::ValueType::FLOAT64;
            } else {
                table_id = register_window_rows(WindowStateKey{});
                stored_value_type = StateHandle::ValueType::BYTES;
            }
        } else if (!void_ns && !typed_time_window && !typed_long_namespace) {
            stored_key_type = StateHandle::KeyType::BYTES;
            if (kind == StateHandle::StateKind::MAP_) {
                table_id = engine->register_state<NamespaceBlob, InnerMap>(name, false);
                stored_value_type = StateHandle::ValueType::MAP;
                map_inner_kind = StateHandle::MapInnerKind::STRING_STRING;
            } else if (kind == StateHandle::StateKind::LIST) {
                table_id = engine->register_state<NamespaceBlob, ElementList>(name, false);
                stored_value_type = StateHandle::ValueType::LIST;
            } else {
                table_id = engine->register_state<NamespaceBlob, NamespaceBlob>(name, false);
                stored_value_type = StateHandle::ValueType::BYTES;
            }
        } else if (typed_time_window || typed_long_namespace) {
            stored_key_type = StateHandle::KeyType::INT64;
            if (kind == StateHandle::StateKind::MAP_) {
                // Java's LONG_MAP_TW uses serialized user keys and values.
                table_id = engine->register_state<int64_t, InnerMap>(name, false);
                stored_value_type = StateHandle::ValueType::MAP;
                map_inner_kind = StateHandle::MapInnerKind::STRING_STRING;
            } else if (kind == StateHandle::StateKind::LIST) {
                table_id = engine->register_state<int64_t, ElementList>(name, false);
                stored_value_type = StateHandle::ValueType::LIST;
            } else if ((is_value_state && !(typed_long_namespace && row_data_value)
                        && (valueTypeId == 2 || valueTypeId == 1))
                       || (kind == StateHandle::StateKind::REDUCING && valueTypeId == 2)) {
                table_id = engine->register_state<int64_t, int64_t>(name, false);
                stored_value_type = StateHandle::ValueType::INT64;
            } else if (is_value_state && !(typed_long_namespace && row_data_value) && valueTypeId == 4) {
                table_id = engine->register_state<int64_t, double>(name, false);
                stored_value_type = StateHandle::ValueType::FLOAT64;
            } else {
                table_id = engine->register_state<int64_t, NamespaceBlob>(name, false);
                stored_value_type = StateHandle::ValueType::BYTES;
            }
        } else if (is_value_state) {
            auto register_bytes = [&](auto key) {
                using K = decltype(key);
                return void_ns && row_data_value ? engine->register_state<K, NamespaceRow>(name, true)
                                                 : engine->register_state<K, NamespaceBlob>(name, void_ns);
            };
            // Only register specialized types that have MATCHING Java fast paths.
            if (keyTypeId == 2 && (valueTypeId == 2 || valueTypeId == 1)) {
                // long key + long/int value → SwissTable<int64_t, int64_t>
                table_id = engine->register_state<int64_t, int64_t>(name, void_ns);
                stored_key_type = StateHandle::KeyType::INT64;
                stored_value_type = StateHandle::ValueType::INT64;
            } else if (keyTypeId == 2 && valueTypeId == 4) {
                // long key + double value → SwissTable<int64_t, double>
                table_id = engine->register_state<int64_t, double>(name, void_ns);
                stored_key_type = StateHandle::KeyType::INT64;
                stored_value_type = StateHandle::ValueType::FLOAT64;
            } else if (keyTypeId == 2) {
                table_id = register_bytes(int64_t{});
                stored_key_type = StateHandle::KeyType::INT64;
                stored_value_type = StateHandle::ValueType::BYTES;
            } else if (keyTypeId == 1 && (valueTypeId == 2 || valueTypeId == 1)) {
                // int key + long/int value → SwissTable<int32_t, int64_t>
                table_id = engine->register_state<int32_t, int64_t>(name, void_ns);
                stored_key_type = StateHandle::KeyType::INT32;
                stored_value_type = StateHandle::ValueType::INT64;
            } else if (keyTypeId == 1 && valueTypeId == 4) {
                // int key + double value → SwissTable<int32_t, double>
                table_id = engine->register_state<int32_t, double>(name, void_ns);
                stored_key_type = StateHandle::KeyType::INT32;
                stored_value_type = StateHandle::ValueType::FLOAT64;
            } else if (keyTypeId == 1) {
                table_id = register_bytes(int32_t{});
                stored_key_type = StateHandle::KeyType::INT32;
                stored_value_type = StateHandle::ValueType::BYTES;
            } else if (keyTypeId == 13) {
                const bool pair = parsed_key_layout && parsed_key_layout->type_id == TypeId::FIXED_ROW
                        && parsed_key_layout->fixed_row_arity == 2;
                stored_key_type = pair ? StateHandle::KeyType::FIXED_PAIR : StateHandle::KeyType::FIXED_ROW;
                auto register_fixed = [&](auto value) {
                    using V = decltype(value);
                    return pair ? engine->register_state<FixedPair, V>(name, void_ns)
                                : engine->register_state<FixedRow, V>(name, void_ns);
                };
                if (valueTypeId == 1 || valueTypeId == 2) {
                    table_id = register_fixed(int64_t{});
                    stored_value_type = StateHandle::ValueType::INT64;
                } else if (valueTypeId == 4) {
                    table_id = register_fixed(double{});
                    stored_value_type = StateHandle::ValueType::FLOAT64;
                } else {
                    table_id = void_ns && row_data_value ? register_fixed(NamespaceRow{}) : register_fixed(NamespaceBlob{});
                    stored_value_type = StateHandle::ValueType::BYTES;
                }
            } else {
                // Non-INT64 key: generic path uses serialized bytes
                table_id = register_bytes(NamespaceBlob{});
                stored_key_type = StateHandle::KeyType::BYTES;
                stored_value_type = StateHandle::ValueType::BYTES;
            }
        } else if (kind == StateHandle::StateKind::LIST) {
            // ListState: store std::vector<std::string> (ElementList) per key
            if (keyTypeId == 2) {
                table_id = engine->register_state<int64_t, ElementList>(name, void_ns);
                stored_key_type = StateHandle::KeyType::INT64;
            } else {
                table_id = engine->register_state<NamespaceBlob, ElementList>(name, void_ns);
                stored_key_type = StateHandle::KeyType::BYTES;
            }
            stored_value_type = StateHandle::ValueType::LIST;
        } else if (kind == StateHandle::StateKind::MAP_) {
            // MapState: select InnerMap specialization based on UK/UV types.
            if (keyTypeId == 2) {
                stored_key_type = StateHandle::KeyType::INT64;
                switch (map_inner_kind) {
                    case StateHandle::MapInnerKind::WINDOW_WINDOW:
                        table_id = engine->register_state<int64_t, InnerMapTimeWindow>(name, void_ns);
                        break;
                    case StateHandle::MapInnerKind::LONG_LONG:
                        table_id = engine->register_state<int64_t, InnerMapLongLong>(name, void_ns);
                        break;
                    case StateHandle::MapInnerKind::LONG_STRING:
                        table_id = engine->register_state<int64_t, InnerMapLongString>(name, void_ns);
                        break;
                    case StateHandle::MapInnerKind::STRING_LONG:
                        table_id = engine->register_state<int64_t, InnerMapStringLong>(name, void_ns);
                        break;
                    case StateHandle::MapInnerKind::ROW_FOUR:
                        table_id = engine->register_state<int64_t, InnerMapRowFour>(name, void_ns);
                        break;
                    case StateHandle::MapInnerKind::WIDE_ROW_FOUR:
                        table_id = engine->register_state<int64_t, InnerMapWideRowFour>(name, void_ns);
                        break;
                    case StateHandle::MapInnerKind::STRING_INLINE:
                        table_id = engine->register_state<int64_t, InnerMapInline>(name, void_ns);
                        break;
                    default:
                        table_id = engine->register_state<int64_t, InnerMap>(name, void_ns);
                        break;
                }
            } else {
                table_id = engine->register_state<NamespaceBlob, InnerMap>(name, void_ns);
                stored_key_type = StateHandle::KeyType::BYTES;
            }
            stored_value_type = StateHandle::ValueType::MAP;
        } else {
            // REDUCING/AGGREGATING: Java does read-modify-write with reduce/aggregate function.
            // ReducingState with INT64 key + INT64 value uses <int64_t, int64_t> (zero-ser key+value).
            // AggregatingState always serializes values (ACC type goes through Java),
            // so use <int64_t, std::string> for Long key to avoid key serialization only.
            bool is_reducing = (kind == StateHandle::StateKind::REDUCING);
            if (keyTypeId == 2 && is_reducing && valueTypeId == 2) {
                // Only ReducingState<Long> with Long key → zero-ser both key and value.
                // INT32 value goes through <int64_t, std::string> (serialize value only).
                table_id = engine->register_state<int64_t, int64_t>(name, void_ns);
                stored_key_type = StateHandle::KeyType::INT64;
                stored_value_type = StateHandle::ValueType::INT64;
            } else if (keyTypeId == 2) {
                table_id = engine->register_state<int64_t, NamespaceBlob>(name, void_ns);
                stored_key_type = StateHandle::KeyType::INT64;
                stored_value_type = StateHandle::ValueType::BYTES;
            } else {
                // The generic Java path always serializes accumulator values, even
                // when the logical value type is Long or Integer.
                table_id = engine->register_state<NamespaceBlob, NamespaceBlob>(name, void_ns);
                stored_key_type = StateHandle::KeyType::BYTES;
                stored_value_type = StateHandle::ValueType::BYTES;
            }
        }

        // Create a StateHandle with STORED types (matching actual table types)
        auto* handle = new StateHandle();
        handle->state_name = name;
        handle->engine = engine;
        handle->table_id = table_id;
        handle->key_type = stored_key_type;
        handle->value_type = stored_value_type;
        handle->kind = kind;
        handle->ns_type = sql_window_table ? StateHandle::NsType::SQL_WINDOW
                        : typed_long_namespace || long_namespace_table ? StateHandle::NsType::LONG_NS
                        : typed_time_window ? StateHandle::NsType::TIME_WINDOW
                        : (void_ns ? StateHandle::NsType::VOID_NS
                                   : StateHandle::NsType::BYTES);
        handle->void_namespace = void_ns;
        handle->row_data_key = row_data_key;
        handle->row_data_value = row_data_value;
        handle->root_rows = void_ns && is_value_state && row_data_value && stored_value_type == StateHandle::ValueType::BYTES;
        handle->namespace_value_tables = namespace_value_tables;
        handle->numeric_window_lists = numeric_window_lists;
        handle->namespace_serialized_keys = namespace_serialized_keys;
        handle->window_row_width = width;
        handle->map_inner_kind = map_inner_kind;
        handle->table_handle = engine->state_handles().at(table_id).get();

        // Register StateHandle pointer in engine for checkpoint type dispatch.
        // Engine takes ownership and will free on destruction.
        engine->register_state_handle_ptr(table_id, handle,
            [](void* p) { delete static_cast<StateHandle*>(p); });

        // Generic JNI keys carry the serialized namespace prefix on the wire.
        // Split recognized Flink encodings in storage and rejoin for the old checkpoint format.
        if (!void_ns && !namespace_value_tables && !numeric_window_lists && stored_key_type == StateHandle::KeyType::BYTES
                && parsed_ns_layout && (parsed_ns_layout->serialized_fixed_size >= 0
                    || parsed_ns_layout->serialized_row || parsed_ns_layout->type_id == TypeId::TIME_WINDOW
                    || (nsTypeId >= 1 && nsTypeId <= 7))) {
            handle->table_handle->set_serialized_namespace(std::move(parsed_ns_layout));
        }

        // Move pre-parsed layouts to handle (parsed earlier for MapState dispatch).
        if (parsed_key_layout && parsed_key_layout->type_id == TypeId::FIXED_ROW) {
            handle->key_layout = std::move(parsed_key_layout);
        }
        if (parsed_val_layout) {
            handle->value_layout = std::move(parsed_val_layout);
        }

        return to_handle(handle);
    })
}

JNIEXPORT jlong JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_stateReadResultAddress(JNIEnv*, jclass, jlong state) {
    return reinterpret_cast<jlong>(from_handle<StateHandle>(state)->read_result);
}
JNIEXPORT jlongArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_stateCacheMemoryRange(JNIEnv* env, jclass, jlong state) {
    JNI_ENTRY_RETURN(jlongArray, nullptr, {
        const auto* handle = from_handle<StateHandle>(state);
        const auto* arena = handle->table_handle->namespace_arena();
        if (!arena) return nullptr;
        const auto* pool = handle->engine->hot_cache_manager();
        jlong range[5] = {
                reinterpret_cast<jlong>(arena->data()), static_cast<jlong>(arena->primary_capacity()),
                pool ? reinterpret_cast<jlong>(pool->data()) : 0,
                pool ? static_cast<jlong>(pool->capacity_bytes()) : 0,
                1,  // Packed inner maps carry a 64-bit tag mask after their entry pointer.
        };
        jlongArray out = env->NewLongArray(5);
        env->SetLongArrayRegion(out, 0, 5, range);
        return out;
    })
}
JNIEXPORT jobject JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_rootMembershipViews(JNIEnv* env, jclass, jlong state) {
    JNI_ENTRY_RETURN(jobject, nullptr, {
        auto* handle = from_handle<StateHandle>(state);
        if (!handle->void_namespace) return nullptr;
        using KeyType = StateHandle::KeyType;
        using Kind = StateHandle::StateKind;
        const bool fixed_pair = handle->key_type == KeyType::FIXED_PAIR;
        const bool numeric_value = handle->kind == Kind::VALUE
                && (handle->key_type == KeyType::INT32 || handle->key_type == KeyType::INT64 || fixed_pair);
        const bool numeric_map = handle->kind == Kind::MAP_ && handle->key_type == KeyType::INT64;
        if (!numeric_value && !numeric_map) return nullptr;
        auto* views = handle->table_handle->root_membership_views();
        if (!views) return nullptr;
        return env->NewDirectByteBuffer(views, jlong(handle->engine->num_key_groups()) * sizeof(RootMembershipView));
    })
}

JNIEXPORT jobject JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_residentControlViews(JNIEnv* env, jclass, jlong state) {
    JNI_ENTRY_RETURN(jobject, nullptr, {
        auto* h = from_handle<StateHandle>(state);
        auto* views = h->table_handle->resident_control_views();
        const size_t width = ResidentControls::per_group(h->table_handle->is_void_namespace());
        return views ? env->NewDirectByteBuffer(views, jlong(h->engine->num_key_groups()) * width * sizeof(ResidentControls)) : nullptr;
    })
}

// --- Utility methods ---

JNIEXPORT jlong JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_totalEntries(
        JNIEnv* env, jclass, jlong engineHandle) {
    JNI_ENTRY_RETURN(jlong, 0, {
        auto* engine = from_handle<StateEngine>(engineHandle);
        return static_cast<jlong>(engine->total_entries());
    })
}

JNIEXPORT jlong JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_stateEntries(
        JNIEnv* env, jclass, jlong stateHandle) {
    JNI_ENTRY_RETURN(jlong, 0, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        return static_cast<jlong>(handle->table_handle->total_size());
    })
}

// --- Key enumeration (for getKeys) ---

JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_getWindowStateKeys(
        JNIEnv* env, jclass, jlong stateHandle, jboolean allNamespaces, jlong first, jlong second) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        const auto* handle = from_handle<StateHandle>(stateHandle);
        if (!handle->namespace_value_tables && !handle->numeric_window_lists) return nullptr;
        WriteBuffer out;
        out.write_int(0);
        int32_t count = 0;
        auto emit_keys = [&](const auto* table, const auto& filter) {
            using N = std::decay_t<decltype(filter)>;
            auto emit = [&](const N& ns, const auto& key, const auto&) {
                write_window_state_key(out, key, handle->row_data_key);
                if (allNamespaces) {
                    if constexpr (std::is_same_v<N, TimeWindow>) {
                        out.write_long(ns.first);
                        out.write_long(ns.second);
                    } else {
                        out.write_long(ns);
                    }
                }
                ++count;
            };
            const int start = table->start_key_group();
            const int end = start + table->num_key_groups();
            for (int group = start; group < end; ++group) {
                if (allNamespaces) table->template for_each_in_key_group_ns<N>(group, emit);
                else table->for_each_in_namespace(group, filter, emit);
            }
        };
        if (handle->numeric_window_lists) {
            if (handle->namespace_serialized_keys) emit_keys(handle->table<NamespaceBlob, ElementList>(), int64_t(first));
            else emit_keys(handle->table<WindowStateKey, ElementList>(), int64_t(first));
        } else if (handle->namespace_serialized_keys) {
            handle->visit_window_rows<NamespaceBlob>([&](const auto* table) {
                if (handle->ns_type == StateHandle::NsType::SQL_WINDOW) emit_keys(table, TimeWindow{first, second});
                else emit_keys(table, int64_t(first));
            });
        } else if (handle->ns_type == StateHandle::NsType::SQL_WINDOW) {
            handle->visit_window_rows<WindowStateKey>([&](const auto* table) { emit_keys(table, TimeWindow{first, second}); });
        } else if (handle->value_type == StateHandle::ValueType::INT64) {
            emit_keys(handle->table<WindowStateKey, int64_t>(), int64_t(first));
        } else if (handle->value_type == StateHandle::ValueType::FLOAT64) {
            emit_keys(handle->table<WindowStateKey, double>(), int64_t(first));
        } else {
            handle->visit_window_rows<WindowStateKey>([&](const auto* table) { emit_keys(table, int64_t(first)); });
        }
        out.patch_int(0, count);
        return string_to_jbytearray(env, {reinterpret_cast<const char*>(out.data()), out.size()});
    })
}

JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_getStateKeys(
        JNIEnv* env, jclass, jlong stateHandle) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        if (handle->namespace_value_tables || handle->numeric_window_lists) return nullptr;  // Requires a namespace.
        WriteBuffer buf;

        if (handle->key_type == StateHandle::KeyType::INT64) {
            // INT64 key: check all possible value types for this key type
            auto vt = handle->value_type;
            int32_t count = 0;
            size_t count_pos = buf.size();
            buf.write_int(0);  // placeholder

            // Lambda to iterate int64 keys from any value type
            auto emit_keys = [&](auto* tbl) {
                if (!tbl) return;
                tbl->for_each_void([&](int, const int64_t& k, const auto&) {
                    buf.write_long(k); ++count;
                });
            };

            if (vt == StateHandle::ValueType::INT64) {
                emit_keys(handle->table<int64_t, int64_t>());
            } else if (vt == StateHandle::ValueType::FLOAT64) {
                emit_keys(handle->table<int64_t, double>());
            } else if (vt == StateHandle::ValueType::LIST) {
                emit_keys(handle->table<int64_t, ElementList>());
            } else if (vt == StateHandle::ValueType::MAP) {
                switch (handle->map_inner_kind) {
                    case StateHandle::MapInnerKind::WINDOW_WINDOW:
                        emit_keys(handle->table<int64_t, InnerMapTimeWindow>());
                        break;
                    case StateHandle::MapInnerKind::LONG_LONG:
                        emit_keys(handle->table<int64_t, InnerMapLongLong>());
                        break;
                    case StateHandle::MapInnerKind::LONG_STRING:
                        emit_keys(handle->table<int64_t, InnerMapLongString>());
                        break;
                    case StateHandle::MapInnerKind::STRING_LONG:
                        emit_keys(handle->table<int64_t, InnerMapStringLong>());
                        break;
                    case StateHandle::MapInnerKind::ROW_FOUR:
                        emit_keys(handle->table<int64_t, InnerMapRowFour>());
                        break;
                    case StateHandle::MapInnerKind::WIDE_ROW_FOUR:
                        emit_keys(handle->table<int64_t, InnerMapWideRowFour>());
                        break;
                    case StateHandle::MapInnerKind::STRING_INLINE:
                        emit_keys(handle->table<int64_t, InnerMapInline>());
                        break;
                    default:
                        emit_keys(handle->table<int64_t, InnerMap>());
                        break;
                }
            } else {
                // BYTES or STRING value type → <int64_t, std::string>
                handle->visit_byte_table<int64_t>(emit_keys);
            }

            uint8_t* p = buf.data_mut() + count_pos;
            p[0] = (count >> 24) & 0xFF;
            p[1] = (count >> 16) & 0xFF;
            p[2] = (count >> 8) & 0xFF;
            p[3] = count & 0xFF;
        } else if (handle->key_type == StateHandle::KeyType::INT32) {
            // INT32 key: emit as 4-byte int
            auto vt = handle->value_type;
            int32_t count = 0;
            size_t count_pos = buf.size();
            buf.write_int(0);  // placeholder

            auto emit_keys = [&](auto* tbl) {
                if (!tbl) return;
                tbl->for_each_void([&](int, const int32_t& k, const auto&) {
                    buf.write_int(k); ++count;
                });
            };

            if (vt == StateHandle::ValueType::INT64) {
                emit_keys(handle->table<int32_t, int64_t>());
            } else if (vt == StateHandle::ValueType::FLOAT64) {
                emit_keys(handle->table<int32_t, double>());
            } else {
                handle->visit_byte_table<int32_t>(emit_keys);
            }

            uint8_t* p = buf.data_mut() + count_pos;
            p[0] = (count >> 24) & 0xFF;
            p[1] = (count >> 16) & 0xFF;
            p[2] = (count >> 8) & 0xFF;
            p[3] = count & 0xFF;
        } else if (handle->key_type == StateHandle::KeyType::FIXED_PAIR || handle->key_type == StateHandle::KeyType::FIXED_ROW) {
            int32_t count = 0;
            buf.write_int(0);
            auto enumerate = [&](auto key) {
                using K = decltype(key);
                auto emit = [&](auto* table) {
                    table->for_each_void([&](int, const K& current, const auto&) {
                        const auto position = buf.size();
                        buf.write_int(0);
                        buf.write_fixed_row(current);
                        buf.patch_int(position, static_cast<int32_t>(buf.size() - position - 4));
                        ++count;
                    });
                };
                if (handle->value_type == StateHandle::ValueType::INT64) emit(handle->table<K, int64_t>());
                else if (handle->value_type == StateHandle::ValueType::FLOAT64) emit(handle->table<K, double>());
                else handle->visit_byte_table<K>(emit);
            };
            if (handle->key_type == StateHandle::KeyType::FIXED_PAIR) enumerate(FixedPair{});
            else enumerate(FixedRow{});
            buf.patch_int(0, count);
        } else {
            auto vt = handle->value_type;
            int32_t count = 0;
            size_t count_pos = buf.size();
            buf.write_int(0);  // placeholder

            auto emit_str_keys = [&](auto* tbl) {
                if (!tbl) return;
                tbl->for_each_void([&](int, const NamespaceBlob& k, const auto&) {
                    buf.write_int(static_cast<int32_t>(k.size()));
                    buf.write_raw(reinterpret_cast<const uint8_t*>(k.data()), k.size());
                    ++count;
                });
            };

            if (vt == StateHandle::ValueType::LIST) {
                emit_str_keys(handle->table<NamespaceBlob, ElementList>());
            } else if (vt == StateHandle::ValueType::MAP) {
                emit_str_keys(handle->table<NamespaceBlob, InnerMap>());
            } else {
                handle->visit_byte_table<NamespaceBlob>(emit_str_keys);
            }
            uint8_t* p = buf.data_mut() + count_pos;
            p[0] = (count >> 24) & 0xFF;
            p[1] = (count >> 16) & 0xFF;
            p[2] = (count >> 8) & 0xFF;
            p[3] = count & 0xFF;
        }

        if (buf.size() <= 4) return nullptr;
        return buffer_to_jbytearray(env, buf.data(), buf.size());
    })
}

}  // extern "C"
