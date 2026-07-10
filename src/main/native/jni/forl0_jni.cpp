// forl0_jni.cpp — JNI entry points for StateEngine lifecycle and state registration.
// Maps to NativeEngine.java native method declarations.

#include <jni.h>
#include <cstdio>
#include "jni_utils.h"
#include "state_engine.h"
#include "hot_cache.h"
#include "type_layout.h"
#include "flink_binary_format.h"

using namespace forl0;

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
        jboolean l0Enabled, jlong l0Capacity, jint initialTableCapacity) {
    JNI_ENTRY_RETURN(jlong, 0, {
        Allocator* alloc = &DefaultAllocator::instance();
        std::unique_ptr<HotCacheManager> mgr;
        if (l0Enabled) {
            mgr = std::unique_ptr<HotCacheManager>(
                new HotCacheManager(static_cast<size_t>(l0Capacity)));
            // Hardware-gating telemetry per design §3.2.
            if (mgr->is_active()) {
                fprintf(stderr,
                        "[ForL0-HotCache] L0 cache enabled: capacity=%zuMB, total_sets=%u\n",
                        mgr->capacity_bytes() / (1024 * 1024), mgr->total_sets());
            } else {
                fprintf(stderr,
                        "[ForL0-HotCache] WARN: L0 hardware not available (reason: %s); "
                        "cache forcibly disabled.\n",
                        mgr->failure_reason().c_str());
                mgr.reset();  // Drop manager so cache_ pointers stay null.
            }
        } else {
            fprintf(stderr, "[ForL0-HotCache] L0 cache disabled by config.\n");
        }
        auto* engine = new StateEngine(startKeyGroup, numKeyGroups, totalKeyGroups,
                                       alloc, std::move(mgr),
                                       static_cast<size_t>(initialTableCapacity));
        return to_handle(engine);
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_destroyEngine(
        JNIEnv* env, jclass, jlong engineHandle) {
    JNI_ENTRY_VOID({
        auto* engine = from_handle<StateEngine>(engineHandle);
        if (auto* mgr = engine->hot_cache_manager()) {
            fprintf(stderr,
                    "[ForL0-HotCache] Shutdown: capacity=%zuMB, used_bytes=%zuKB, "
                    "total_sets=%u, free_sets=%u\n",
                    mgr->capacity_bytes() / (1024 * 1024),
                    mgr->used_bytes() / 1024,
                    mgr->total_sets(), mgr->free_sets());
        }
        delete engine;
    })
}

// --- Hot-cache metrics (design §8) ---
// Layout of the returned long[9]:
//   [0] active              (1 = L0 cache active, 0 = disabled / hw unavailable)
//   [1] capacity_bytes      (actual bytes obtained from l0_mem_alloc)
//   [2] used_bytes          (sets currently acquired × sizeof(HotSet))
//   [3] total_sets
//   [4] free_sets
//   [5] total_lookups       (sum across all attached caches)
//   [6] total_hits          (sum across all attached caches)
//   [7] total_invalidations (sum across all attached caches)
//   [8] reserved (0)
// Java callers must pass an array of length >= 9. Older 6-slot callers get
// the first 6 fields populated exactly as before (forward-compatible).

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_getHotCacheManagerStats(
        JNIEnv* env, jclass, jlong engineHandle, jlongArray out) {
    JNI_ENTRY_VOID({
        auto* engine = from_handle<StateEngine>(engineHandle);
        jsize n = env->GetArrayLength(out);
        jlong buf[9] = {0, 0, 0, 0, 0, 0, 0, 0, 0};
        if (auto* mgr = engine->hot_cache_manager()) {
            buf[0] = mgr->is_active() ? 1 : 0;
            buf[1] = static_cast<jlong>(mgr->capacity_bytes());
            buf[2] = static_cast<jlong>(mgr->used_bytes());
            buf[3] = static_cast<jlong>(mgr->total_sets());
            buf[4] = static_cast<jlong>(mgr->free_sets());
            buf[5] = static_cast<jlong>(mgr->total_lookups());
            buf[6] = static_cast<jlong>(mgr->total_hits());
            buf[7] = static_cast<jlong>(mgr->total_invalidations());
        }
        jsize toWrite = (n < 9) ? n : 9;
        env->SetLongArrayRegion(out, 0, toWrite, buf);
    })
}

// Layout of the returned long[4] for a single state's attached HotCacheLL:
//   [0] attached (1 = cache attached, 0 = not attached)
//   [1] lookups  (hits + misses)
//   [2] hits
//   [3] invalidations

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_getHotCacheStats(
        JNIEnv* env, jclass, jlong stateHandle, jlongArray out) {
    JNI_ENTRY_VOID({
        auto* handle = from_handle<StateHandle>(stateHandle);
        jlong buf[4] = {0, 0, 0, 0};
        if (handle->hot_cache_ll) {
            buf[0] = 1;
            buf[1] = static_cast<jlong>(handle->hot_cache_ll->lookups());
            buf[2] = static_cast<jlong>(handle->hot_cache_ll->hits());
            buf[3] = static_cast<jlong>(handle->hot_cache_ll->invalidations());
        }
        env->SetLongArrayRegion(out, 0, 4, buf);
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
        auto* mgr = engine->hot_cache_manager();
        if (!mgr) return 0;
        return static_cast<jint>(mgr->rebalance_if_needed(
            static_cast<uint64_t>(intervalOps),
            static_cast<double>(missRateThreshold)));
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
            if (uk_id == TypeId::INT64 && uv_id == TypeId::INT64) {
                map_inner_kind = StateHandle::MapInnerKind::LONG_LONG;
            } else if (uk_id == TypeId::INT64) {
                map_inner_kind = StateHandle::MapInnerKind::LONG_STRING;
            } else if (uv_id == TypeId::INT64) {
                map_inner_kind = StateHandle::MapInnerKind::STRING_LONG;
            }
        }

        // Determine actual storage types (must match Java JNI access patterns)
        StateHandle::KeyType stored_key_type;
        StateHandle::ValueType stored_value_type;

        // CRITICAL: When void_ns is false and namespace is not a known specialization,
        // Java ALWAYS uses the generic byte[] JNI path
        // (serializing key+namespace together). The C++ table MUST be <std::string, std::string>
        // to match. Typed specializations are valid for VoidNamespace and TimeWindow states.
        bool is_time_window_ns = (nsTypeId == 21);  // TYPE_TIME_WINDOW
        if (!void_ns && !is_time_window_ns) {
            table_id = engine->register_state<std::string, std::string>(name, void_ns);
            stored_key_type = StateHandle::KeyType::BYTES;
            stored_value_type = StateHandle::ValueType::BYTES;
        } else if (is_time_window_ns) {
            // TimeWindow namespace: register typed tables (same specialization as VoidNamespace).
            // Java will use *WithTW JNI methods with typed key/value.
            if (keyTypeId == 2 && (valueTypeId == 2 || valueTypeId == 1)) {
                table_id = engine->register_state<int64_t, int64_t>(name, void_ns);
                stored_key_type = StateHandle::KeyType::INT64;
                stored_value_type = StateHandle::ValueType::INT64;
            } else if (keyTypeId == 2 && valueTypeId == 4) {
                table_id = engine->register_state<int64_t, double>(name, void_ns);
                stored_key_type = StateHandle::KeyType::INT64;
                stored_value_type = StateHandle::ValueType::FLOAT64;
            } else if (keyTypeId == 2) {
                table_id = engine->register_state<int64_t, std::string>(name, void_ns);
                stored_key_type = StateHandle::KeyType::INT64;
                stored_value_type = StateHandle::ValueType::BYTES;
            } else if (kind == StateHandle::StateKind::REDUCING && (valueTypeId == 2 || valueTypeId == 1)) {
                // BYTES_TW: String/bytes key + long value ReducingState with TimeWindow ns.
                // Stores <std::string, int64_t> so C++ can do builtin aggregation in-place.
                table_id = engine->register_state<std::string, int64_t>(name, void_ns);
                stored_key_type = StateHandle::KeyType::BYTES;
                stored_value_type = StateHandle::ValueType::INT64;
            } else {
                table_id = engine->register_state<std::string, std::string>(name, void_ns);
                stored_key_type = StateHandle::KeyType::BYTES;
                stored_value_type = StateHandle::ValueType::BYTES;
            }
        } else if (is_value_state) {
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
                // INT64 key + STRING/BYTES/other → SwissTable<int64_t, std::string>
                table_id = engine->register_state<int64_t, std::string>(name, void_ns);
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
                // INT32 key + STRING/BYTES/other → SwissTable<int32_t, std::string>
                table_id = engine->register_state<int32_t, std::string>(name, void_ns);
                stored_key_type = StateHandle::KeyType::INT32;
                stored_value_type = StateHandle::ValueType::BYTES;
            } else if (keyTypeId == 13) {
                // FIXED_ROW key: multi-field RowData stored as FixedRow
                if (valueTypeId == 2 || valueTypeId == 1) {
                    table_id = engine->register_state<FixedRow, int64_t>(name, void_ns);
                    stored_key_type = StateHandle::KeyType::FIXED_ROW;
                    stored_value_type = StateHandle::ValueType::INT64;
                } else if (valueTypeId == 4) {
                    table_id = engine->register_state<FixedRow, double>(name, void_ns);
                    stored_key_type = StateHandle::KeyType::FIXED_ROW;
                    stored_value_type = StateHandle::ValueType::FLOAT64;
                } else {
                    table_id = engine->register_state<FixedRow, std::string>(name, void_ns);
                    stored_key_type = StateHandle::KeyType::FIXED_ROW;
                    stored_value_type = StateHandle::ValueType::BYTES;
                }
            } else {
                // Non-INT64 key: generic path uses serialized bytes
                table_id = engine->register_state<std::string, std::string>(name, void_ns);
                stored_key_type = StateHandle::KeyType::BYTES;
                stored_value_type = StateHandle::ValueType::BYTES;
            }
        } else if (kind == StateHandle::StateKind::LIST) {
            // ListState: store std::vector<std::string> (ElementList) per key
            if (keyTypeId == 2) {
                table_id = engine->register_state<int64_t, ElementList>(name, void_ns);
                stored_key_type = StateHandle::KeyType::INT64;
            } else {
                table_id = engine->register_state<std::string, ElementList>(name, void_ns);
                stored_key_type = StateHandle::KeyType::BYTES;
            }
            stored_value_type = StateHandle::ValueType::LIST;
        } else if (kind == StateHandle::StateKind::MAP_) {
            // MapState: select InnerMap specialization based on UK/UV types.
            if (keyTypeId == 2) {
                stored_key_type = StateHandle::KeyType::INT64;
                switch (map_inner_kind) {
                    case StateHandle::MapInnerKind::LONG_LONG:
                        table_id = engine->register_state<int64_t, InnerMapLongLong>(name, void_ns);
                        break;
                    case StateHandle::MapInnerKind::LONG_STRING:
                        table_id = engine->register_state<int64_t, InnerMapLongString>(name, void_ns);
                        break;
                    case StateHandle::MapInnerKind::STRING_LONG:
                        table_id = engine->register_state<int64_t, InnerMapStringLong>(name, void_ns);
                        break;
                    default:
                        table_id = engine->register_state<int64_t, InnerMap>(name, void_ns);
                        break;
                }
            } else {
                table_id = engine->register_state<std::string, InnerMap>(name, void_ns);
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
                table_id = engine->register_state<int64_t, std::string>(name, void_ns);
                stored_key_type = StateHandle::KeyType::INT64;
                stored_value_type = StateHandle::ValueType::BYTES;
            } else if (is_reducing && (valueTypeId == 2 || valueTypeId == 1)) {
                // String/bytes key + long value ReducingState (VoidNamespace).
                // Stores <std::string, int64_t> so reduceAddGenericLong can do in-place aggregation.
                table_id = engine->register_state<std::string, int64_t>(name, void_ns);
                stored_key_type = StateHandle::KeyType::BYTES;
                stored_value_type = StateHandle::ValueType::INT64;
            } else {
                table_id = engine->register_state<std::string, std::string>(name, void_ns);
                stored_key_type = StateHandle::KeyType::BYTES;
                stored_value_type = StateHandle::ValueType::BYTES;
            }
        }

        // Create a StateHandle with STORED types (matching actual table types)
        auto* handle = new StateHandle();
        handle->engine = engine;
        handle->table_id = table_id;
        handle->key_type = stored_key_type;
        handle->value_type = stored_value_type;
        handle->kind = kind;
        handle->ns_type = is_time_window_ns ? StateHandle::NsType::TIME_WINDOW
                        : (void_ns ? StateHandle::NsType::VOID_NS
                                   : StateHandle::NsType::BYTES);
        handle->void_namespace = void_ns;
        handle->map_inner_kind = map_inner_kind;

        // Register StateHandle pointer in engine for checkpoint type dispatch.
        // Engine takes ownership and will free on destruction.
        engine->register_state_handle_ptr(table_id, handle,
            [](void* p) { delete static_cast<StateHandle*>(p); });

        // Move pre-parsed layouts to handle (parsed earlier for MapState dispatch).
        if (parsed_key_layout && parsed_key_layout->type_id == TypeId::FIXED_ROW) {
            handle->key_layout = std::move(parsed_key_layout);
        }
        if (parsed_val_layout) {
            handle->value_layout = std::move(parsed_val_layout);
        }

        // L0 hot-key cache attachment. Phase A covered <int64,int64>;
        // Phase B extends the fast-path to <int64,double>, <int32,int64>,
        // <int32,double> by bit-casting doubles to int64 and sign-extending
        // int32 keys — all bit-exact round-trips through HotCacheLL.
        // Phase B also covers TimeWindow namespace (via `hotcache_fold_tw_key`
        // to fold (key, nsStart, nsEnd) into a single int64) and FIXED_ROW
        // keys (via `hotcache_fold_fixed_row_key`). A fold collision can only
        // cause a cache miss — the SwissTable lookup below always confirms.
        // Variable-length values would require a pointer-mode cache with a
        // SwissTable rehash hook; deferred until that hook exists.
        if (auto* mgr = engine->hot_cache_manager()) {
            const bool scalar_value =
                stored_value_type == StateHandle::ValueType::INT64
                || stored_value_type == StateHandle::ValueType::INT32
                || stored_value_type == StateHandle::ValueType::FLOAT64;
            const bool cacheable_key =
                stored_key_type == StateHandle::KeyType::INT64
                || stored_key_type == StateHandle::KeyType::INT32
                || stored_key_type == StateHandle::KeyType::FIXED_ROW;
            if (mgr->is_active()
                && handle->kind == StateHandle::StateKind::VALUE
                && cacheable_key
                && scalar_value) {
                // Default sets-per-state: 64 sets = 12 KB. acquire_ll rounds
                // DOWN to a power of 2 if the manager is too full.
                auto cache = mgr->acquire_ll(64);
                if (cache) {
                    // Manager retains ownership; release the unique_ptr so
                    // the raw pointer survives until manager teardown.
                    handle->hot_cache_ll = cache.release();
                }
            }
        }

        return to_handle(handle);
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
        auto kt = handle->key_type;
        auto vt = handle->value_type;
        // Dispatch based on stored types (must match registration)
        if (kt == StateHandle::KeyType::INT64 && vt == StateHandle::ValueType::INT64) {
            auto* tbl = handle->engine->get_state_table<int64_t, int64_t>(handle->table_id);
            return tbl ? static_cast<jlong>(tbl->total_size()) : 0;
        }
        if (kt == StateHandle::KeyType::INT64 && vt == StateHandle::ValueType::FLOAT64) {
            auto* tbl = handle->engine->get_state_table<int64_t, double>(handle->table_id);
            return tbl ? static_cast<jlong>(tbl->total_size()) : 0;
        }
        if (kt == StateHandle::KeyType::INT64 && vt == StateHandle::ValueType::BYTES) {
            auto* tbl = handle->engine->get_state_table<int64_t, std::string>(handle->table_id);
            return tbl ? static_cast<jlong>(tbl->total_size()) : 0;
        }
        if (kt == StateHandle::KeyType::INT64 && vt == StateHandle::ValueType::LIST) {
            auto* tbl = handle->engine->get_state_table<int64_t, ElementList>(handle->table_id);
            return tbl ? static_cast<jlong>(tbl->total_size()) : 0;
        }
        if (kt == StateHandle::KeyType::INT64 && vt == StateHandle::ValueType::MAP) {
            // Dispatch based on InnerMap specialization
            switch (handle->map_inner_kind) {
                case StateHandle::MapInnerKind::LONG_LONG: {
                    auto* tbl = handle->engine->get_state_table<int64_t, InnerMapLongLong>(handle->table_id);
                    return tbl ? static_cast<jlong>(tbl->total_size()) : 0;
                }
                case StateHandle::MapInnerKind::LONG_STRING: {
                    auto* tbl = handle->engine->get_state_table<int64_t, InnerMapLongString>(handle->table_id);
                    return tbl ? static_cast<jlong>(tbl->total_size()) : 0;
                }
                case StateHandle::MapInnerKind::STRING_LONG: {
                    auto* tbl = handle->engine->get_state_table<int64_t, InnerMapStringLong>(handle->table_id);
                    return tbl ? static_cast<jlong>(tbl->total_size()) : 0;
                }
                default: {
                    auto* tbl = handle->engine->get_state_table<int64_t, InnerMap>(handle->table_id);
                    return tbl ? static_cast<jlong>(tbl->total_size()) : 0;
                }
            }
        }
        if (kt == StateHandle::KeyType::FIXED_ROW && vt == StateHandle::ValueType::INT64) {
            auto* tbl = handle->engine->get_state_table<FixedRow, int64_t>(handle->table_id);
            return tbl ? static_cast<jlong>(tbl->total_size()) : 0;
        }
        if (kt == StateHandle::KeyType::FIXED_ROW && vt == StateHandle::ValueType::FLOAT64) {
            auto* tbl = handle->engine->get_state_table<FixedRow, double>(handle->table_id);
            return tbl ? static_cast<jlong>(tbl->total_size()) : 0;
        }
        if (kt == StateHandle::KeyType::FIXED_ROW && vt == StateHandle::ValueType::BYTES) {
            auto* tbl = handle->engine->get_state_table<FixedRow, std::string>(handle->table_id);
            return tbl ? static_cast<jlong>(tbl->total_size()) : 0;
        }
        // INT32 key types
        if (kt == StateHandle::KeyType::INT32 && vt == StateHandle::ValueType::INT64) {
            auto* tbl = handle->engine->get_state_table<int32_t, int64_t>(handle->table_id);
            return tbl ? static_cast<jlong>(tbl->total_size()) : 0;
        }
        if (kt == StateHandle::KeyType::INT32 && vt == StateHandle::ValueType::FLOAT64) {
            auto* tbl = handle->engine->get_state_table<int32_t, double>(handle->table_id);
            return tbl ? static_cast<jlong>(tbl->total_size()) : 0;
        }
        if (kt == StateHandle::KeyType::INT32 && vt == StateHandle::ValueType::BYTES) {
            auto* tbl = handle->engine->get_state_table<int32_t, std::string>(handle->table_id);
            return tbl ? static_cast<jlong>(tbl->total_size()) : 0;
        }
        if (kt == StateHandle::KeyType::INT32 && vt == StateHandle::ValueType::LIST) {
            auto* tbl = handle->engine->get_state_table<int32_t, ElementList>(handle->table_id);
            return tbl ? static_cast<jlong>(tbl->total_size()) : 0;
        }
        if (kt == StateHandle::KeyType::INT32 && vt == StateHandle::ValueType::MAP) {
            switch (handle->map_inner_kind) {
                case StateHandle::MapInnerKind::LONG_LONG: {
                    auto* tbl = handle->engine->get_state_table<int32_t, InnerMapLongLong>(handle->table_id);
                    return tbl ? static_cast<jlong>(tbl->total_size()) : 0;
                }
                case StateHandle::MapInnerKind::LONG_STRING: {
                    auto* tbl = handle->engine->get_state_table<int32_t, InnerMapLongString>(handle->table_id);
                    return tbl ? static_cast<jlong>(tbl->total_size()) : 0;
                }
                case StateHandle::MapInnerKind::STRING_LONG: {
                    auto* tbl = handle->engine->get_state_table<int32_t, InnerMapStringLong>(handle->table_id);
                    return tbl ? static_cast<jlong>(tbl->total_size()) : 0;
                }
                default: {
                    auto* tbl = handle->engine->get_state_table<int32_t, InnerMap>(handle->table_id);
                    return tbl ? static_cast<jlong>(tbl->total_size()) : 0;
                }
            }
        }
        if (vt == StateHandle::ValueType::LIST) {
            auto* tbl = handle->engine->get_state_table<std::string, ElementList>(handle->table_id);
            return tbl ? static_cast<jlong>(tbl->total_size()) : 0;
        }
        if (vt == StateHandle::ValueType::MAP) {
            auto* tbl = handle->engine->get_state_table<std::string, InnerMap>(handle->table_id);
            return tbl ? static_cast<jlong>(tbl->total_size()) : 0;
        }
        // Generic fallback: all other types use <std::string, std::string>
        auto* tbl = handle->engine->get_state_table<std::string, std::string>(handle->table_id);
        return tbl ? static_cast<jlong>(tbl->total_size()) : 0;
    })
}

// --- Key enumeration (for getKeys) ---

JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_getStateKeys(
        JNIEnv* env, jclass, jlong stateHandle) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        auto* handle = from_handle<StateHandle>(stateHandle);
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
                emit_keys(handle->engine->get_state_table<int64_t, int64_t>(handle->table_id));
            } else if (vt == StateHandle::ValueType::FLOAT64) {
                emit_keys(handle->engine->get_state_table<int64_t, double>(handle->table_id));
            } else if (vt == StateHandle::ValueType::LIST) {
                emit_keys(handle->engine->get_state_table<int64_t, ElementList>(handle->table_id));
            } else if (vt == StateHandle::ValueType::MAP) {
                switch (handle->map_inner_kind) {
                    case StateHandle::MapInnerKind::LONG_LONG:
                        emit_keys(handle->engine->get_state_table<int64_t, InnerMapLongLong>(handle->table_id));
                        break;
                    case StateHandle::MapInnerKind::LONG_STRING:
                        emit_keys(handle->engine->get_state_table<int64_t, InnerMapLongString>(handle->table_id));
                        break;
                    case StateHandle::MapInnerKind::STRING_LONG:
                        emit_keys(handle->engine->get_state_table<int64_t, InnerMapStringLong>(handle->table_id));
                        break;
                    default:
                        emit_keys(handle->engine->get_state_table<int64_t, InnerMap>(handle->table_id));
                        break;
                }
            } else {
                // BYTES or STRING value type → <int64_t, std::string>
                emit_keys(handle->engine->get_state_table<int64_t, std::string>(handle->table_id));
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
                emit_keys(handle->engine->get_state_table<int32_t, int64_t>(handle->table_id));
            } else if (vt == StateHandle::ValueType::FLOAT64) {
                emit_keys(handle->engine->get_state_table<int32_t, double>(handle->table_id));
            } else {
                emit_keys(handle->engine->get_state_table<int32_t, std::string>(handle->table_id));
            }

            uint8_t* p = buf.data_mut() + count_pos;
            p[0] = (count >> 24) & 0xFF;
            p[1] = (count >> 16) & 0xFF;
            p[2] = (count >> 8) & 0xFF;
            p[3] = count & 0xFF;
        } else {
            auto vt = handle->value_type;
            int32_t count = 0;
            size_t count_pos = buf.size();
            buf.write_int(0);  // placeholder

            auto emit_str_keys = [&](auto* tbl) {
                if (!tbl) return;
                tbl->for_each_void([&](int, const std::string& k, const auto&) {
                    buf.write_int(static_cast<int32_t>(k.size()));
                    buf.write_raw(reinterpret_cast<const uint8_t*>(k.data()), k.size());
                    ++count;
                });
            };

            if (vt == StateHandle::ValueType::LIST) {
                emit_str_keys(handle->engine->get_state_table<std::string, ElementList>(handle->table_id));
            } else if (vt == StateHandle::ValueType::MAP) {
                emit_str_keys(handle->engine->get_state_table<std::string, InnerMap>(handle->table_id));
            } else {
                emit_str_keys(handle->engine->get_state_table<std::string, std::string>(handle->table_id));
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
