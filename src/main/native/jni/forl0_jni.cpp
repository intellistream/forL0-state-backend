// forl0_jni.cpp — JNI entry points for StateEngine lifecycle and state registration.
// Maps to NativeEngine.java native method declarations.

#include <jni.h>
#include "jni_utils.h"
#include "state_engine.h"
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
        JNIEnv* env, jclass, jint startKeyGroup, jint numKeyGroups, jint totalKeyGroups) {
    JNI_ENTRY_RETURN(jlong, 0, {
        auto* engine = new StateEngine(startKeyGroup, numKeyGroups, totalKeyGroups);
        return to_handle(engine);
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_destroyEngine(
        JNIEnv* env, jclass, jlong engineHandle) {
    JNI_ENTRY_VOID({
        delete from_handle<StateEngine>(engineHandle);
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

        // Determine actual storage types (must match Java JNI access patterns)
        StateHandle::KeyType stored_key_type;
        StateHandle::ValueType stored_value_type;

        // CRITICAL: When void_ns is false, Java ALWAYS uses the generic byte[] JNI path
        // (serializing key+namespace together). The C++ table MUST be <std::string, std::string>
        // to match. Typed specializations are ONLY valid for VoidNamespace states.
        if (!void_ns) {
            table_id = engine->register_state<std::string, std::string>(name, void_ns);
            stored_key_type = StateHandle::KeyType::BYTES;
            stored_value_type = StateHandle::ValueType::BYTES;
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
            // MapState: store std::unordered_map<std::string, std::string> (InnerMap) per key
            if (keyTypeId == 2) {
                table_id = engine->register_state<int64_t, InnerMap>(name, void_ns);
                stored_key_type = StateHandle::KeyType::INT64;
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
        handle->void_namespace = void_ns;

        // Register StateHandle pointer in engine for checkpoint type dispatch
        engine->register_state_handle_ptr(table_id, handle);

        // Parse type descriptor if provided.
        // Descriptor format: [key_type][ns_type][value_type] — three sequential layouts.
        // We need the VALUE layout (third element) for checkpoint serialization.
        if (typeDescriptor) {
            jsize descLen = env->GetArrayLength(typeDescriptor);
            if (descLen > 0) {
                std::vector<uint8_t> desc(descLen);
                env->GetByteArrayRegion(typeDescriptor, 0, descLen,
                                        reinterpret_cast<jbyte*>(desc.data()));
                try {
                    size_t offset = 0;
                    // Parse key layout
                    auto key_layout = TypeLayoutParser::parse_at(desc.data(), descLen, offset);
                    // Parse namespace layout (skip it)
                    auto ns_layout = TypeLayoutParser::parse_at(desc.data(), descLen, offset);
                    // Parse value layout — this is what we need
                    auto val_layout = TypeLayoutParser::parse_at(desc.data(), descLen, offset);

                    // Store key layout for FIXED_ROW (needed for arity in checkpoint)
                    if (key_layout && key_layout->type_id == TypeId::FIXED_ROW) {
                        handle->key_layout = std::move(key_layout);
                    }

                    if (val_layout) {
                        // Store the complete value layout (keep tree intact for
                        // checkpoint reader's read_flink_value which accesses children).
                        handle->value_layout = std::move(val_layout);
                    }
                    (void)key_layout;
                    (void)ns_layout;
                } catch (const std::exception&) {
                    // Type descriptor parsing is optional — fallback to default layouts
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
            auto* tbl = handle->engine->get_state_table<int64_t, InnerMap>(handle->table_id);
            return tbl ? static_cast<jlong>(tbl->total_size()) : 0;
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
                emit_keys(handle->engine->get_state_table<int64_t, InnerMap>(handle->table_id));
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
        return string_to_jbytearray(env,
                std::string(reinterpret_cast<const char*>(buf.data()), buf.size()));
    })
}

}  // extern "C"
