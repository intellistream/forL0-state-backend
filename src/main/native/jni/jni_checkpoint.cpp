// jni_checkpoint.cpp — JNI entry points for checkpoint/restore operations.
// Bridges the type-erased StateEngine handles to typed CheckpointWriter/Reader templates.

#include <jni.h>
#include "jni_utils.h"
#include "state_engine.h"
#include "checkpoint_writer.h"
#include "checkpoint_reader.h"
#include "flink_binary_format.h"

using namespace forl0;

// ============================================================================
//  Type dispatch helpers — route type-erased StateHandle to typed template
// ============================================================================

namespace {

// Default TypeLayouts for checkpoint format (one per supported type).
static TypeLayout layout_int32{TypeId::INT32};
static TypeLayout layout_int64{TypeId::INT64};
static TypeLayout layout_float32{TypeId::FLOAT32};
static TypeLayout layout_float64{TypeId::FLOAT64};
static TypeLayout layout_bool{TypeId::BOOL};
static TypeLayout layout_string{TypeId::STRING};
static TypeLayout layout_bytes{TypeId::BYTES};

const TypeLayout& layout_for_key_type(StateHandle::KeyType kt) {
    switch (kt) {
        case StateHandle::KeyType::INT32:  return layout_int32;
        case StateHandle::KeyType::INT64:  return layout_int64;
        case StateHandle::KeyType::STRING: return layout_string;
        default:                           return layout_bytes;
    }
}

const TypeLayout& layout_for_value_type(StateHandle::ValueType vt) {
    switch (vt) {
        case StateHandle::ValueType::INT32:   return layout_int32;
        case StateHandle::ValueType::INT64:   return layout_int64;
        case StateHandle::ValueType::FLOAT32: return layout_float32;
        case StateHandle::ValueType::FLOAT64: return layout_float64;
        case StateHandle::ValueType::BOOL:    return layout_bool;
        case StateHandle::ValueType::STRING:  return layout_string;
        default:                              return layout_bytes;
    }
}

template<typename V, typename N, typename K = WindowStateKey>
size_t write_window_namespaces(const StateHandle& handle, int group, WriteBuffer& out,
                               const TypeLayout& layout) {
    const auto* table = handle.table<K, V>();
    const bool narrow = layout.type_id == TypeId::INT32;
    constexpr int namespace_bytes = std::is_same_v<N, TimeWindow> ? 16 : 8;
    size_t count = 0;
    table->template for_each_snapshot_in_key_group_ns<N>(group,
        [&](const N& ns, const K& key, const V& value) {
            int32_t key_bytes;
            if constexpr (std::is_same_v<K, NamespaceBlob>) key_bytes = static_cast<int32_t>(key.size());
            else key_bytes = handle.row_data_key ? 20 : 8;
            out.write_int(namespace_bytes + key_bytes);
            if constexpr (std::is_same_v<N, TimeWindow>) {
                out.write_long(ns.first);
                out.write_long(ns.second);
            } else {
                out.write_long(ns);
            }
            write_window_state_key(out, key, handle.row_data_key);
            if constexpr (std::is_same_v<V, ElementList>) {
                write_flink_value(out, value, layout);
            } else if constexpr (std::is_same_v<V, NamespaceRow> || is_fixed_window_row<V>) {
                out.write_int(static_cast<int32_t>(value.size()));
                out.write_raw(reinterpret_cast<const uint8_t*>(value.data()), value.size());
            } else if constexpr (std::is_same_v<V, double>) {
                out.write_int(8);
                out.write_double(value);
            } else {
                out.write_int(narrow ? 4 : 8);
                if (narrow) out.write_int(static_cast<int32_t>(value));
                else out.write_long(value);
            }
            ++count;
        });
    return count;
}

template<typename V, typename N, typename K = WindowStateKey>
void read_window_namespaces(StateHandle& handle, int group, ReadBuffer& in, int32_t count,
                            const TypeLayout& layout) {
    auto* table = handle.table<K, V>();
    const bool narrow = std::is_same_v<V, int64_t> && layout.type_id == TypeId::INT32;
    constexpr int namespace_bytes = std::is_same_v<N, TimeWindow> ? 16 : 8;
    for (int32_t i = 0; i < count; ++i) {
        const auto entry_key_bytes = in.read_int();
        if (entry_key_bytes < namespace_bytes || static_cast<size_t>(entry_key_bytes) > in.remaining()) {
            throw std::runtime_error("invalid window namespace key length");
        }
        N ns;
        if constexpr (std::is_same_v<N, TimeWindow>) ns = read_time_window_namespace(in);
        else ns = in.read_long();
        const auto key = [&]() -> K {
            const size_t key_length = entry_key_bytes - namespace_bytes;
            if constexpr (std::is_same_v<K, NamespaceBlob>) {
                K bytes(reinterpret_cast<const char*>(in.current()), key_length);
                in.skip(key_length);
                return bytes;
            } else {
                if (key_length != (handle.row_data_key ? 20u : 8u)) {
                    throw std::runtime_error("invalid numeric window key length");
                }
                return read_window_state_key(in, handle.row_data_key);
            }
        }();
        if constexpr (std::is_same_v<V, ElementList>) {
            table->put(group, ns, key, read_flink_value<ElementList>(in, layout));
        } else if constexpr (std::is_same_v<V, NamespaceRow> || is_fixed_window_row<V>) {
            const int32_t length = in.read_int();
            if (length < 0 || static_cast<size_t>(length) > in.remaining()) {
                throw std::runtime_error("invalid window namespace blob length");
            }
            V value(reinterpret_cast<const char*>(in.current()), static_cast<size_t>(length));
            in.skip(static_cast<size_t>(length));
            table->put(group, ns, key, std::move(value));
        } else {
            if (in.read_int() != (narrow ? 4 : 8)) throw std::runtime_error("invalid window namespace value length");
            if constexpr (std::is_same_v<V, double>) table->put(group, ns, key, in.read_double());
            else table->put(group, ns, key, narrow ? int64_t(in.read_int()) : in.read_long());
        }
    }
}

template<class N, class K = WindowStateKey>
size_t write_window_rows(StateHandle& handle, int group, WriteBuffer& buf, const TypeLayout& layout) {
    return StateHandle::dispatch_window_row(handle.window_row_width, [&](auto tag) {
        return write_window_namespaces<typename decltype(tag)::type, N, K>(handle, group, buf, layout);
    });
}

template<class N, class K = WindowStateKey>
void read_window_rows(StateHandle& handle, int group, ReadBuffer& buf, int32_t count, const TypeLayout& layout) {
    StateHandle::dispatch_window_row(handle.window_row_width, [&](auto tag) {
        read_window_namespaces<typename decltype(tag)::type, N, K>(handle, group, buf, count, layout);
    });
}

// Write entries for one state + one key group into buf.
// Returns entry count written.
size_t write_state_key_group(StateEngine* engine, StateHandle* handle,
                             int key_group, WriteBuffer& buf) {
    bool vns = handle->void_namespace;
    auto kt = handle->key_type;
    auto vt = handle->value_type;
    auto kind = handle->kind;
    int64_t tid = handle->table_id;

    // For FIXED_ROW key, use the stored key_layout (contains arity).
    // For other key types, use the static layout.
    const TypeLayout& kl = ((kt == StateHandle::KeyType::FIXED_ROW || kt == StateHandle::KeyType::FIXED_PAIR) && handle->key_layout)
        ? *handle->key_layout
        : layout_for_key_type(kt);
    // When stored value type is BYTES, values are opaque serialized bytes →
    // always use BYTES layout (4-byte len + data) to match Java FMT_LEN_PREFIXED.
    // For LIST/MAP: use the handle's value_layout if available.
    const TypeLayout& vl = (vt == StateHandle::ValueType::BYTES)
        ? layout_bytes
        : (handle->value_layout ? *handle->value_layout : layout_for_value_type(vt));

    if (handle->numeric_window_lists) {
        if (handle->namespace_serialized_keys) return write_window_namespaces<ElementList, int64_t, NamespaceBlob>(*handle, key_group, buf, vl);
        return write_window_namespaces<ElementList, int64_t>(*handle, key_group, buf, vl);
    }
    if (handle->namespace_value_tables) {
        if (handle->namespace_serialized_keys) {
            if (handle->ns_type == StateHandle::NsType::SQL_WINDOW) {
                return write_window_rows<TimeWindow, NamespaceBlob>(*handle, key_group, buf, vl);
            }
            return write_window_rows<int64_t, NamespaceBlob>(*handle, key_group, buf, vl);
        }
        if (handle->ns_type == StateHandle::NsType::SQL_WINDOW) {
            return write_window_rows<TimeWindow>(*handle, key_group, buf, vl);
        }
        if (vt == StateHandle::ValueType::INT64) {
            return write_window_namespaces<int64_t, int64_t>(*handle, key_group, buf, vl);
        }
        if (vt == StateHandle::ValueType::FLOAT64) {
            return write_window_namespaces<double, int64_t>(*handle, key_group, buf, vl);
        }
        return write_window_rows<int64_t>(*handle, key_group, buf, vl);
    }

    // Macro to reduce boilerplate for each (K,V) combination.
    // Uses the CheckpointStateWriter template to iterate SwissTable entries.
#define WRITE_KG(KT, VT) do {                                                \
        auto* tbl = engine->get_state_table<KT, VT>(tid);                    \
        if (!tbl) return 0;                                                   \
        CheckpointStateWriter<KT, VT> writer(kl, vl, vns);                   \
        if (handle->ns_type == StateHandle::NsType::TIME_WINDOW) {           \
            return writer.write_key_group_tw(*tbl, key_group, buf);          \
        }                                                                     \
        return writer.write_key_group(*tbl, key_group, buf);                  \
    } while (0)

    if (handle->root_rows) {
        if (kt == StateHandle::KeyType::INT64) WRITE_KG(int64_t, NamespaceRow);
        if (kt == StateHandle::KeyType::INT32) WRITE_KG(int32_t, NamespaceRow);
        if (kt == StateHandle::KeyType::FIXED_PAIR) WRITE_KG(FixedPair, NamespaceRow);
        if (kt == StateHandle::KeyType::FIXED_ROW) WRITE_KG(FixedRow, NamespaceRow);
        WRITE_KG(NamespaceBlob, NamespaceRow);
    }

    // LIST state: value is ElementList (std::vector<std::string>)
    if (kind == StateHandle::StateKind::LIST) {
        if (kt == StateHandle::KeyType::INT64) {
            WRITE_KG(int64_t, ElementList);
        } else {
            WRITE_KG(NamespaceBlob, ElementList);
        }
    }

    // MAP state: dispatch based on InnerMap specialization
    if (kind == StateHandle::StateKind::MAP_) {
        if (kt == StateHandle::KeyType::INT64) {
            switch (handle->map_inner_kind) {
                case StateHandle::MapInnerKind::WINDOW_WINDOW:
                    WRITE_KG(int64_t, InnerMapTimeWindow);
                    break;
                case StateHandle::MapInnerKind::LONG_LONG:
                    WRITE_KG(int64_t, InnerMapLongLong);
                    break;
                case StateHandle::MapInnerKind::LONG_STRING:
                    WRITE_KG(int64_t, InnerMapLongString);
                    break;
                case StateHandle::MapInnerKind::STRING_LONG:
                    WRITE_KG(int64_t, InnerMapStringLong);
                    break;
                case StateHandle::MapInnerKind::ROW_FOUR:
                    WRITE_KG(int64_t, InnerMapRowFour);
                    break;
                case StateHandle::MapInnerKind::WIDE_ROW_FOUR:
                    WRITE_KG(int64_t, InnerMapWideRowFour);
                    break;
                case StateHandle::MapInnerKind::STRING_INLINE:
                    WRITE_KG(int64_t, InnerMapInline);
                    break;
                default:
                    WRITE_KG(int64_t, InnerMap);
                    break;
            }
        } else {
            WRITE_KG(NamespaceBlob, InnerMap);
        }
    }

    // REDUCING/AGGREGATING: dispatch based on stored types
    if (kind == StateHandle::StateKind::REDUCING ||
        kind == StateHandle::StateKind::AGGREGATING) {
        if (kt == StateHandle::KeyType::INT64 && vt == StateHandle::ValueType::INT64)
            WRITE_KG(int64_t, int64_t);
        if (kt == StateHandle::KeyType::INT64 && (vt == StateHandle::ValueType::BYTES || vt == StateHandle::ValueType::STRING))
            WRITE_KG(int64_t, NamespaceBlob);
        if (kt == StateHandle::KeyType::BYTES && vt == StateHandle::ValueType::INT64)
            WRITE_KG(NamespaceBlob, int64_t);
        WRITE_KG(NamespaceBlob, NamespaceBlob);
    }

    // VALUE state: dispatch based on stored (key_type, value_type).
    if (kt == StateHandle::KeyType::INT64) {
        if (vt == StateHandle::ValueType::INT64)   WRITE_KG(int64_t, int64_t);
        if (vt == StateHandle::ValueType::FLOAT64) WRITE_KG(int64_t, double);
        if (vt == StateHandle::ValueType::BYTES || vt == StateHandle::ValueType::STRING)
            WRITE_KG(int64_t, NamespaceBlob);
    }
    if (kt == StateHandle::KeyType::INT32) {
        if (vt == StateHandle::ValueType::INT64)   WRITE_KG(int32_t, int64_t);
        if (vt == StateHandle::ValueType::FLOAT64) WRITE_KG(int32_t, double);
        if (vt == StateHandle::ValueType::BYTES || vt == StateHandle::ValueType::STRING)
            WRITE_KG(int32_t, NamespaceBlob);
    }
    if (kt == StateHandle::KeyType::FIXED_PAIR) {
        if (vt == StateHandle::ValueType::INT64)   WRITE_KG(FixedPair, int64_t);
        if (vt == StateHandle::ValueType::FLOAT64) WRITE_KG(FixedPair, double);
        if (vt == StateHandle::ValueType::BYTES || vt == StateHandle::ValueType::STRING)
            WRITE_KG(FixedPair, NamespaceBlob);
    }
    if (kt == StateHandle::KeyType::FIXED_ROW) {
        if (vt == StateHandle::ValueType::INT64)   WRITE_KG(FixedRow, int64_t);
        if (vt == StateHandle::ValueType::FLOAT64) WRITE_KG(FixedRow, double);
        if (vt == StateHandle::ValueType::BYTES || vt == StateHandle::ValueType::STRING)
            WRITE_KG(FixedRow, NamespaceBlob);
    }
    // Default: all other VALUE combos stored as <string, string>
    WRITE_KG(NamespaceBlob, NamespaceBlob);
    
#undef WRITE_KG
}

// Read entries for one state + one key group from reader into the state table.
void read_state_key_group(StateEngine* engine, StateHandle* handle,
                          int key_group, ReadBuffer& reader, int32_t entry_count) {
    bool vns = handle->void_namespace;
    auto kt = handle->key_type;
    auto vt = handle->value_type;
    auto kind = handle->kind;
    int64_t tid = handle->table_id;

    const TypeLayout& kl = ((kt == StateHandle::KeyType::FIXED_ROW || kt == StateHandle::KeyType::FIXED_PAIR) && handle->key_layout)
        ? *handle->key_layout
        : layout_for_key_type(kt);
    // Same BYTES override as write path — see write_state_key_group() comment.
    const TypeLayout& vl = (vt == StateHandle::ValueType::BYTES)
        ? layout_bytes
        : (handle->value_layout ? *handle->value_layout : layout_for_value_type(vt));

    if (handle->numeric_window_lists) {
        if (handle->namespace_serialized_keys) read_window_namespaces<ElementList, int64_t, NamespaceBlob>(*handle, key_group, reader, entry_count, vl);
        else read_window_namespaces<ElementList, int64_t>(*handle, key_group, reader, entry_count, vl);
        return;
    }
    if (handle->namespace_value_tables) {
        if (handle->namespace_serialized_keys) {
            if (handle->ns_type == StateHandle::NsType::SQL_WINDOW) {
                read_window_rows<TimeWindow, NamespaceBlob>(*handle, key_group, reader, entry_count, vl);
            } else {
                read_window_rows<int64_t, NamespaceBlob>(*handle, key_group, reader, entry_count, vl);
            }
        } else if (handle->ns_type == StateHandle::NsType::SQL_WINDOW) {
            read_window_rows<TimeWindow>(*handle, key_group, reader, entry_count, vl);
        } else if (vt == StateHandle::ValueType::INT64) {
            read_window_namespaces<int64_t, int64_t>(*handle, key_group, reader, entry_count, vl);
        } else if (vt == StateHandle::ValueType::FLOAT64) {
            read_window_namespaces<double, int64_t>(*handle, key_group, reader, entry_count, vl);
        } else {
            read_window_rows<int64_t>(*handle, key_group, reader, entry_count, vl);
        }
        return;
    }

#define READ_KG(KT, VT) do {                                                  \
        auto* tbl = engine->get_state_table<KT, VT>(tid);                     \
        if (!tbl) return;                                                      \
        CheckpointStateReader<KT, VT> rd(kl, vl, vns);                        \
        if (handle->ns_type == StateHandle::NsType::TIME_WINDOW) {            \
            rd.read_entries_tw(reader, *tbl, key_group, entry_count);          \
        } else {                                                               \
            rd.read_entries(reader, *tbl, key_group, entry_count);             \
        }                                                                      \
        return;                                                                \
    } while (0)

    if (handle->root_rows) {
        if (kt == StateHandle::KeyType::INT64) READ_KG(int64_t, NamespaceRow);
        if (kt == StateHandle::KeyType::INT32) READ_KG(int32_t, NamespaceRow);
        if (kt == StateHandle::KeyType::FIXED_PAIR) READ_KG(FixedPair, NamespaceRow);
        if (kt == StateHandle::KeyType::FIXED_ROW) READ_KG(FixedRow, NamespaceRow);
        READ_KG(NamespaceBlob, NamespaceRow);
    }

    // LIST state: value is ElementList
    if (kind == StateHandle::StateKind::LIST) {
        if (kt == StateHandle::KeyType::INT64) {
            READ_KG(int64_t, ElementList);
        } else {
            READ_KG(NamespaceBlob, ElementList);
        }
    }

    // MAP state: dispatch based on InnerMap specialization
    if (kind == StateHandle::StateKind::MAP_) {
        if (kt == StateHandle::KeyType::INT64) {
            switch (handle->map_inner_kind) {
                case StateHandle::MapInnerKind::WINDOW_WINDOW:
                    READ_KG(int64_t, InnerMapTimeWindow);
                    break;
                case StateHandle::MapInnerKind::LONG_LONG:
                    READ_KG(int64_t, InnerMapLongLong);
                    break;
                case StateHandle::MapInnerKind::LONG_STRING:
                    READ_KG(int64_t, InnerMapLongString);
                    break;
                case StateHandle::MapInnerKind::STRING_LONG:
                    READ_KG(int64_t, InnerMapStringLong);
                    break;
                case StateHandle::MapInnerKind::ROW_FOUR:
                    READ_KG(int64_t, InnerMapRowFour);
                    break;
                case StateHandle::MapInnerKind::WIDE_ROW_FOUR:
                    READ_KG(int64_t, InnerMapWideRowFour);
                    break;
                case StateHandle::MapInnerKind::STRING_INLINE:
                    READ_KG(int64_t, InnerMapInline);
                    break;
                default:
                    READ_KG(int64_t, InnerMap);
                    break;
            }
        } else {
            READ_KG(NamespaceBlob, InnerMap);
        }
    }

    // REDUCING/AGGREGATING: dispatch based on stored types
    if (kind == StateHandle::StateKind::REDUCING ||
        kind == StateHandle::StateKind::AGGREGATING) {
        if (kt == StateHandle::KeyType::INT64 && vt == StateHandle::ValueType::INT64)
            READ_KG(int64_t, int64_t);
        if (kt == StateHandle::KeyType::INT64 && (vt == StateHandle::ValueType::BYTES || vt == StateHandle::ValueType::STRING))
            READ_KG(int64_t, NamespaceBlob);
        if (kt == StateHandle::KeyType::BYTES && vt == StateHandle::ValueType::INT64)
            READ_KG(NamespaceBlob, int64_t);
        READ_KG(NamespaceBlob, NamespaceBlob);
    }

    // VALUE state: dispatch based on stored (key_type, value_type).
    if (kt == StateHandle::KeyType::INT64) {
        if (vt == StateHandle::ValueType::INT64)   READ_KG(int64_t, int64_t);
        if (vt == StateHandle::ValueType::FLOAT64) READ_KG(int64_t, double);
        if (vt == StateHandle::ValueType::BYTES || vt == StateHandle::ValueType::STRING)
            READ_KG(int64_t, NamespaceBlob);
    }
    if (kt == StateHandle::KeyType::INT32) {
        if (vt == StateHandle::ValueType::INT64)   READ_KG(int32_t, int64_t);
        if (vt == StateHandle::ValueType::FLOAT64) READ_KG(int32_t, double);
        if (vt == StateHandle::ValueType::BYTES || vt == StateHandle::ValueType::STRING)
            READ_KG(int32_t, NamespaceBlob);
    }
    if (kt == StateHandle::KeyType::FIXED_PAIR) {
        if (vt == StateHandle::ValueType::INT64)   READ_KG(FixedPair, int64_t);
        if (vt == StateHandle::ValueType::FLOAT64) READ_KG(FixedPair, double);
        if (vt == StateHandle::ValueType::BYTES || vt == StateHandle::ValueType::STRING)
            READ_KG(FixedPair, NamespaceBlob);
    }
    if (kt == StateHandle::KeyType::FIXED_ROW) {
        if (vt == StateHandle::ValueType::INT64)   READ_KG(FixedRow, int64_t);
        if (vt == StateHandle::ValueType::FLOAT64) READ_KG(FixedRow, double);
        if (vt == StateHandle::ValueType::BYTES || vt == StateHandle::ValueType::STRING)
            READ_KG(FixedRow, NamespaceBlob);
    }
    // Default: all other VALUE combos stored as <string, string>
    READ_KG(NamespaceBlob, NamespaceBlob);

#undef READ_KG
}

}  // anonymous namespace

// ============================================================================
//  JNI functions
// ============================================================================

extern "C" {

// ============================================================================
//  Snapshot preparation
// ============================================================================

JNIEXPORT jlong JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_prepareSnapshot(
        JNIEnv* env, jclass, jlong engineHandle) {
    JNI_ENTRY_RETURN(jlong, 0, {
        auto* engine = from_handle<StateEngine>(engineHandle);
        return static_cast<jlong>(engine->prepare_snapshot());
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_releaseSnapshot(
        JNIEnv* env, jclass, jlong engineHandle) {
    JNI_ENTRY_VOID({
        auto* engine = from_handle<StateEngine>(engineHandle);
        engine->release_snapshot();
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_releaseCheckpointGroup(
        JNIEnv* env, jclass, jlong engineHandle, jint group) {
    JNI_ENTRY_VOID({
        from_handle<StateEngine>(engineHandle)->release_snapshot_group(group);
    })
}

// ============================================================================
//  Write key group data — called per key group during async snapshot.
//  Returns byte[] containing all state entries for this key group in
//  Flink-compatible checkpoint format.
// ============================================================================

JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_writeKeyGroupData(
        JNIEnv* env, jclass, jlong engineHandle, jint keyGroupId) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        auto* engine = from_handle<StateEngine>(engineHandle);
        // Consecutive key groups usually have similar sizes. Reserve from the
        // previous result to avoid repeatedly growing/copying a large payload.
        // Retain only a size hint, never an extra checkpoint byte buffer.
        static thread_local size_t previous_capacity = 65536;
        WriteBuffer buf(previous_capacity);

        auto registered = engine->registered_state_handles<StateHandle>();

        for (auto& [state_id, sh] : registered) {
            // Write stateId
            buf.write_short(static_cast<int16_t>(state_id));

            // Reserve space for entry count, write entries, then patch count
            size_t count_pos = buf.size();
            buf.write_int(0);  // placeholder

            size_t count = write_state_key_group(engine, sh, keyGroupId, buf);

            // Patch the entry count at count_pos
            uint8_t* p = buf.data_mut() + count_pos;
            int32_t c = static_cast<int32_t>(count);
            p[0] = static_cast<uint8_t>((c >> 24) & 0xFF);
            p[1] = static_cast<uint8_t>((c >> 16) & 0xFF);
            p[2] = static_cast<uint8_t>((c >> 8) & 0xFF);
            p[3] = static_cast<uint8_t>(c & 0xFF);
        }

        // End-of-keygroup marker
        buf.write_short(-1);

        if (buf.size() <= 2) return nullptr;  // only marker, no data
        previous_capacity = std::max(size_t(65536), std::min(size_t(256) * 1024 * 1024,
                                                          buf.size() + buf.size() / 8));
        jbyteArray result = env->NewByteArray(static_cast<jsize>(buf.size()));
        env->SetByteArrayRegion(result, 0, static_cast<jsize>(buf.size()),
                reinterpret_cast<const jbyte*>(buf.data()));
        return result;
    })
}

// ============================================================================
//  Read key group data — called per key group during restore.
//  Reads entries from Flink checkpoint format and populates SwissTables.
// ============================================================================

static void restore_key_group(JNIEnv* env, jlong engineHandle, jint keyGroupId,
                              jbyteArray data, jintArray destinationIds) {
    if (!data) return;
    auto* engine = from_handle<StateEngine>(engineHandle);
    std::vector<jint> mapping;
    if (destinationIds) {
        mapping.resize(env->GetArrayLength(destinationIds));
        env->GetIntArrayRegion(destinationIds, 0, static_cast<jsize>(mapping.size()), mapping.data());
        if (env->ExceptionCheck()) return;
    }

    jsize dataLen = env->GetArrayLength(data);
    std::vector<uint8_t> buf(dataLen);
    env->GetByteArrayRegion(data, 0, dataLen, reinterpret_cast<jbyte*>(buf.data()));

    ReadBuffer reader(buf.data(), buf.size());
    auto registered = engine->registered_state_handles<StateHandle>();

    while (reader.remaining() > 0) {
        int16_t stateId = reader.read_short();
        if (stateId < 0) break;  // end-of-keygroup marker

        int32_t entryCount = reader.read_int();
        if (entryCount <= 0) continue;

        // Find the StateHandle for this stateId
        int64_t target = stateId;
        if (destinationIds) {
            if (size_t(stateId) >= mapping.size() || mapping[stateId] < 1) {
                throw std::runtime_error("Unmapped native checkpoint state ID " + std::to_string(stateId));
            }
            target = mapping[stateId];
        }
        auto it = registered.find(target);
        if (it == registered.end()) {
            throw std::runtime_error(
                "readKeyGroupData: unknown stateId " + std::to_string(stateId));
        }

        read_state_key_group(engine, it->second, keyGroupId, reader, entryCount);
    }
}

JNIEXPORT jint JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_checkpointStateId(JNIEnv* env, jclass, jlong state) {
    JNI_ENTRY_RETURN(jint, 0, {
        const auto id = from_handle<StateHandle>(state)->table_id;
        if (id < 1 || id > INT16_MAX) throw std::runtime_error("State ID exceeds the checkpoint format");
        return static_cast<jint>(id);
    })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_readKeyGroupData(
        JNIEnv* env, jclass, jlong engineHandle, jint keyGroupId, jbyteArray data) {
    JNI_ENTRY_VOID({ restore_key_group(env, engineHandle, keyGroupId, data, nullptr); })
}

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_readMappedKeyGroupData(
        JNIEnv* env, jclass, jlong engineHandle, jint keyGroupId, jbyteArray data, jintArray destinationIds) {
    JNI_ENTRY_VOID({ restore_key_group(env, engineHandle, keyGroupId, data, destinationIds); })
}

// ============================================================================
//  Write entries for a single state in a single key group.
//  Used by ForL0KeyValueStateIterator for canonical savepoint writing.
//  Format: [count(4)][entries...] where each entry is [ns][key][value].
// ============================================================================

JNIEXPORT jbyteArray JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_writeStateKeyGroupEntries(
        JNIEnv* env, jclass, jlong stateHandle, jint keyGroupId) {
    JNI_ENTRY_RETURN(jbyteArray, nullptr, {
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* engine = handle->engine;
        WriteBuffer buf;

        // Reserve space for entry count
        size_t count_pos = buf.size();
        buf.write_int(0);  // placeholder

        size_t count = write_state_key_group(engine, handle, keyGroupId, buf);

        if (count == 0) return nullptr;

        // Patch the entry count
        uint8_t* p = buf.data_mut() + count_pos;
        int32_t c = static_cast<int32_t>(count);
        p[0] = static_cast<uint8_t>((c >> 24) & 0xFF);
        p[1] = static_cast<uint8_t>((c >> 16) & 0xFF);
        p[2] = static_cast<uint8_t>((c >> 8) & 0xFF);
        p[3] = static_cast<uint8_t>(c & 0xFF);

        jbyteArray result = env->NewByteArray(static_cast<jsize>(buf.size()));
        env->SetByteArrayRegion(result, 0, static_cast<jsize>(buf.size()),
                reinterpret_cast<const jbyte*>(buf.data()));
        return result;
    })
}

// ============================================================================
//  Read entries for a single state in a single key group.
//  Used for restoring from canonical savepoint format.
//  Data format: [count(4)][entries...] matching the checkpoint binary format.
// ============================================================================

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_readStateKeyGroupEntries(
        JNIEnv* env, jclass, jlong stateHandle, jint keyGroupId, jbyteArray data) {
    JNI_ENTRY_VOID({
        if (!data) return;
        auto* handle = from_handle<StateHandle>(stateHandle);
        auto* engine = handle->engine;

        jsize dataLen = env->GetArrayLength(data);
        std::vector<uint8_t> buf(dataLen);
        env->GetByteArrayRegion(data, 0, dataLen, reinterpret_cast<jbyte*>(buf.data()));

        ReadBuffer reader(buf.data(), buf.size());
        int32_t entryCount = reader.read_int();
        if (entryCount <= 0) return;

        read_state_key_group(engine, handle, keyGroupId, reader, entryCount);
    })
}

}  // extern "C"
