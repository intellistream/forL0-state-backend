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
    const TypeLayout& kl = (kt == StateHandle::KeyType::FIXED_ROW && handle->key_layout)
        ? *handle->key_layout
        : layout_for_key_type(kt);
    // When stored value type is BYTES, values are opaque serialized bytes →
    // always use BYTES layout (4-byte len + data) to match Java FMT_LEN_PREFIXED.
    // For LIST/MAP: use the handle's value_layout if available.
    const TypeLayout& vl = (vt == StateHandle::ValueType::BYTES)
        ? layout_bytes
        : (handle->value_layout ? *handle->value_layout : layout_for_value_type(vt));

    // Macro to reduce boilerplate for each (K,V) combination.
    // Uses the CheckpointStateWriter template to iterate SwissTable entries.
#define WRITE_KG(KT, VT) do {                                                \
        auto* tbl = engine->get_state_table<KT, VT>(tid);                    \
        if (!tbl) return 0;                                                   \
        CheckpointStateWriter<KT, VT> writer(kl, vl, vns);                   \
        return writer.write_key_group(*tbl, key_group, buf);                  \
    } while (0)

    // LIST state: value is ElementList (std::vector<std::string>)
    if (kind == StateHandle::StateKind::LIST) {
        if (kt == StateHandle::KeyType::INT64) {
            WRITE_KG(int64_t, ElementList);
        } else {
            WRITE_KG(std::string, ElementList);
        }
    }

    // MAP state: value is InnerMap (std::unordered_map<std::string, std::string>)
    if (kind == StateHandle::StateKind::MAP_) {
        if (kt == StateHandle::KeyType::INT64) {
            WRITE_KG(int64_t, InnerMap);
        } else {
            WRITE_KG(std::string, InnerMap);
        }
    }

    // REDUCING/AGGREGATING: dispatch based on stored types
    if (kind == StateHandle::StateKind::REDUCING ||
        kind == StateHandle::StateKind::AGGREGATING) {
        if (kt == StateHandle::KeyType::INT64 && vt == StateHandle::ValueType::INT64)
            WRITE_KG(int64_t, int64_t);
        if (kt == StateHandle::KeyType::INT64 && (vt == StateHandle::ValueType::BYTES || vt == StateHandle::ValueType::STRING))
            WRITE_KG(int64_t, std::string);
        WRITE_KG(std::string, std::string);
    }

    // VALUE state: dispatch based on stored (key_type, value_type).
    if (kt == StateHandle::KeyType::INT64) {
        if (vt == StateHandle::ValueType::INT64)   WRITE_KG(int64_t, int64_t);
        if (vt == StateHandle::ValueType::FLOAT64) WRITE_KG(int64_t, double);
        if (vt == StateHandle::ValueType::BYTES || vt == StateHandle::ValueType::STRING)
            WRITE_KG(int64_t, std::string);
    }
    if (kt == StateHandle::KeyType::INT32) {
        if (vt == StateHandle::ValueType::INT64)   WRITE_KG(int32_t, int64_t);
        if (vt == StateHandle::ValueType::FLOAT64) WRITE_KG(int32_t, double);
        if (vt == StateHandle::ValueType::BYTES || vt == StateHandle::ValueType::STRING)
            WRITE_KG(int32_t, std::string);
    }
    if (kt == StateHandle::KeyType::FIXED_ROW) {
        if (vt == StateHandle::ValueType::INT64)   WRITE_KG(FixedRow, int64_t);
        if (vt == StateHandle::ValueType::FLOAT64) WRITE_KG(FixedRow, double);
        if (vt == StateHandle::ValueType::BYTES || vt == StateHandle::ValueType::STRING)
            WRITE_KG(FixedRow, std::string);
    }
    // Default: all other VALUE combos stored as <string, string>
    WRITE_KG(std::string, std::string);
    
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

    const TypeLayout& kl = (kt == StateHandle::KeyType::FIXED_ROW && handle->key_layout)
        ? *handle->key_layout
        : layout_for_key_type(kt);
    // Same BYTES override as write path — see write_state_key_group() comment.
    const TypeLayout& vl = (vt == StateHandle::ValueType::BYTES)
        ? layout_bytes
        : (handle->value_layout ? *handle->value_layout : layout_for_value_type(vt));

#define READ_KG(KT, VT) do {                                                  \
        auto* tbl = engine->get_state_table<KT, VT>(tid);                     \
        if (!tbl) return;                                                      \
        CheckpointStateReader<KT, VT> rd(kl, vl, vns);                        \
        rd.read_entries(reader, *tbl, key_group, entry_count);                 \
        return;                                                                \
    } while (0)

    // LIST state: value is ElementList
    if (kind == StateHandle::StateKind::LIST) {
        if (kt == StateHandle::KeyType::INT64) {
            READ_KG(int64_t, ElementList);
        } else {
            READ_KG(std::string, ElementList);
        }
    }

    // MAP state: value is InnerMap
    if (kind == StateHandle::StateKind::MAP_) {
        if (kt == StateHandle::KeyType::INT64) {
            READ_KG(int64_t, InnerMap);
        } else {
            READ_KG(std::string, InnerMap);
        }
    }

    // REDUCING/AGGREGATING: dispatch based on stored types
    if (kind == StateHandle::StateKind::REDUCING ||
        kind == StateHandle::StateKind::AGGREGATING) {
        if (kt == StateHandle::KeyType::INT64 && vt == StateHandle::ValueType::INT64)
            READ_KG(int64_t, int64_t);
        if (kt == StateHandle::KeyType::INT64 && (vt == StateHandle::ValueType::BYTES || vt == StateHandle::ValueType::STRING))
            READ_KG(int64_t, std::string);
        READ_KG(std::string, std::string);
    }

    // VALUE state: dispatch based on stored (key_type, value_type).
    if (kt == StateHandle::KeyType::INT64) {
        if (vt == StateHandle::ValueType::INT64)   READ_KG(int64_t, int64_t);
        if (vt == StateHandle::ValueType::FLOAT64) READ_KG(int64_t, double);
        if (vt == StateHandle::ValueType::BYTES || vt == StateHandle::ValueType::STRING)
            READ_KG(int64_t, std::string);
    }
    if (kt == StateHandle::KeyType::INT32) {
        if (vt == StateHandle::ValueType::INT64)   READ_KG(int32_t, int64_t);
        if (vt == StateHandle::ValueType::FLOAT64) READ_KG(int32_t, double);
        if (vt == StateHandle::ValueType::BYTES || vt == StateHandle::ValueType::STRING)
            READ_KG(int32_t, std::string);
    }
    if (kt == StateHandle::KeyType::FIXED_ROW) {
        if (vt == StateHandle::ValueType::INT64)   READ_KG(FixedRow, int64_t);
        if (vt == StateHandle::ValueType::FLOAT64) READ_KG(FixedRow, double);
        if (vt == StateHandle::ValueType::BYTES || vt == StateHandle::ValueType::STRING)
            READ_KG(FixedRow, std::string);
    }
    // Default: all other VALUE combos stored as <string, string>
    READ_KG(std::string, std::string);

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
        WriteBuffer buf;

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
        return string_to_jbytearray(env,
                std::string(reinterpret_cast<const char*>(buf.data()), buf.size()));
    })
}

// ============================================================================
//  Read key group data — called per key group during restore.
//  Reads entries from Flink checkpoint format and populates SwissTables.
// ============================================================================

JNIEXPORT void JNICALL
Java_org_apache_flink_state_forl0_NativeEngine_readKeyGroupData(
        JNIEnv* env, jclass, jlong engineHandle, jint keyGroupId, jbyteArray data) {
    JNI_ENTRY_VOID({
        if (!data) return;
        auto* engine = from_handle<StateEngine>(engineHandle);

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
            auto it = registered.find(static_cast<int64_t>(stateId));
            if (it == registered.end()) {
                throw std::runtime_error(
                    "readKeyGroupData: unknown stateId " + std::to_string(stateId));
            }

            read_state_key_group(engine, it->second, keyGroupId, reader, entryCount);
        }
    })
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

        return string_to_jbytearray(env,
                std::string(reinterpret_cast<const char*>(buf.data()), buf.size()));
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
