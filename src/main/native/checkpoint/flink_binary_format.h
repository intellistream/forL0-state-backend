// Flink binary format reproduction in C++.
// Writes/reads data in the exact same format as Flink's built-in TypeSerializers
// so that checkpoint data is compatible with Flink Heap StateBackend.
//
// All multi-byte integers are big-endian (Java DataOutputStream convention).

#pragma once

#include "type_layout.h"

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include <stdexcept>
#include <algorithm>

namespace forl0 {

// ============================================================================
//  WriteBuffer — accumulates bytes for a key group's checkpoint data
// ============================================================================

class WriteBuffer {
public:
    explicit WriteBuffer(size_t initial_capacity = 65536) {
        buf_.reserve(initial_capacity);
    }

    void clear() { buf_.clear(); }
    const uint8_t* data() const { return buf_.data(); }
    uint8_t* data_mut() { return buf_.data(); }
    size_t size() const { return buf_.size(); }

    // --- Primitive writes (all big-endian) ---

    void write_byte(uint8_t v) {
        buf_.push_back(v);
    }

    void write_bool(bool v) {
        buf_.push_back(v ? 1 : 0);
    }

    void write_short(int16_t v) {
        buf_.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
        buf_.push_back(static_cast<uint8_t>(v & 0xFF));
    }

    void write_int(int32_t v) {
        buf_.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
        buf_.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
        buf_.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
        buf_.push_back(static_cast<uint8_t>(v & 0xFF));
    }

    void write_long(int64_t v) {
        for (int i = 56; i >= 0; i -= 8) {
            buf_.push_back(static_cast<uint8_t>((v >> i) & 0xFF));
        }
    }

    void write_float(float v) {
        int32_t bits;
        std::memcpy(&bits, &v, 4);
        write_int(bits);
    }

    void write_double(double v) {
        int64_t bits;
        std::memcpy(&bits, &v, 8);
        write_long(bits);
    }

    // --- Flink StringValue format: VarInt(strlen+1) + VarInt-encoded chars ---
    // Flink StringSerializer uses StringValue.writeString which encodes:
    //   1. VarInt(charCount + 1)   (0 means null string)
    //   2. For each char: VarInt(charCode)   (ASCII chars < 128 → 1 byte)

    void write_string_flink(const std::string& s) {
        // Flink StringValue format: VarInt(charCount+1) + VarInt-encoded chars
        // For our use case, strings are stored as raw bytes from Java serialization.
        // We write length+1 as VarInt, then each byte as a VarInt-encoded char.
        // For ASCII chars (< 128), this is single bytes — same as raw.
        write_varint(static_cast<int32_t>(s.size() + 1));
        for (size_t i = 0; i < s.size(); ++i) {
            uint32_t c = static_cast<uint8_t>(s[i]);
            if (c < 0x80) {
                buf_.push_back(static_cast<uint8_t>(c));
            } else {
                // VarInt encode the char code
                write_varint(static_cast<int32_t>(c));
            }
        }
    }

    // --- Flink byte[] format: 4-byte length + raw bytes ---

    void write_bytes_flink(const std::string& s) {
        write_int(static_cast<int32_t>(s.size()));
        buf_.insert(buf_.end(), s.data(), s.data() + s.size());
    }

    // --- Raw bytes ---

    void write_raw(const uint8_t* data, size_t len) {
        buf_.insert(buf_.end(), data, data + len);
    }

    // OPT-9: Patch a big-endian int32 at a previously written position.
    // Used with the "placeholder + patch" pattern to avoid temp WriteBuffer.
    void patch_int(size_t offset, int32_t v) {
        buf_[offset]     = static_cast<uint8_t>((v >> 24) & 0xFF);
        buf_[offset + 1] = static_cast<uint8_t>((v >> 16) & 0xFF);
        buf_[offset + 2] = static_cast<uint8_t>((v >> 8) & 0xFF);
        buf_[offset + 3] = static_cast<uint8_t>(v & 0xFF);
    }

    // --- VarInt (used in some Flink formats) ---

    void write_varint(int32_t value) {
        uint32_t v = static_cast<uint32_t>(value);
        while (v > 0x7F) {
            buf_.push_back(static_cast<uint8_t>((v & 0x7F) | 0x80));
            v >>= 7;
        }
        buf_.push_back(static_cast<uint8_t>(v));
    }

    // --- Write a typed value according to TypeLayout ---

    void write_typed_value(const void* data, const TypeLayout& layout) {
        switch (layout.type_id) {
            case TypeId::INT32:
                write_int(*static_cast<const int32_t*>(data));
                break;
            case TypeId::INT64:
                write_long(*static_cast<const int64_t*>(data));
                break;
            case TypeId::FLOAT32:
                write_float(*static_cast<const float*>(data));
                break;
            case TypeId::FLOAT64:
                write_double(*static_cast<const double*>(data));
                break;
            case TypeId::BOOL:
                write_bool(*static_cast<const bool*>(data));
                break;
            case TypeId::STRING:
                write_string_flink(*static_cast<const std::string*>(data));
                break;
            case TypeId::BYTES:
                write_bytes_flink(*static_cast<const std::string*>(data));
                break;
            case TypeId::STRUCT: {
                auto base = static_cast<const uint8_t*>(data);
                for (const auto& f : layout.fields) {
                    if (f.child_index >= 0) {
                        write_typed_value(base + f.cpp_offset, *layout.children[f.child_index]);
                    } else {
                        TypeLayout tmp(f.type_id);
                        write_typed_value(base + f.cpp_offset, tmp);
                    }
                }
                break;
            }
            case TypeId::LIST: {
                // OPT-9: Direct write with placeholder length
                auto* vec = static_cast<const ElementList*>(data);
                size_t len_pos = buf_.size();
                write_int(0);  // placeholder
                write_int(static_cast<int32_t>(vec->size()));
                for (const auto& elem : *vec) {
                    write_raw(reinterpret_cast<const uint8_t*>(elem.data()), elem.size());
                }
                patch_int(len_pos, static_cast<int32_t>(buf_.size() - len_pos - 4));
                break;
            }
            case TypeId::MAP: {
                // OPT-9: Direct write with placeholder length
                auto* inner_map = static_cast<const InnerMap*>(data);
                size_t len_pos = buf_.size();
                write_int(0);  // placeholder
                write_int(static_cast<int32_t>(inner_map->size()));
                for (const auto& [uk, uv] : *inner_map) {
                    write_raw(reinterpret_cast<const uint8_t*>(uk.data()), uk.size());
                    write_raw(reinterpret_cast<const uint8_t*>(uv.data()), uv.size());
                }
                patch_int(len_pos, static_cast<int32_t>(buf_.size() - len_pos - 4));
                break;
            }
            case TypeId::FIXED_ROW: {
                // FixedRow: write as BinaryRowData format (length-prefixed)
                auto* row = static_cast<const FixedRow*>(data);
                uint8_t arity = layout.fixed_row_arity;
                int32_t total_size = 8 + static_cast<int32_t>(arity) * 8;
                write_int(total_size);
                for (int i = 0; i < 8; ++i) write_byte(0);  // header
                for (uint8_t i = 0; i < arity; ++i) {
                    int64_t f = row->f[i];
                    for (int b = 0; b < 8; ++b) {
                        write_byte(static_cast<uint8_t>((f >> (b * 8)) & 0xFF));
                    }
                }
                break;
            }
            default:
                break;
        }
    }

private:
    std::vector<uint8_t> buf_;
};

// ============================================================================
//  ReadBuffer — reads from a byte buffer
// ============================================================================

class ReadBuffer {
public:
    ReadBuffer(const uint8_t* data, size_t len)
        : data_(data), len_(len), pos_(0) {}

    size_t remaining() const { return len_ - pos_; }
    const uint8_t* current() const { return data_ + pos_; }
    size_t position() const { return pos_; }

    uint8_t read_byte() {
        check(1);
        return data_[pos_++];
    }

    bool read_bool() {
        return read_byte() != 0;
    }

    int16_t read_short() {
        check(2);
        int16_t v = (static_cast<int16_t>(data_[pos_]) << 8) | data_[pos_ + 1];
        pos_ += 2;
        return v;
    }

    int32_t read_int() {
        check(4);
        int32_t v = (static_cast<int32_t>(data_[pos_]) << 24)
                  | (static_cast<int32_t>(data_[pos_ + 1]) << 16)
                  | (static_cast<int32_t>(data_[pos_ + 2]) << 8)
                  | data_[pos_ + 3];
        pos_ += 4;
        return v;
    }

    int64_t read_long() {
        check(8);
        int64_t v = 0;
        for (int i = 0; i < 8; ++i) {
            v = (v << 8) | data_[pos_ + i];
        }
        pos_ += 8;
        return v;
    }

    float read_float() {
        int32_t bits = read_int();
        float v;
        std::memcpy(&v, &bits, 4);
        return v;
    }

    double read_double() {
        int64_t bits = read_long();
        double v;
        std::memcpy(&v, &bits, 8);
        return v;
    }

    std::string read_string_flink() {
        // Flink StringValue format: VarInt(charCount+1) + VarInt-encoded chars
        int32_t len_plus1 = read_varint();
        if (len_plus1 <= 0) return {};
        int32_t char_count = len_plus1 - 1;
        std::string s;
        s.reserve(char_count);
        for (int32_t i = 0; i < char_count; ++i) {
            // Each char is VarInt-encoded; ASCII chars (< 128) are 1 byte
            int32_t c = read_varint();
            if (c < 0x80) {
                s.push_back(static_cast<char>(c));
            } else if (c < 0x800) {
                // 2-byte UTF-8
                s.push_back(static_cast<char>(0xC0 | (c >> 6)));
                s.push_back(static_cast<char>(0x80 | (c & 0x3F)));
            } else {
                // 3-byte UTF-8
                s.push_back(static_cast<char>(0xE0 | (c >> 12)));
                s.push_back(static_cast<char>(0x80 | ((c >> 6) & 0x3F)));
                s.push_back(static_cast<char>(0x80 | (c & 0x3F)));
            }
        }
        return s;
    }

    std::string read_bytes_flink() {
        int32_t len = read_int();
        if (len < 0) throw std::runtime_error("negative byte array length");
        check(static_cast<size_t>(len));
        std::string s(reinterpret_cast<const char*>(data_ + pos_), len);
        pos_ += len;
        return s;
    }

    int32_t read_varint() {
        uint32_t result = 0;
        int shift = 0;
        while (true) {
            uint8_t b = read_byte();
            result |= static_cast<uint32_t>(b & 0x7F) << shift;
            if ((b & 0x80) == 0) break;
            shift += 7;
        }
        return static_cast<int32_t>(result);
    }

    void read_raw(uint8_t* dst, size_t len) {
        check(len);
        std::memcpy(dst, data_ + pos_, len);
        pos_ += len;
    }

    void skip(size_t n) {
        check(n);
        pos_ += n;
    }

    // Skip a typed value in the buffer (advance position past it).
    void skip_typed_value(const TypeLayout& layout) {
        switch (layout.type_id) {
            case TypeId::INT32:
            case TypeId::FLOAT32:
                skip(4); break;
            case TypeId::INT64:
            case TypeId::FLOAT64:
                skip(8); break;
            case TypeId::BOOL:
                skip(1); break;
            case TypeId::STRING: {
                // Flink StringValue format: VarInt(charCount+1) + VarInt-encoded chars
                int32_t len_plus1 = read_varint();
                if (len_plus1 > 0) {
                    int32_t char_count = len_plus1 - 1;
                    for (int32_t i = 0; i < char_count; ++i) {
                        // Skip each VarInt-encoded char
                        uint8_t b = read_byte();
                        while (b >= 0x80) b = read_byte();
                    }
                }
                break;
            }
            case TypeId::BYTES: {
                int32_t len = read_int();
                if (len > 0) skip(static_cast<size_t>(len));
                break;
            }
            case TypeId::LIST:
            case TypeId::MAP: {
                // Length-prefixed: [total_byte_length(4)][content...]
                int32_t len = read_int();
                if (len > 0) skip(static_cast<size_t>(len));
                break;
            }
            default:
                throw std::runtime_error("ReadBuffer: cannot skip unknown type");
        }
    }

    // Read a serialized element and return the raw bytes as a string.
    // The element format is determined by the TypeLayout.
    std::string read_element_bytes(const TypeLayout& layout) {
        size_t start = pos_;
        skip_typed_value(layout);
        return std::string(reinterpret_cast<const char*>(data_ + start), pos_ - start);
    }

private:
    void check(size_t needed) const {
        if (pos_ + needed > len_) {
            throw std::runtime_error("ReadBuffer: unexpected end of data");
        }
    }

    const uint8_t* data_;
    size_t len_;
    size_t pos_;
};

}  // namespace forl0
