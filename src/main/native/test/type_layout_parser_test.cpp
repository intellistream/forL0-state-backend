// TypeLayoutParser tests — verifies parsing of Java-generated descriptor bytes.

#ifdef FORL0_USE_MINI_GTEST
#include "mini_gtest.h"
#else
#include <gtest/gtest.h>
#endif
#include "type_layout.h"

using namespace forl0;

// ============================================================================
//  Primitive Types
// ============================================================================

TEST(TypeLayoutParserTest, ParseInt32) {
    uint8_t desc[] = {static_cast<uint8_t>(TypeId::INT32)};
    auto layout = TypeLayoutParser::parse(desc, sizeof(desc));
    ASSERT_EQ(layout->type_id, TypeId::INT32);
    ASSERT_EQ(layout->cpp_size, 4u);
}

TEST(TypeLayoutParserTest, ParseInt64) {
    uint8_t desc[] = {static_cast<uint8_t>(TypeId::INT64)};
    auto layout = TypeLayoutParser::parse(desc, sizeof(desc));
    ASSERT_EQ(layout->type_id, TypeId::INT64);
    ASSERT_EQ(layout->cpp_size, 8u);
}

TEST(TypeLayoutParserTest, ParseFloat32) {
    uint8_t desc[] = {static_cast<uint8_t>(TypeId::FLOAT32)};
    auto layout = TypeLayoutParser::parse(desc, sizeof(desc));
    ASSERT_EQ(layout->type_id, TypeId::FLOAT32);
    ASSERT_EQ(layout->cpp_size, 4u);
}

TEST(TypeLayoutParserTest, ParseFloat64) {
    uint8_t desc[] = {static_cast<uint8_t>(TypeId::FLOAT64)};
    auto layout = TypeLayoutParser::parse(desc, sizeof(desc));
    ASSERT_EQ(layout->type_id, TypeId::FLOAT64);
    ASSERT_EQ(layout->cpp_size, 8u);
}

TEST(TypeLayoutParserTest, ParseBool) {
    uint8_t desc[] = {static_cast<uint8_t>(TypeId::BOOL)};
    auto layout = TypeLayoutParser::parse(desc, sizeof(desc));
    ASSERT_EQ(layout->type_id, TypeId::BOOL);
    ASSERT_EQ(layout->cpp_size, 1u);
}

TEST(TypeLayoutParserTest, ParseString) {
    uint8_t desc[] = {static_cast<uint8_t>(TypeId::STRING)};
    auto layout = TypeLayoutParser::parse(desc, sizeof(desc));
    ASSERT_EQ(layout->type_id, TypeId::STRING);
    ASSERT_EQ(layout->cpp_size, sizeof(std::string));
}

TEST(TypeLayoutParserTest, ParseBytes) {
    uint8_t desc[] = {static_cast<uint8_t>(TypeId::BYTES)};
    auto layout = TypeLayoutParser::parse(desc, sizeof(desc));
    ASSERT_EQ(layout->type_id, TypeId::BYTES);
    ASSERT_EQ(layout->cpp_size, sizeof(std::string));
}

// ============================================================================
//  FIXED_ROW
// ============================================================================

TEST(TypeLayoutParserTest, ParseFixedRowArity3) {
    // [FIXED_ROW(0x0d), arity_hi(0), arity_lo(3)]
    uint8_t desc[] = {
        static_cast<uint8_t>(TypeId::FIXED_ROW),
        0x00, 0x03  // arity = 3
    };
    auto layout = TypeLayoutParser::parse(desc, sizeof(desc));
    ASSERT_EQ(layout->type_id, TypeId::FIXED_ROW);
    ASSERT_EQ(layout->fixed_row_arity, 3u);
    ASSERT_EQ(layout->cpp_size, sizeof(FixedRow));
}

TEST(TypeLayoutParserTest, ParseFixedRowArity8) {
    uint8_t desc[] = {
        static_cast<uint8_t>(TypeId::FIXED_ROW),
        0x00, 0x08  // max arity
    };
    auto layout = TypeLayoutParser::parse(desc, sizeof(desc));
    ASSERT_EQ(layout->fixed_row_arity, 8u);
}

TEST(TypeLayoutParserTest, ParseFixedRowArity1) {
    uint8_t desc[] = {
        static_cast<uint8_t>(TypeId::FIXED_ROW),
        0x00, 0x01
    };
    auto layout = TypeLayoutParser::parse(desc, sizeof(desc));
    ASSERT_EQ(layout->fixed_row_arity, 1u);
}

// ============================================================================
//  LIST
// ============================================================================

TEST(TypeLayoutParserTest, ParseListInt64) {
    // LIST, followed by INT64 element
    uint8_t desc[] = {
        static_cast<uint8_t>(TypeId::LIST),
        static_cast<uint8_t>(TypeId::INT64)
    };
    auto layout = TypeLayoutParser::parse(desc, sizeof(desc));
    ASSERT_EQ(layout->type_id, TypeId::LIST);
    ASSERT_EQ(layout->children.size(), 1u);
    ASSERT_EQ(layout->children[0]->type_id, TypeId::INT64);
}

TEST(TypeLayoutParserTest, ParseListString) {
    uint8_t desc[] = {
        static_cast<uint8_t>(TypeId::LIST),
        static_cast<uint8_t>(TypeId::STRING)
    };
    auto layout = TypeLayoutParser::parse(desc, sizeof(desc));
    ASSERT_EQ(layout->type_id, TypeId::LIST);
    ASSERT_EQ(layout->children[0]->type_id, TypeId::STRING);
}

// ============================================================================
//  MAP
// ============================================================================

TEST(TypeLayoutParserTest, ParseMapStringString) {
    // MAP, followed by key type STRING, then value type STRING
    uint8_t desc[] = {
        static_cast<uint8_t>(TypeId::MAP),
        static_cast<uint8_t>(TypeId::STRING),
        static_cast<uint8_t>(TypeId::STRING)
    };
    auto layout = TypeLayoutParser::parse(desc, sizeof(desc));
    ASSERT_EQ(layout->type_id, TypeId::MAP);
    ASSERT_EQ(layout->children.size(), 2u);
    ASSERT_EQ(layout->children[0]->type_id, TypeId::STRING);
    ASSERT_EQ(layout->children[1]->type_id, TypeId::STRING);
}

TEST(TypeLayoutParserTest, ParseMapInt64Int64) {
    uint8_t desc[] = {
        static_cast<uint8_t>(TypeId::MAP),
        static_cast<uint8_t>(TypeId::INT64),
        static_cast<uint8_t>(TypeId::INT64)
    };
    auto layout = TypeLayoutParser::parse(desc, sizeof(desc));
    ASSERT_EQ(layout->children[0]->type_id, TypeId::INT64);
    ASSERT_EQ(layout->children[1]->type_id, TypeId::INT64);
}

// ============================================================================
//  Namespace Types
// ============================================================================

TEST(TypeLayoutParserTest, ParseVoidNamespace) {
    uint8_t desc[] = {static_cast<uint8_t>(TypeId::VOID_NS)};
    auto layout = TypeLayoutParser::parse(desc, sizeof(desc));
    ASSERT_EQ(layout->type_id, TypeId::VOID_NS);
    ASSERT_EQ(layout->cpp_size, 0u);
}

TEST(TypeLayoutParserTest, ParseTimeWindow) {
    uint8_t desc[] = {static_cast<uint8_t>(TypeId::TIME_WINDOW)};
    auto layout = TypeLayoutParser::parse(desc, sizeof(desc));
    ASSERT_EQ(layout->type_id, TypeId::TIME_WINDOW);
    ASSERT_EQ(layout->cpp_size, sizeof(TimeWindow));
}

// ============================================================================
//  Error Cases
// ============================================================================

TEST(TypeLayoutParserTest, EmptyDescriptorThrows) {
    uint8_t desc[] = {0};
    ASSERT_THROW(TypeLayoutParser::parse(desc, 0), std::runtime_error);
}

TEST(TypeLayoutParserTest, UnknownTypeIdThrows) {
    uint8_t desc[] = {0xFF};  // invalid type id
    ASSERT_THROW(TypeLayoutParser::parse(desc, sizeof(desc)), std::runtime_error);
}

TEST(TypeLayoutParserTest, TruncatedFixedRowThrows) {
    // FIXED_ROW without arity bytes
    uint8_t desc[] = {static_cast<uint8_t>(TypeId::FIXED_ROW)};
    ASSERT_THROW(TypeLayoutParser::parse(desc, sizeof(desc)), std::runtime_error);
}

// ============================================================================
//  parse_at with offset
// ============================================================================

TEST(TypeLayoutParserTest, ParseAtOffset) {
    // Two types packed: INT32, then INT64
    uint8_t desc[] = {
        static_cast<uint8_t>(TypeId::INT32),
        static_cast<uint8_t>(TypeId::INT64)
    };

    size_t offset = 0;
    auto layout1 = TypeLayoutParser::parse_at(desc, sizeof(desc), offset);
    ASSERT_EQ(layout1->type_id, TypeId::INT32);
    ASSERT_EQ(offset, 1u);

    auto layout2 = TypeLayoutParser::parse_at(desc, sizeof(desc), offset);
    ASSERT_EQ(layout2->type_id, TypeId::INT64);
    ASSERT_EQ(offset, 2u);
}
