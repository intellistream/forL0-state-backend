#pragma once
#include "type_layout.h"
#include <utility>
#include <variant>

namespace forl0 {
// All JNI iterator producers and consumers must share the same layout.
struct NativeMapIterator {
    using Strings = std::pair<InnerMap::const_iterator, InnerMap::const_iterator>;
    using Windows = std::pair<InnerMapTimeWindow::const_iterator, InnerMapTimeWindow::const_iterator>;
    using Inline = std::pair<InnerMapInline::const_iterator, InnerMapInline::const_iterator>;
    using RowFour = std::pair<InnerMapRowFour::const_iterator, InnerMapRowFour::const_iterator>;
    using WideRowFour = std::pair<InnerMapWideRowFour::const_iterator, InnerMapWideRowFour::const_iterator>;
    std::variant<InnerMap, InnerMapTimeWindow, InnerMapInline, InnerMapRowFour, InnerMapWideRowFour> snapshot;
    std::variant<Strings, Windows, Inline, RowFour, WideRowFour> range;

    explicit NativeMapIterator(const InnerMap& value) : snapshot(value) {
        const auto& copy = std::get<InnerMap>(snapshot);
        range = Strings{copy.cbegin(), copy.cend()};
    }
    explicit NativeMapIterator(const InnerMapRowFour& value) : snapshot(value) {
        const auto& copy = std::get<InnerMapRowFour>(snapshot);
        range = RowFour{copy.cbegin(), copy.cend()};
    }
    explicit NativeMapIterator(const InnerMapWideRowFour& value) : snapshot(value) {
        const auto& copy = std::get<InnerMapWideRowFour>(snapshot);
        range = WideRowFour{copy.cbegin(), copy.cend()};
    }
    explicit NativeMapIterator(const InnerMapInline& value) : snapshot(value) {
        const auto& copy = std::get<InnerMapInline>(snapshot);
        range = Inline{copy.cbegin(), copy.cend()};
    }
    explicit NativeMapIterator(const InnerMapTimeWindow& value) : snapshot(value) {
        const auto& copy = std::get<InnerMapTimeWindow>(snapshot);
        range = Windows{copy.cbegin(), copy.cend()};
    }
};
} // namespace forl0
