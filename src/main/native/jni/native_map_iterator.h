#pragma once
#include "type_layout.h"
#include <utility>
#include <variant>

namespace forl0 {
// All JNI iterator producers and consumers must share the same layout.
struct NativeMapIterator {
    using Strings = std::pair<InnerMap::const_iterator, InnerMap::const_iterator>;
    using Windows = std::pair<InnerMapTimeWindow::const_iterator, InnerMapTimeWindow::const_iterator>;
    std::variant<Strings, Windows> range;

    NativeMapIterator(InnerMap::const_iterator begin, InnerMap::const_iterator end)
        : range(Strings{begin, end}) {}
    NativeMapIterator(InnerMapTimeWindow::const_iterator begin, InnerMapTimeWindow::const_iterator end)
        : range(Windows{begin, end}) {}
};
} // namespace forl0
