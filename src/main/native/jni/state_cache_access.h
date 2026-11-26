#pragma once
#include "jni_utils.h"
#include <array>
#include <algorithm>
#include <optional>
#include <type_traits>

namespace forl0 {

template<class N, class K = WindowStateKey>
std::optional<std::string_view> get_namespace_blob(StateHandle* handle, int group, const N& ns,
                                                    const K& key) {
    return handle->visit_window_rows<K>([&](auto* table) -> std::optional<std::string_view> {
        const auto* value = table->get(group, ns, key);
        if (!value) return std::nullopt;
        return std::string_view(value->data(), value->size());
    });
}

template<class N, class Copy, class K = WindowStateKey>
void put_namespace_blob(StateHandle* handle, int group, const N& ns, const K& key,
                        size_t length, Copy&& copy) {
    handle->visit_window_rows<K>([&](auto* table) {
        table->update_or_create(group, ns, key, [&](auto& value) {
            if constexpr (is_fixed_window_row<std::decay_t<decltype(value)>>) value.overwrite(length, copy);
            else {
                value.resize(length);
                copy(value.data());
            }
        });
    });
}

template<class N>
void put_namespace_blob_address(StateHandle* handle, int group, const N& ns, const WindowStateKey& key,
                                const char* source, size_t length) {
    const auto* arena = handle->table_handle->namespace_arena();
    std::array<char, 128> local;
    std::string owned;
    if (arena && arena->contains(source)) {
        // A write that grows a resident table may first move it to DRAM.
        if (length <= local.size()) {
            std::memcpy(local.data(), source, length);
            source = local.data();
        } else {
            owned.assign(source, length);
            source = owned.data();
        }
    }
    handle->visit_window_rows<WindowStateKey>([&](auto* table) {
        table->update_or_create(group, ns, key, [&](auto& value) { value.assign(source, length); });
    });
}

template<class K, class V, class... Args>
auto get_value(StateTable<K, V>* table, Args&&... args) {
    using Result = std::conditional_t<std::is_same_v<V, NamespaceBlob>, std::string_view, V>;
    const auto* value = table->get(std::forward<Args>(args)...);
    return value ? std::optional<Result>(*value) : std::nullopt;
}

}  // namespace forl0
