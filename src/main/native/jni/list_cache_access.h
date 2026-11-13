#pragma once
#include "state_cache_access.h"
#include "flink_binary_format.h"

namespace forl0 {

// Each element has its own cache slot. Append never serializes or recopies the
// existing list. Java receives an owned bulk result, so read misses may admit.
template<class K, class... Args>
jbyteArray cached_list_get(JNIEnv* env, StateHandle* h,
                          StateTable<K, ElementList>* table, Args&&... args) {
    auto* elements = table->get(args...);
    if (!elements || elements->empty()) return nullptr;
    MapCallCache cache(h, args...);
    WriteBuffer buffer;
    buffer.write_int(static_cast<int32_t>(elements->size()));
    for (size_t i = 0; i < elements->size(); ++i) {
        std::string_view bytes;
        if (!cache.get(uint64_t(i), &bytes)) {
            bytes = (*elements)[i];
            cache.put(uint64_t(i), bytes);
        }
        buffer.write_raw(reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size());
    }
    return buffer_to_jbytearray(env, buffer.data(), buffer.size());
}

template<class K, class... Args>
void cached_list_append(StateHandle* h, StateTable<K, ElementList>* table,
                        ElementList&& added, Args&&... args) {
    if (added.empty()) return;
    MapCallCache cache(h, args...);
    auto append = [&](ElementList& elements) {
        size_t start = elements.size();
        elements.insert(elements.end(), std::make_move_iterator(added.begin()),
                        std::make_move_iterator(added.end()));
        for (size_t i = start; i < elements.size(); ++i) cache.put(uint64_t(i), elements[i]);
    };
    try {
        if (!table->modify_in_place(args..., append)) {
            auto* stored = table->put(args..., std::move(added));
            for (size_t i = 0; i < stored->size(); ++i) cache.put(uint64_t(i), (*stored)[i]);
        }
    } catch (...) { cache.invalidate(); throw; }
}

template<class K, class... Args>
void cached_list_add(StateHandle* h, StateTable<K, ElementList>* table,
                     std::string&& element, Args&&... args) {
    MapCallCache cache(h, args...);
    try {
        if (!table->modify_in_place(args..., [&](ElementList& elements) {
            elements.push_back(std::move(element));
            cache.put(uint64_t(elements.size() - 1), elements.back());
        })) {
            ElementList elements;
            elements.push_back(std::move(element));
            auto* stored = table->put(args..., std::move(elements));
            cache.put(uint64_t(0), stored->front());
        }
    } catch (...) { cache.invalidate(); throw; }
}

template<class K, class... Args>
void cached_list_update(StateHandle* h, StateTable<K, ElementList>* table,
                        ElementList&& elements, Args&&... args) {
    MapCallCache cache(h, args...);
    cache.invalidate();
    auto* stored = table->put(args..., std::move(elements));
    try {
        for (size_t i = 0; i < stored->size(); ++i) cache.put(uint64_t(i), (*stored)[i]);
    } catch (...) { cache.invalidate(); throw; }
}

template<class K, class... Args>
void cached_list_clear(StateHandle* h, StateTable<K, ElementList>* table, Args&&... args) {
    MapCallCache cache(h, args...);
    cache.invalidate();
    table->remove(args...);
}
} // namespace forl0
