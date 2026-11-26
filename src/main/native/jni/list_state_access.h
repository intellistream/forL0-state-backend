#pragma once
#include "jni_utils.h"

namespace forl0 {

template<class K, class... Args>
jbyteArray list_get(JNIEnv* env, StateTable<K, ElementList>* table, const Args&... args) {
    const auto* elements = table->get(args...);
    if (!elements || elements->empty()) return nullptr;
    const auto bytes = elements->serialized();
    return buffer_to_jbytearray(env, reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size());
}

template<class K, class... Args>
void list_append(StateTable<K, ElementList>* table, ElementList&& added, const Args&... args) {
    if (added.empty()) return;
    table->update_or_create(args..., [&](ElementList& elements) { elements.append_list(added); });
}

template<class K, class... Args>
void list_add(StateTable<K, ElementList>* table, NamespaceBlob&& element, const Args&... args) {
    table->update_or_create(args..., [&](ElementList& elements) { elements.append(element); });
}

template<class K, class... Args>
void list_update(StateTable<K, ElementList>* table, ElementList&& elements, const Args&... args) {
    table->put(args..., std::move(elements));
}

template<class K, class... Args>
void list_clear(StateTable<K, ElementList>* table, const Args&... args) {
    table->remove(args...);
}

}  // namespace forl0
