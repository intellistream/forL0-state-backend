#pragma once
#include "state_engine.h"

namespace forl0 {
// Session windows commonly remove a window and immediately insert its merged
// replacement. Retain one detached node and one empty bucket array per state,
// while removing the empty parent from the canonical table as before.
inline void put_window_entry(StateTable<int64_t, InnerMapTimeWindow>* table,
                             TimeWindowMapScratch& scratch, int group, int64_t key,
                             const TimeWindow& user, const TimeWindow& value) {
    auto update = [&](InnerMapTimeWindow& map) {
        auto found = map.find(user);
        if (found != map.end()) { found->second = value; return; }
        if (!scratch.node.empty()) {
            scratch.node.key() = user;
            scratch.node.mapped() = value;
            map.insert(std::move(scratch.node));
        } else map.emplace(user, value);
    };
    if (!table->modify_in_place(group, key, update)) {
        InnerMapTimeWindow map;
        map.swap(scratch.empty_map);
        update(map);
        table->put(group, key, std::move(map));
    }
}

inline void remove_window_entry(StateTable<int64_t, InnerMapTimeWindow>* table,
                                TimeWindowMapScratch& scratch, int group, int64_t key,
                                const TimeWindow& user) {
    table->modify_or_remove_in_place(group, key, [&](InnerMapTimeWindow& map) {
        auto removed = map.extract(user);
        if (!removed.empty()) scratch.node = std::move(removed);
        // Bound retained bucket storage even if a caller had a very large map.
        if (map.empty() && map.bucket_count() <= 64) map.swap(scratch.empty_map);
    });
}
} // namespace forl0
