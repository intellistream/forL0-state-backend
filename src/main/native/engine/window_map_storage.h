#pragma once
#include "state_engine.h"

namespace forl0 {

inline void put_window_entry(StateTable<int64_t, InnerMapTimeWindow>* table,
                             int group, int64_t key, const TimeWindow& user, const TimeWindow& value) {
    table->update_or_create(group, key, [&](InnerMapTimeWindow& map) { map[user] = value; });
}

inline void remove_window_entry(StateTable<int64_t, InnerMapTimeWindow>* table,
                                int group, int64_t key, const TimeWindow& user) {
    table->modify_or_remove_in_place(group, key, [&](InnerMapTimeWindow& map) { map.erase(user); });
}

}  // namespace forl0
