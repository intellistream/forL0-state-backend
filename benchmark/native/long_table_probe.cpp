// Auxiliary hash/probe microbenchmark. Not an end-to-end Flink measurement.
#include "swiss_table.h"
#include <chrono>
#include <iostream>
#include <memory>
#include <vector>

struct MixedLongHash {
    size_t operator()(int64_t key) const {
        uint64_t x = static_cast<uint64_t>(key);
        x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
        x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
        return x ^ (x >> 31);
    }
};

template<class Hash> void measure(const char* name, size_t n) {
    using Table = forl0::SwissTable<int64_t,int64_t,Hash>;
    std::vector<std::unique_ptr<Table>> tables;
    for (int i=0;i<128;i++) tables.emplace_back(new Table(64));
    auto start=std::chrono::steady_clock::now();
    for (size_t k=0;k<n;k++) tables[MixedLongHash{}(k)&127]->insert_or_assign(k,1);
    auto filled=std::chrono::steady_clock::now();
    uint64_t seed=42, sum=0;
    for (size_t i=0;i<n*4;i++) {
        seed+=0x9e3779b97f4a7c15ULL;
        int64_t k=MixedLongHash{}(seed)%n;
        auto* v=tables[MixedLongHash{}(k)&127]->find(k);
        if(!v) std::abort();
        sum+=++*v;
    }
    auto end=std::chrono::steady_clock::now();
    std::cout << name << " keys=" << n << " fill_s=" << std::chrono::duration<double>(filled-start).count()
              << " update_s=" << std::chrono::duration<double>(end-filled).count() << " checksum=" << sum << std::endl;
}
int main() { for(int i=0;i<3;i++) { measure<std::hash<int64_t>>("identity", 4000000); measure<MixedLongHash>("mixed",4000000); } }
