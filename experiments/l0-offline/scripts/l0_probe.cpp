#include "engine/hot_cache.h"
#include <chrono>
#include <fstream>
#include <iostream>
#include <thread>
#include <unistd.h>

// Both probes remain alive until the controller releases the barrier. This
// checks two simultaneous TM-sized SDK allocations, not two sequential ones.
int main(int argc,char** argv) {
    if(argc!=3)return 2;
    const std::string barrier=argv[1],role=argv[2];
    const char* requested=std::getenv("FORL0_L0_PROBE_BYTES");
    const size_t quota=requested?std::strtoull(requested,nullptr,10):35ULL*1024*1024;
    forl0::HotCacheManager manager(quota,nullptr,true);
    if(!manager.is_active()) {
        std::cerr << "L0 allocation failed: " << manager.failure_reason() << '\n';
        return 3;
    }
    auto rows=manager.acquire_rows(quota);
    if(!rows)return 4;
    const size_t bytes=size_t(rows->num_sets())*sizeof(forl0::HotRowSet);
    volatile unsigned char* p=reinterpret_cast<volatile unsigned char*>(rows->base_address());
    uint64_t checksum=0;
    // Touch every physical cache line; allocation alone does not validate mmap.
    for(size_t i=0;i<bytes;i+=64)p[i]=static_cast<unsigned char>((i/64)%251);
    for(size_t i=0;i<bytes;i+=64)checksum+=p[i];
    std::ofstream(barrier+"/ready-"+role) << "ready\n";
    auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(90);
    while(!std::ifstream(barrier+"/release").good()) {
        if(std::chrono::steady_clock::now()>deadline)return 5;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    // Restore empty-set headers before the normal manager destructor.
    for(uint32_t i=0;i<rows->num_sets();++i) {
        auto* set=reinterpret_cast<forl0::HotRowSet*>(rows->base_address())+i;
        set->valid=0;
    }
    std::cout << "{\"active\":true,\"memory_mode\":\"" << manager.memory_mode()
              << "\",\"requested_bytes\":" << quota << ",\"capacity_bytes\":" << manager.capacity_bytes()
              << ",\"touched_bytes\":" << bytes << ",\"checksum\":" << checksum << "}\n";
    manager.release_rows(rows.release());
    return 0;
}
