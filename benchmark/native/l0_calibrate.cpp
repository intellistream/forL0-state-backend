// Standalone L0/DRAM calibration used to parameterize the local performance model.
// It deliberately links only libdl/pthreads and discovers libl0mempool at runtime.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <dlfcn.h>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <random>
#include <string>
#include <thread>
#include <vector>

using Clock = std::chrono::steady_clock;

namespace {

using InitFn = int (*)(void**, size_t);
using DestroyFn = int (*)(void*);
using AllocFn = void* (*)(void*, size_t);
using FreeFn = int (*)(void*, void*);

struct Api {
    void* handle = nullptr;
    InitFn init = nullptr;
    DestroyFn destroy = nullptr;
    AllocFn alloc = nullptr;
    FreeFn free = nullptr;
};

struct Sample {
    size_t bytes;
    double random_read_ns;
    double sequential_read_gib_s;
    double sequential_write_gib_s;
};

struct ParallelSample {
    int workers;
    double sequential_read_gib_s;
};

struct HotSetSample {
    size_t requested_active_bytes;
    size_t actual_active_bytes;
    size_t sets;
    double init_ns_per_set;
    double lookup_ns_per_op;
    double update_ns_per_op;
};

double seconds_since(Clock::time_point start) {
    return std::chrono::duration<double>(Clock::now() - start).count();
}

double measure_hash_mix_mops_s() {
    constexpr uint64_t iterations = 20000000;
    uint64_t value = 0x9e3779b97f4a7c15ULL;
    auto start = Clock::now();
    for (uint64_t i = 0; i < iterations; ++i) {
        value ^= value >> 33;
        value *= 0xff51afd7ed558ccdULL;
        value ^= value >> 33;
        value *= 0xc4ceb9fe1a85ec53ULL;
        value ^= value >> 33;
        value += i;
    }
    const double elapsed = seconds_since(start);
    volatile uint64_t sink = value;
    (void)sink;
    return static_cast<double>(iterations) / elapsed / 1e6;
}

std::string escape_json(const std::string& value) {
    std::string out;
    for (unsigned char c : value) {
        if (c == '\\' || c == '"') { out.push_back('\\'); out.push_back(c); }
        else if (c == '\n') out += "\\n";
        else if (c >= 0x20) out.push_back(static_cast<char>(c));
    }
    return out;
}

void write_stage(const std::string& path, const char* stage) {
    std::ofstream marker(path, std::ios::trunc);
    marker << stage << "\n";
    marker.flush();
}

Sample measure(uint8_t* memory, size_t bytes) {
    constexpr size_t line = 64;
    const size_t lines = std::max<size_t>(2, bytes / line);
    std::vector<uint32_t> order(lines);
    std::iota(order.begin(), order.end(), 0);
    std::mt19937 generator(0xF0A0u + static_cast<unsigned>(lines));
    std::shuffle(order.begin(), order.end(), generator);
    for (size_t i = 0; i < lines; ++i) {
        auto* slot = reinterpret_cast<uint64_t*>(memory + static_cast<size_t>(order[i]) * line);
        *slot = order[(i + 1) % lines];
    }

    const size_t random_iterations = std::max<size_t>(200000, std::min<size_t>(2000000, lines * 4));
    uint64_t cursor = order[0];
    auto start = Clock::now();
    for (size_t i = 0; i < random_iterations; ++i) {
        cursor = *reinterpret_cast<volatile uint64_t*>(memory + cursor * line);
    }
    const double random_ns = seconds_since(start) * 1e9 / random_iterations;

    volatile uint64_t checksum = cursor;
    constexpr int passes = 2;
    start = Clock::now();
    for (int pass = 0; pass < passes; ++pass) {
        for (size_t offset = 0; offset < bytes; offset += line) {
            checksum += *reinterpret_cast<volatile uint64_t*>(memory + offset);
        }
    }
    const double read_seconds = seconds_since(start);

    start = Clock::now();
    for (int pass = 0; pass < passes; ++pass) {
        for (size_t offset = 0; offset < bytes; offset += line) {
            *reinterpret_cast<volatile uint64_t*>(memory + offset) = checksum + offset + pass;
        }
    }
    const double write_seconds = seconds_since(start);
    const double gib = static_cast<double>(bytes) * passes / (1024.0 * 1024.0 * 1024.0);
    return {bytes, random_ns, gib / read_seconds, gib / write_seconds};
}

std::vector<Sample> measure_curve(uint8_t* memory, size_t allocation_bytes) {
    const size_t candidates[] = {
        64 * 1024ULL, 1024 * 1024ULL, 8 * 1024 * 1024ULL,
        32 * 1024 * 1024ULL, 128 * 1024 * 1024ULL,
    };
    std::vector<Sample> result;
    for (size_t bytes : candidates) {
        if (bytes <= allocation_bytes) {
            std::vector<Sample> repeats;
            for (int repeat = 0; repeat < 3; ++repeat) repeats.push_back(measure(memory, bytes));
            auto median = [&](auto field) {
                std::vector<double> values;
                for (const auto& sample : repeats) values.push_back(field(sample));
                std::sort(values.begin(), values.end());
                return values[values.size() / 2];
            };
            result.push_back({
                bytes,
                median([](const Sample& s) { return s.random_read_ns; }),
                median([](const Sample& s) { return s.sequential_read_gib_s; }),
                median([](const Sample& s) { return s.sequential_write_gib_s; }),
            });
        }
    }
    if (result.empty() || result.back().bytes != allocation_bytes) {
        std::vector<Sample> repeats;
        for (int repeat = 0; repeat < 3; ++repeat) repeats.push_back(measure(memory, allocation_bytes));
        auto by_random = repeats;
        auto by_read = repeats;
        auto by_write = repeats;
        std::sort(by_random.begin(), by_random.end(), [](const Sample& a, const Sample& b) {
            return a.random_read_ns < b.random_read_ns;
        });
        std::sort(by_read.begin(), by_read.end(), [](const Sample& a, const Sample& b) {
            return a.sequential_read_gib_s < b.sequential_read_gib_s;
        });
        std::sort(by_write.begin(), by_write.end(), [](const Sample& a, const Sample& b) {
            return a.sequential_write_gib_s < b.sequential_write_gib_s;
        });
        result.push_back({allocation_bytes, by_random[1].random_read_ns,
                          by_read[1].sequential_read_gib_s,
                          by_write[1].sequential_write_gib_s});
    }
    return result;
}

std::vector<ParallelSample> measure_parallel_curve(uint8_t* memory, size_t bytes) {
    constexpr size_t line = 64;
    std::vector<ParallelSample> result;
    for (int workers : {1, 2, 4}) {
        std::vector<std::thread> threads;
        std::vector<uint64_t> checksums(workers, 0);
        const size_t chunk = (bytes / static_cast<size_t>(workers) / line) * line;
        auto start = Clock::now();
        for (int worker = 0; worker < workers; ++worker) {
            threads.emplace_back([=, &checksums]() {
                volatile uint64_t sum = 0;
                uint8_t* begin = memory + static_cast<size_t>(worker) * chunk;
                for (int pass = 0; pass < 2; ++pass) {
                    for (size_t offset = 0; offset < chunk; offset += line) {
                        sum += *reinterpret_cast<volatile uint64_t*>(begin + offset);
                    }
                }
                checksums[worker] = sum;
            });
        }
        for (auto& thread : threads) thread.join();
        const double seconds = seconds_since(start);
        const double gib = static_cast<double>(chunk) * workers * 2 /
                           (1024.0 * 1024.0 * 1024.0);
        // Keep the reads observable without including reduction in the timer.
        volatile uint64_t sink = 0;
        for (uint64_t value : checksums) sink += value;
        (void)sink;
        result.push_back({workers, gib / seconds});
    }
    return result;
}

uint64_t mix64(uint64_t value) {
    value ^= value >> 33;
    value *= 0xff51afd7ed558ccdULL;
    value ^= value >> 33;
    value *= 0xc4ceb9fe1a85ec53ULL;
    value ^= value >> 33;
    return value;
}

HotSetSample measure_hotset(uint8_t* memory, size_t requested_active_bytes) {
    constexpr size_t set_bytes = 192;
    const size_t available_sets = requested_active_bytes / set_bytes;
    size_t sets = 1;
    while (sets <= available_sets / 2) sets <<= 1;
    const size_t actual_active_bytes = sets * set_bytes;

    auto start = Clock::now();
    for (size_t index = 0; index < sets; ++index) {
        uint8_t* set = memory + index * set_bytes;
        std::memset(set, 0x80, 8);
        set[8] = 0;
    }
    const double init_ns = seconds_since(start) * 1e9 / static_cast<double>(sets);

    for (size_t index = 0; index < sets; ++index) {
        uint8_t* set = memory + index * set_bytes;
        set[0] = static_cast<uint8_t>(index & 0x7f);
        *reinterpret_cast<uint64_t*>(set + 64) = index;
        *reinterpret_cast<uint64_t*>(set + 128) = mix64(index);
    }

    const size_t operations = std::max<size_t>(500000, std::min<size_t>(2000000, sets * 64));
    volatile uint64_t checksum = 0;
    uint64_t state = 0x9e3779b97f4a7c15ULL;
    start = Clock::now();
    for (size_t operation = 0; operation < operations; ++operation) {
        state = mix64(state + operation);
        const size_t index = state & (sets - 1);
        uint8_t* set = memory + index * set_bytes;
        checksum += set[0];
        checksum += *reinterpret_cast<volatile uint64_t*>(set + 64);
        checksum += *reinterpret_cast<volatile uint64_t*>(set + 128);
    }
    const double lookup_ns = seconds_since(start) * 1e9 / static_cast<double>(operations);

    state = checksum ^ 0xd1b54a32d192ed03ULL;
    start = Clock::now();
    for (size_t operation = 0; operation < operations; ++operation) {
        state = mix64(state + operation);
        const size_t index = state & (sets - 1);
        uint8_t* set = memory + index * set_bytes;
        set[0] = static_cast<uint8_t>(state & 0x7f);
        *reinterpret_cast<volatile uint64_t*>(set + 64) = state;
        *reinterpret_cast<volatile uint64_t*>(set + 128) = checksum + state;
    }
    const double update_ns = seconds_since(start) * 1e9 / static_cast<double>(operations);
    return {requested_active_bytes, actual_active_bytes, sets, init_ns,
            lookup_ns, update_ns};
}

std::vector<HotSetSample> measure_hotset_curve(uint8_t* memory, size_t allocation_bytes) {
    const size_t candidates[] = {
        1024 * 1024ULL, 2 * 1024 * 1024ULL, 4 * 1024 * 1024ULL,
        6 * 1024 * 1024ULL, 8 * 1024 * 1024ULL,
    };
    std::vector<HotSetSample> result;
    for (size_t bytes : candidates) {
        if (bytes <= allocation_bytes) result.push_back(measure_hotset(memory, bytes));
    }
    if (result.empty() || result.back().requested_active_bytes != allocation_bytes) {
        result.push_back(measure_hotset(memory, allocation_bytes));
    }
    return result;
}

void write_curve(std::ostream& out, const std::vector<Sample>& curve) {
    out << "[\n";
    for (size_t i = 0; i < curve.size(); ++i) {
        const auto& s = curve[i];
        out << "      {\"working_set_bytes\": " << s.bytes
            << ", \"random_read_ns\": " << s.random_read_ns
            << ", \"sequential_read_gib_s\": " << s.sequential_read_gib_s
            << ", \"sequential_write_gib_s\": " << s.sequential_write_gib_s << "}";
        out << (i + 1 == curve.size() ? "\n" : ",\n");
    }
    out << "    ]";
}

void write_parallel_curve(std::ostream& out, const std::vector<ParallelSample>& curve) {
    out << "[";
    for (size_t i = 0; i < curve.size(); ++i) {
        out << "{\"workers\": " << curve[i].workers
            << ", \"sequential_read_gib_s\": " << curve[i].sequential_read_gib_s << "}";
        if (i + 1 != curve.size()) out << ", ";
    }
    out << "]";
}

void write_hotset_curve(std::ostream& out, const std::vector<HotSetSample>& curve) {
    out << "[\n";
    for (size_t i = 0; i < curve.size(); ++i) {
        const auto& sample = curve[i];
        out << "      {\"requested_active_bytes\": " << sample.requested_active_bytes
            << ", \"actual_active_bytes\": " << sample.actual_active_bytes
            << ", \"sets\": " << sample.sets
            << ", \"init_ns_per_set\": " << sample.init_ns_per_set
            << ", \"lookup_ns_per_op\": " << sample.lookup_ns_per_op
            << ", \"update_ns_per_op\": " << sample.update_ns_per_op << "}";
        out << (i + 1 == curve.size() ? "\n" : ",\n");
    }
    out << "    ]";
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 4 || argc > 6) {
        std::cerr << "usage: l0_calibrate LIBL0 OUTPUT_JSON ALLOCATION_BYTES "
                     "[EVIDENCE_LABEL] [TUNER_CAPACITY_BYTES]\n";
        return 2;
    }
    const std::string library = argv[1];
    const std::string output = argv[2];
    const std::string stage_output = output + ".stage";
    const size_t requested = std::strtoull(argv[3], nullptr, 10);
    const std::string evidence_label = argc >= 5 ? argv[4] : "real-hardware-calibration";
    const size_t tuner_capacity = argc == 6 ? std::strtoull(argv[5], nullptr, 10) : requested;
    if (requested == 0 || tuner_capacity < requested) {
        std::cerr << "allocation must be non-zero and no larger than tuner capacity\n";
        return 2;
    }
    // Reserve alignment slack when the process-scoped tuner has room. The
    // vendor ABI does not promise a cache-line-aligned return address, while
    // HotSet uses 64-byte tag/key/value lines. Without this slack a 48-byte
    // pointer adjustment turned a nominal 6 MiB probe into only 3 MiB of
    // power-of-two sets.
    constexpr size_t alignment_slack = 63;
    const size_t vendor_allocation = (
        requested <= SIZE_MAX - alignment_slack
        && tuner_capacity >= requested + alignment_slack)
        ? requested + alignment_slack : requested;
    std::ofstream out(output);
    out << std::fixed << std::setprecision(3);
    out << "{\n  \"schema_version\": 1,\n"
        << "  \"evidence_label\": \"" << escape_json(evidence_label) << "\",\n"
        << "  \"requested_bytes\": " << requested << ",\n"
        << "  \"tuner_capacity_bytes\": " << tuner_capacity << ",\n"
        << "  \"curve_repeats\": 3,\n"
        << "  \"cpu_hash_mix_mops_s\": " << measure_hash_mix_mops_s() << ",\n";
    // Preserve useful diagnostic context even if a vendor allocation later
    // terminates this isolated probe process.
    out.flush();

    write_stage(stage_output, "heap_allocation");
    void* heap = nullptr;
    if (posix_memalign(&heap, 64, requested) != 0 || !heap) return 3;
    std::memset(heap, 0, requested);
    write_stage(stage_output, "heap_measurement");
    const auto heap_curve = measure_curve(static_cast<uint8_t*>(heap), requested);
    const auto heap_parallel = measure_parallel_curve(static_cast<uint8_t*>(heap), requested);

    write_stage(stage_output, "vendor_library_load");
    Api api;
    if (library != "-") api.handle = dlopen(library.c_str(), RTLD_NOW);
    if (api.handle) {
        api.init = reinterpret_cast<InitFn>(dlsym(api.handle, "cache_tuner_init"));
        api.destroy = reinterpret_cast<DestroyFn>(dlsym(api.handle, "cache_tuner_destroy"));
        api.alloc = reinterpret_cast<AllocFn>(dlsym(api.handle, "l0_mem_alloc"));
        api.free = reinterpret_cast<FreeFn>(dlsym(api.handle, "l0_mem_free"));
    }
    if (!api.handle || !api.init || !api.destroy || !api.alloc || !api.free) {
        const char* error = dlerror();
        out << "  \"status\": \"heap-only\",\n"
            << "  \"heap\": ";
        write_curve(out, heap_curve);
        out << ",\n  \"heap_parallel_read_scaling\": ";
        write_parallel_curve(out, heap_parallel);
        out << ",\n  \"l0\": null,\n"
            << "  \"l0_reason\": \""
            << escape_json(library == "-" ? "heap-only mode" : (error ? error : "missing L0 symbols"))
            << "\"\n}\n";
        if (api.handle) dlclose(api.handle);
        std::free(heap);
        return 0;
    }

    void* tuner = nullptr;
    write_stage(stage_output, "cache_tuner_init");
    const int init_rc = api.init(&tuner, tuner_capacity);
    write_stage(stage_output, "cache_tuner_init_returned");
    if (init_rc != 0 || !tuner) {
        out << "  \"status\": \"failed\",\n"
            << "  \"heap\": ";
        write_curve(out, heap_curve);
        out << ",\n  \"heap_parallel_read_scaling\": ";
        write_parallel_curve(out, heap_parallel);
        out << ",\n  \"l0\": null,\n"
            << "  \"l0_reason\": \"cache_tuner_init failed\",\n"
            << "  \"failure_stage\": \"cache_tuner_init\",\n"
            << "  \"cache_tuner_init_rc\": " << init_rc << ",\n"
            << "  \"tuner_created\": " << (tuner ? "true" : "false") << "\n}\n";
        if (tuner) api.destroy(tuner);
        dlclose(api.handle);
        std::free(heap);
        return 0;
    }
    write_stage(stage_output, "l0_mem_alloc");
    void* l0 = (init_rc == 0 && tuner) ? api.alloc(tuner, vendor_allocation) : nullptr;
    write_stage(stage_output, "l0_mem_alloc_returned");
    if (!l0) {
        out << "  \"status\": \"failed\",\n"
            << "  \"heap\": ";
        write_curve(out, heap_curve);
        out << ",\n  \"heap_parallel_read_scaling\": ";
        write_parallel_curve(out, heap_parallel);
        out << ",\n  \"l0\": null,\n"
            << "  \"l0_reason\": \""
            << "L0 allocation failed\",\n"
            << "  \"failure_stage\": \"l0_mem_alloc\",\n"
            << "  \"cache_tuner_init_rc\": " << init_rc << ",\n"
            << "  \"tuner_created\": " << (tuner ? "true" : "false") << "\n}\n";
        if (tuner) api.destroy(tuner);
        dlclose(api.handle);
        std::free(heap);
        return 0;
    }
    const uintptr_t raw_address = reinterpret_cast<uintptr_t>(l0);
    const uintptr_t aligned_address = (raw_address + 63u) & ~uintptr_t(63);
    const size_t alignment_padding = aligned_address - raw_address;
    const size_t usable_bytes = vendor_allocation - alignment_padding;
    uint8_t* const l0_aligned = reinterpret_cast<uint8_t*>(aligned_address);

    // Production HotCache never zero-fills the whole tuner allocation. It
    // initializes tag metadata sparsely and touches selected tag/key/value
    // cache lines. Keep dense bandwidth calibration bounded to the already
    // proven 1 MiB region, then measure production-shaped pressure separately.
    const size_t measurement_bytes = std::min(requested, usable_bytes);
    const size_t dense_bytes = std::min<size_t>(measurement_bytes, 1024 * 1024ULL);
    write_stage(stage_output, "l0_dense_measurement");
    const auto l0_curve = measure_curve(l0_aligned, dense_bytes);
    const auto l0_parallel = measure_parallel_curve(l0_aligned, dense_bytes);
    write_stage(stage_output, "l0_hotset_measurement");
    const auto l0_hotset = measure_hotset_curve(l0_aligned, measurement_bytes);

    out << "  \"status\": \"complete\",\n"
        << "  \"allocation_bytes\": " << requested << ",\n"
        << "  \"vendor_allocation_bytes\": " << vendor_allocation << ",\n"
        << "  \"usable_allocation_bytes\": " << usable_bytes << ",\n"
        << "  \"measurement_bytes\": " << measurement_bytes << ",\n"
        << "  \"alignment_padding_bytes\": " << alignment_padding << ",\n"
        << "  \"dense_measurement_bytes\": " << dense_bytes << ",\n"
        << "  \"access_pattern\": \"dense-up-to-1mb-plus-hotset-sparse\",\n"
        << "  \"heap\": ";
    write_curve(out, heap_curve);
    out << ",\n  \"heap_parallel_read_scaling\": ";
    write_parallel_curve(out, heap_parallel);
    out << ",\n  \"l0\": ";
    write_curve(out, l0_curve);
    out << ",\n  \"l0_parallel_read_scaling\": ";
    write_parallel_curve(out, l0_parallel);
    out << ",\n  \"l0_hotset_pressure_curve\": ";
    write_hotset_curve(out, l0_hotset);
    out << "\n}\n";

    write_stage(stage_output, "l0_release");
    std::free(heap);
    api.free(tuner, l0);
    api.destroy(tuner);
    dlclose(api.handle);
    write_stage(stage_output, "complete");
    return 0;
}
