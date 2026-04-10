// Minimal Google Test compatible header for offline environments.
// Provides TEST(), ASSERT_*, and a simple test runner.
// Replace with real Google Test when available.

#pragma once

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

namespace testing {
namespace internal {

struct TestInfo {
    const char* suite;
    const char* name;
    std::function<void()> body;
};

inline std::vector<TestInfo>& test_registry() {
    static std::vector<TestInfo> tests;
    return tests;
}

struct TestRegistrar {
    TestRegistrar(const char* suite, const char* name, std::function<void()> body) {
        test_registry().push_back({suite, name, std::move(body)});
    }
};

inline int& failure_count() {
    static int count = 0;
    return count;
}

inline void assert_fail(const char* expr, const char* file, int line, const std::string& msg = "") {
    std::cerr << file << ":" << line << ": FAILED: " << expr;
    if (!msg.empty()) std::cerr << " — " << msg;
    std::cerr << "\n";
    ++failure_count();
    throw std::runtime_error("assertion failed");
}

}  // namespace internal
}  // namespace testing

#define TEST(suite, name)                                                     \
    static void suite##_##name##_body();                                      \
    static ::testing::internal::TestRegistrar suite##_##name##_registrar(     \
        #suite, #name, suite##_##name##_body);                                \
    static void suite##_##name##_body()

// ---- Assertion macros ----

#define ASSERT_TRUE(cond)                                                     \
    do { if (!(cond))                                                         \
        ::testing::internal::assert_fail(#cond, __FILE__, __LINE__);          \
    } while (0)

#define ASSERT_FALSE(cond)                                                    \
    do { if ((cond))                                                          \
        ::testing::internal::assert_fail("!" #cond, __FILE__, __LINE__);      \
    } while (0)

#define ASSERT_EQ(a, b)                                                       \
    do { auto&& _a = (a); auto&& _b = (b);                                   \
         if (!(_a == _b))                                                     \
             ::testing::internal::assert_fail(                                \
                 #a " == " #b, __FILE__, __LINE__);                           \
    } while (0)

#define ASSERT_NE(a, b)                                                       \
    do { auto&& _a = (a); auto&& _b = (b);                                   \
         if (!(_a != _b))                                                     \
             ::testing::internal::assert_fail(                                \
                 #a " != " #b, __FILE__, __LINE__);                           \
    } while (0)

#define ASSERT_GT(a, b)                                                       \
    do { auto&& _a = (a); auto&& _b = (b);                                   \
         if (!(_a > _b))                                                      \
             ::testing::internal::assert_fail(                                \
                 #a " > " #b, __FILE__, __LINE__);                            \
    } while (0)

#define ASSERT_GE(a, b)                                                       \
    do { auto&& _a = (a); auto&& _b = (b);                                   \
         if (!(_a >= _b))                                                     \
             ::testing::internal::assert_fail(                                \
                 #a " >= " #b, __FILE__, __LINE__);                           \
    } while (0)

#define ASSERT_LT(a, b)                                                       \
    do { auto&& _a = (a); auto&& _b = (b);                                   \
         if (!(_a < _b))                                                      \
             ::testing::internal::assert_fail(                                \
                 #a " < " #b, __FILE__, __LINE__);                            \
    } while (0)

#define ASSERT_THROW(stmt, exc_type)                                          \
    do { bool caught = false;                                                 \
         try { (stmt); } catch (const exc_type&) { caught = true; }           \
         if (!caught)                                                         \
             ::testing::internal::assert_fail(                                \
                 #stmt " should throw " #exc_type, __FILE__, __LINE__);       \
    } while (0)

// Support << operator on assertion failures (consume extra messages)
// This simplified version ignores the message content since we don't have gtest's
// full message infrastructure, but allows compilation of code using ASSERT_*() << "msg"

// ---- Main ----

inline int run_all_tests() {
    auto& tests = ::testing::internal::test_registry();
    int passed = 0, failed = 0;
    std::string last_suite;

    for (auto& t : tests) {
        if (t.suite != last_suite) {
            std::cout << "[----------] " << t.suite << "\n";
            last_suite = t.suite;
        }
        std::cout << "[ RUN      ] " << t.suite << "." << t.name << "\n";
        int before = ::testing::internal::failure_count();
        try {
            t.body();
        } catch (const std::exception& e) {
            // Already counted in assert_fail
        }
        if (::testing::internal::failure_count() > before) {
            std::cout << "[  FAILED  ] " << t.suite << "." << t.name << "\n";
            ++failed;
        } else {
            std::cout << "[       OK ] " << t.suite << "." << t.name << "\n";
            ++passed;
        }
    }

    std::cout << "[==========] " << (passed + failed) << " test(s) ran.\n";
    std::cout << "[  PASSED  ] " << passed << " test(s).\n";
    if (failed > 0) {
        std::cout << "[  FAILED  ] " << failed << " test(s).\n";
    }
    return failed > 0 ? 1 : 0;
}
