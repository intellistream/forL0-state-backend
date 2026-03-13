// Test runner entry point.

#ifdef FORL0_USE_MINI_GTEST
#include "mini_gtest.h"
int main() { return run_all_tests(); }
#else
#include <gtest/gtest.h>
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
#endif
