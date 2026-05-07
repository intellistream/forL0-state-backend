cmake_minimum_required(VERSION 3.14)
project(forl0_engine LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# --------------------------------------------------------------------------
# Find JNI
# --------------------------------------------------------------------------
find_package(JNI REQUIRED)
include_directories(${JNI_INCLUDE_DIRS})

# --------------------------------------------------------------------------
# Platform / SIMD detection
# --------------------------------------------------------------------------
include(CheckCXXCompilerFlag)

if(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64|amd64|AMD64")
    set(FORL0_ARCH "x86_64")
    # SSE2 is baseline for x86-64
    add_compile_definitions(FORL0_SSE2=1)
    check_cxx_compiler_flag("-mavx2" HAS_AVX2)
    if(HAS_AVX2)
        add_compile_definitions(FORL0_HAS_AVX2=1)
    endif()
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|arm64|ARM64")
    set(FORL0_ARCH "aarch64")
    add_compile_definitions(FORL0_NEON=1)
else()
    set(FORL0_ARCH "portable")
    add_compile_definitions(FORL0_PORTABLE=1)
endif()

message(STATUS "ForL0 engine: arch=${FORL0_ARCH}, system=${CMAKE_SYSTEM_NAME}")

# --------------------------------------------------------------------------
# Compiler flags
# --------------------------------------------------------------------------
# Sanitizers are opt-in: enabling them makes the dylib unloadable by the JVM on
# macOS (Sanitizer load violates platform policy). Use
#   cmake -DFORL0_ENABLE_SANITIZERS=ON ..
# when you specifically want ASan/UBSan for running the C++ test binary.
option(FORL0_ENABLE_SANITIZERS "Enable AddressSanitizer + UndefinedBehaviorSanitizer" OFF)

if(CMAKE_BUILD_TYPE STREQUAL "Debug" OR CMAKE_BUILD_TYPE STREQUAL "")
    add_compile_options(-O0 -g)
else()
    add_compile_options(-O3 -DNDEBUG)
    if(NOT APPLE)
        add_compile_options(-march=native)
    endif()
endif()

if(FORL0_ENABLE_SANITIZERS)
    message(STATUS "ForL0 engine: sanitizers ENABLED (asan,undefined)")
    add_compile_options(-fsanitize=address,undefined)
    add_link_options(-fsanitize=address,undefined)
endif()

add_compile_options(-Wall -Wextra -Wno-unused-parameter)

# --------------------------------------------------------------------------
# Source files
# --------------------------------------------------------------------------
set(FORL0_SOURCES
    engine/hot_cache.cpp
    jni/forl0_jni.cpp
    jni/jni_value_state.cpp
    jni/jni_list_state.cpp
    jni/jni_map_state.cpp
    jni/jni_tw_state.cpp
    jni/jni_checkpoint.cpp
)

# --------------------------------------------------------------------------
# Shared library target
# --------------------------------------------------------------------------
add_library(forl0_engine SHARED ${FORL0_SOURCES})

# Link libdl for dlopen/dlsym (required on glibc < 2.34, e.g. Kunpeng/Kylin/CentOS)
target_link_libraries(forl0_engine PRIVATE ${CMAKE_DL_LIBS})

target_include_directories(forl0_engine PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/engine
    ${CMAKE_CURRENT_SOURCE_DIR}/platform
    ${CMAKE_CURRENT_SOURCE_DIR}/checkpoint
    ${CMAKE_CURRENT_SOURCE_DIR}/jni
)

# --------------------------------------------------------------------------
# Output naming
# --------------------------------------------------------------------------
set_target_properties(forl0_engine PROPERTIES
    PREFIX "lib"
    OUTPUT_NAME "forl0_engine"
)

# --------------------------------------------------------------------------
# Install to resources/native
# --------------------------------------------------------------------------
set(NATIVE_RESOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../resources/native")
add_custom_command(TARGET forl0_engine POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E make_directory "${NATIVE_RESOURCE_DIR}"
    COMMAND ${CMAKE_COMMAND} -E copy "$<TARGET_FILE:forl0_engine>" "${NATIVE_RESOURCE_DIR}/"
    COMMENT "Copying libforl0_engine to resources/native/"
)

# --------------------------------------------------------------------------
# Google Test (optional, for C++ unit tests)
# --------------------------------------------------------------------------
option(FORL0_BUILD_TESTS "Build C++ unit tests" ON)

if(FORL0_BUILD_TESTS)
    set(FORL0_TEST_SOURCES
        engine/hot_cache.cpp
        test/swiss_table_test.cpp
        test/state_table_cow_test.cpp
        test/checkpoint_round_trip_test.cpp
        test/type_layout_parser_test.cpp
        test/time_window_namespace_test.cpp
        test/typed_inner_map_test.cpp
        test/hot_cache_test.cpp
        test/hot_cache_manager_test.cpp
        test/hot_cache_phase_b_test.cpp
        test/test_main.cpp
    )

    add_executable(forl0_tests ${FORL0_TEST_SOURCES})

    target_include_directories(forl0_tests PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/engine
        ${CMAKE_CURRENT_SOURCE_DIR}/platform
        ${CMAKE_CURRENT_SOURCE_DIR}/checkpoint
        ${CMAKE_CURRENT_SOURCE_DIR}/jni
        ${CMAKE_CURRENT_SOURCE_DIR}/test
    )

    # Try system GTest first, fall back to mini_gtest.
    # On macOS Homebrew's gtest can lack BUILD_RPATH info, breaking
    # gtest_discover_tests — force the fallback there unless users override.
    option(FORL0_FORCE_MINI_GTEST "Force mini_gtest fallback even if system GTest exists" OFF)
    if(APPLE)
        set(FORL0_FORCE_MINI_GTEST ON)
    endif()
    if(FORL0_FORCE_MINI_GTEST)
        set(GTest_FOUND FALSE)
    else()
        find_package(GTest QUIET)
    endif()
    if(GTest_FOUND)
        message(STATUS "Using system Google Test")
        target_link_libraries(forl0_tests PRIVATE GTest::gtest)
        enable_testing()
        include(GoogleTest)
        gtest_discover_tests(forl0_tests)
    else()
        message(STATUS "Google Test not found — using mini_gtest fallback")
        target_compile_definitions(forl0_tests PRIVATE FORL0_USE_MINI_GTEST=1)
        enable_testing()
        add_test(NAME forl0_tests COMMAND forl0_tests)
    endif()
endif()
