cmake_minimum_required(VERSION 3.16)
project(tidesdb_cpp VERSION 2.3.4 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(TIDESDB_CPP_BUILD_TESTS "Build tests" ON)

# Platform-specific compiler flags
if(MSVC)
    add_compile_options(/W4)
elseif(MINGW)
    add_compile_options(-Wall -Wextra)
else()
    add_compile_options(-Wall -Wextra -Wpedantic -fpermissive)
endif()

find_library(TIDESDB_LIBRARY NAMES tidesdb REQUIRED)

if(WIN32)
    find_package(zstd CONFIG REQUIRED)
    find_package(Snappy CONFIG REQUIRED)
    find_package(lz4 CONFIG REQUIRED)
    find_package(PThreads4W REQUIRED)
endif()

# Create the library
add_library(tidesdb_cpp STATIC src/tidesdb.cpp)
add_library(tidesdb::cpp ALIAS tidesdb_cpp)

target_include_directories(tidesdb_cpp
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
)

# Platform-specific include/link directories and libraries
if(APPLE)
    if(CMAKE_SYSTEM_PROCESSOR STREQUAL "arm64")
        target_include_directories(tidesdb_cpp PUBLIC /opt/homebrew/include)
        target_link_directories(tidesdb_cpp PUBLIC /opt/homebrew/lib)
    else()
        target_include_directories(tidesdb_cpp PUBLIC /usr/local/include)
        target_link_directories(tidesdb_cpp PUBLIC /usr/local/lib)
    endif()
    target_link_libraries(tidesdb_cpp PUBLIC ${TIDESDB_LIBRARY})
elseif(WIN32)
    if(MINGW)
        target_include_directories(tidesdb_cpp PUBLIC /mingw64/include)
        target_link_directories(tidesdb_cpp PUBLIC /mingw64/lib)
    endif()
    # Link TidesDB and its dependencies (static library needs explicit deps)
    target_link_libraries(tidesdb_cpp PUBLIC
        ${TIDESDB_LIBRARY}
        $<IF:$<TARGET_EXISTS:zstd::libzstd_shared>,zstd::libzstd_shared,zstd::libzstd_static>
        Snappy::snappy
        lz4::lz4
        PThreads4W::PThreads4W
    )
else()
    # Linux and other Unix
    target_link_libraries(tidesdb_cpp PUBLIC ${TIDESDB_LIBRARY})
endif()

# Installation
install(TARGETS tidesdb_cpp
    EXPORT tidesdb_cpp-targets
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    RUNTIME DESTINATION bin
)

install(DIRECTORY include/tidesdb DESTINATION include)

install(EXPORT tidesdb_cpp-targets
    FILE tidesdb_cpp-config.cmake
    NAMESPACE tidesdb::
    DESTINATION lib/cmake/tidesdb_cpp
)

# Tests
if(TIDESDB_CPP_BUILD_TESTS)
    include(FetchContent)
    FetchContent_Declare(
        googletest
        URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
    )
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)

    enable_testing()
    add_executable(tidesdb_cpp_tests tests/tidesdb_test.cpp)
    target_link_libraries(tidesdb_cpp_tests PRIVATE GTest::gtest_main tidesdb_cpp)

    include(GoogleTest)
    gtest_discover_tests(tidesdb_cpp_tests)
endif()