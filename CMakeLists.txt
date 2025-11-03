cmake_minimum_required(VERSION 3.25)
project(tidesdb_cpp CXX)

set(CMAKE_CXX_STANDARD 17)
set(PROJECT_VERSION 1.0.0)
add_compile_options(-Wextra -Wall -Werror)

find_library(LIBRARY_TIDEDB NAMES tidesdb REQUIRED)

# INTERFACE library for header-only
add_library(tidesdb_cpp INTERFACE)

if(APPLE AND CMAKE_SYSTEM_PROCESSOR STREQUAL "arm64")
    target_include_directories(tidesdb_cpp INTERFACE /opt/homebrew/include)
    target_link_directories(tidesdb_cpp INTERFACE /opt/homebrew/lib)
elseif(APPLE)
    target_include_directories(tidesdb_cpp INTERFACE /usr/local/include)
    target_link_directories(tidesdb_cpp INTERFACE /usr/local/lib)
endif()

target_link_libraries(tidesdb_cpp INTERFACE ${LIBRARY_TIDEDB})

install(FILES tidesdb.hpp DESTINATION include)
install(TARGETS tidesdb_cpp EXPORT tidesdb_cpp-targets)

include(FetchContent)
FetchContent_Declare(
        googletest
        URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

enable_testing()
add_executable(tests test/tests.cpp)
target_link_libraries(tests GTest::gtest_main tidesdb_cpp)

include(GoogleTest)
gtest_discover_tests(tests)