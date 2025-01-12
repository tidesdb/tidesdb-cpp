# build instructions--
# cmake -S . -B build
# cmake --build build
# cmake --install build
cmake_minimum_required(VERSION 3.25)
project(tidesdb_cpp CXX)

set(CMAKE_CXX_STANDARD 20)
set(PROJECT_VERSION 0.1.0)
add_compile_options(-Wextra -Wall -Werror)

find_library(LIBRARY_TIDEDB NAMES tidesdb REQUIRED) # require tidesdb library

add_library(tidesdb_cpp SHARED tidesdb.cpp)

install(FILES tidesdb.hpp
        DESTINATION include)

if(APPLE AND CMAKE_SYSTEM_PROCESSOR STREQUAL "arm64")
    target_include_directories(tidesdb_cpp PUBLIC /opt/homebrew/include)
    target_link_directories(tidesdb_cpp PUBLIC /opt/homebrew/lib)
elseif(APPLE)
    target_include_directories(tidesdb_cpp PUBLIC /usr/local/include)
    target_link_directories(tidesdb_cpp PUBLIC /usr/local/lib)
endif()

target_link_libraries(tidesdb_cpp PUBLIC ${LIBRARY_TIDEDB})

install(TARGETS tidesdb_cpp
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)

include(FetchContent)
FetchContent_Declare(
        googletest
        URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
)

set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

enable_testing()

add_executable(tests test/tests.cpp)
target_link_libraries(tests GTest::gtest_main tidesdb_cpp ${LIBRARY_TIDEDB})

include(GoogleTest)
gtest_discover_tests(tests)