cmake_minimum_required (VERSION 3.5)
project(spitfire)

#set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_BUILD_TYPE RelWithDebInfo)
#set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fpermissive")

macro(use_cxx14)
    if (CMAKE_VERSION VERSION_LESS "3.1")
        if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
            set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=gnu++14")
        endif ()
    else ()
        set (CMAKE_CXX_STANDARD 14)
    endif ()
endmacro(use_cxx14)

use_cxx14()

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -msse4.2 -fno-omit-frame-pointer")

#find_library(NVM_MALLOC_LIBRARY nvmmalloc HINTS misc/nvm-malloc/)

include_directories(include)
include_directories(misc)

file(GLOB_RECURSE BUF_MGR_SRC_FILES src/buf/logging.cpp src/engine/*.cpp src/buf/*.cpp src/util/*.cpp misc/murmur/*.cpp)

file(GLOB_RECURSE YCSB_SRC_FILES src/benchmark/minimizer.cpp src/benchmark/ycsb/*.cpp)

file(GLOB_RECURSE TPCC_SRC_FILES src/benchmark/minimizer.cpp src/benchmark/tpcc/*.cpp)

#add_executable (buf_mgr_unit_test test/buf_mgr_unit_test.cpp ${BUF_MGR_SRC_FILES})

add_executable (btree_unit_test test/btree_unit_test.cpp ${BUF_MGR_SRC_FILES})

add_executable (txn_unit_test test/txn_unit_test.cpp test/testing_transaction_util.cpp ${BUF_MGR_SRC_FILES})

add_executable (ycsb ${YCSB_SRC_FILES} ${BUF_MGR_SRC_FILES})

add_executable (tpcc ${TPCC_SRC_FILES} ${BUF_MGR_SRC_FILES})

#add_executable (hashmap_test test/hashmap_test.cpp test/testing_transaction_util.cpp ${BUF_MGR_SRC_FILES})

#target_link_libraries(buf_mgr_unit_test tbb pthread tcmalloc)

target_link_libraries(btree_unit_test tbb pthread tcmalloc)

target_link_libraries (txn_unit_test tbb pthread tcmalloc)

target_link_libraries (ycsb tbb pthread tcmalloc)

target_link_libraries (tpcc tbb pthread tcmalloc)
