# Create test executable
add_executable(page_test
    PageTest.cpp
)
# Link against storage library and gtest
target_link_libraries(page_test
    storage
    GTest::gtest_main
)

add_executable(buffer_test BufferPoolManagerTest.cpp)
target_link_libraries(buffer_test 
    buffer     # BufferPoolManager
    storage    # Page (transitively through buffer)
    GTest::gtest_main
)

# Register test with CTest
include(GoogleTest)
gtest_discover_tests(page_test)
gtest_discover_tests(buffer_test)