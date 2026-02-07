# Create test executable
add_executable(page_test
    PageTest.cpp
)

# Link against storage library and gtest
target_link_libraries(page_test
    storage
    GTest::gtest_main
)

# Register test with CTest
include(GoogleTest)
gtest_discover_tests(page_test)