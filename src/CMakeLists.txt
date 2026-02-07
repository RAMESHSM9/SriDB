# Create the storage library
add_library(storage STATIC
    storage/Page.cpp
)

# Tell CMake where to find headers
target_include_directories(storage PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)