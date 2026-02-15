# Create storage library (Page)
add_library(storage STATIC
    storage/Page.cpp
)

target_include_directories(storage PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)

# Create buffer library (BufferPoolManager)
add_library(buffer STATIC
    buffer/BufferPoolManager.cpp
)

target_include_directories(buffer PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)

# Buffer depends on storage!
target_link_libraries(buffer PUBLIC storage)