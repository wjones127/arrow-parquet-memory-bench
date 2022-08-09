project(ParquetMemoryTest)

find_package(Arrow REQUIRED)

add_executable(memory_test main.cc)
target_link_libraries(memory_test PRIVATE arrow_shared parquet)
set_property(TARGET memory_test PROPERTY CXX_STANDARD 17)