#include <iostream>
#include "arrow/api.h"
#include "parquet/file_reader.h"
#include "parquet/arrow/reader.h"
#include <cxxopts.hpp>

struct ReaderSettings
{
  // Input
  int64_t row_group_size;

  // ReaderProperties
  bool use_buffered_stream;
  int64_t buffer_size;

  // ArrowReaderProperties
  bool use_threads;
  int64_t batch_size;
  bool prebuffer;
};

std::vector<int> iota(int64_t n)
{
  std::vector<int> x(n);
  std::iota(std::begin(x), std::end(x), 0);
  return x;
}

arrow::Status ReadParquet(ReaderSettings settings)
{
  arrow::MemoryPool *pool = arrow::default_memory_pool();

  auto reader_properties = parquet::ReaderProperties(pool);
  reader_properties.set_buffer_size(settings.buffer_size);
  if (settings.use_buffered_stream)
  {
    reader_properties.enable_buffered_stream();
  }
  else
  {
    reader_properties.disable_buffered_stream();
  }

  std::unique_ptr<parquet::ParquetFileReader> file_reader;
  file_reader = parquet::ParquetFileReader::OpenFile("tall.parquet", /*memory_map=*/false, reader_properties);

  auto arrow_reader_props = parquet::ArrowReaderProperties(settings.use_threads);
  arrow_reader_props.set_batch_size(settings.batch_size);
  arrow_reader_props.set_pre_buffer(settings.prebuffer);

  std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
  ARROW_RETURN_NOT_OK(parquet::arrow::FileReader::Make(pool, std::move(file_reader), &arrow_reader));

  // std::shared_ptr<arrow::Table> table;
  // ARROW_RETURN_NOT_OK(arrow_reader->ReadTable(&table));

  std::shared_ptr<::arrow::RecordBatchReader> rb_reader;

  ARROW_RETURN_NOT_OK(arrow_reader->GetRecordBatchReader(
      iota(arrow_reader->num_row_groups()),
      iota(arrow_reader->parquet_reader()->metadata()->num_columns()),
      &rb_reader));
  
  for (arrow::Result<std::shared_ptr<arrow::RecordBatch>> maybe_batch : *rb_reader) {
    continue;
  }

  // Settings
  std::cout << "{\"row_group_size\": " << settings.row_group_size << ", ";
  std::cout << "\"use_buffered_stream\": " << settings.use_buffered_stream << ", ";
  std::cout << "\"buffer_size\": " << settings.buffer_size << ", ";
  std::cout << "\"use_threads\": " << settings.use_threads << ", ";
  std::cout << "\"batch_size\": " << settings.batch_size << ", ";
  std::cout << "\"prebuffer\": " << settings.prebuffer << ", ";

  // Metrics
  std::cout << "\"max_memory\": " << pool->max_memory() << ", ";
  std::cout << "\"bytes_allocated\": " << pool->bytes_allocated() << " }" << std::endl;

  return arrow::Status::OK();
}

int main(int argc, char **argv)
{
  cxxopts::Options options("MyProgram", "One line description of MyProgram");
  options.add_options()
    ("use-buffered-stream", "Whether to use buffered stream")
    ("buffer-size", "Size of buffered stream", cxxopts::value<int64_t>()->default_value("16384"))
    ("use-threads", "Whether to read with multiple threads")
    ("batch-size", "Number of rows to read per batch", cxxopts::value<int64_t>())
    ("pre-buffer", "Whether to pre-buffer bytes")
    ("row-group-size", "Size of row groups in file", cxxopts::value<int64_t>())
    ;

  auto result = options.parse(argc, argv);

  ReaderSettings settings;
  settings.row_group_size = result["row-group-size"].as<int64_t>();
  settings.use_buffered_stream = result["use-buffered-stream"].as<bool>();
  settings.buffer_size = result["buffer-size"].as<int64_t>();
  settings.use_threads = result["use-threads"].as<bool>();
  settings.batch_size = result["batch-size"].as<int64_t>();
  settings.prebuffer = result["pre-buffer"].as<bool>();

  arrow::Status st = ReadParquet(settings);
  if (!st.ok())
  {
    return 1;
  }
  return 0;
}