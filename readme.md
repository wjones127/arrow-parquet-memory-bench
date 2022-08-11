Parquet Read Memory Usage
================

The benchmarks in this repo are designed to determine to what degree we
currently can tune the Arrow C++ Parquet reader to use less memory. The
Parquet reader is by default tuned to be fast, even if at the cost of
using more memory. Tuning to use less memory might mean that reading
Parquet is slower.

To minimize memory use:

1.  Turn prebuffering off
2.  Set a smaller batch size
3.  Turn on `use_buffered_stream` (use default buffer size)

## Analysis

``` r
library(arrow)
library(ggplot2)
library(dplyr)

data <- read_json_arrow("data.jsonl") %>%
  mutate(
    use_threads = as.logical(use_threads),
    use_buffered_stream = as.logical(use_buffered_stream),
    prebuffer = as.logical(prebuffer)
  )
```

``` r
data %>%
  filter(use_buffered_stream & !use_threads & batch_size == 65536) %>%
  ggplot(aes(x = as.factor(scales::comma(row_group_size)), y = max_memory, fill = prebuffer)) + 
    geom_col(position = "dodge") +
    labs(
      title="Peak memory usage can be much higher with prebuffer=true",
      x = "Number of rows per row group",
      y = "Peak memory usage (bytes)",
      caption = "For each case, use_buffered_stream=true, use_threads=false, batch_size=65536 (default)."
    )
```

![](readme_files/figure-gfm/prebuffer-1.png)

``` r
data %>%
  filter(use_buffered_stream & !use_threads & !prebuffer) %>%
  ggplot(aes(x = as.factor(batch_size), y = max_memory)) + 
    geom_col() +
    facet_grid(~ row_group_size, labeller = label_both) + 
    labs(
      title="Peak memory usage determined by row_group_size",
      subtitle="Small batch size mitigates effect of large row groups",
      x = "Batch size (rows)",
      y = "Peak memory usage (bytes)",
      caption = "For each case, use_buffered_stream=true, use_threads=false, prebuffer=false."
    )
```

![](readme_files/figure-gfm/group-size-1.png)

``` r
data %>%
  filter(use_threads & batch_size == 65536 & !prebuffer) %>%
  # Don't show extraneous data points
  filter(use_buffered_stream == 0 | buffer_size == 16384) %>%
  mutate(buffered_stream = as.factor(ifelse(use_buffered_stream == 1, 
    paste(scales::comma(buffer_size), "bytes"), "off"
  ))) %>%
  ggplot(aes(x = as.factor(buffered_stream), y = max_memory)) + 
    geom_col() +
    facet_grid(~ row_group_size, labeller = label_both) + 
    labs(
      title="enabled_buffered_stream reduces memory usage for larger row groups",
      x = "size of stream buffer",
      y = "Peak memory usage (bytes)",
      caption = "For each case, use_threads=true, prebuffer=false, batch_size=65,536 (default)."
    )
```

![](readme_files/figure-gfm/buffered-stream-1.png)

``` r
data %>%
  filter(use_buffered_stream & !prebuffer & batch_size == 65536) %>%
  ggplot(aes(x = use_threads, y = max_memory)) + 
    geom_col() +
    facet_grid(~ row_group_size, labeller = as_labeller(function(x) paste0("row_group_size=", x))) + 
    labs(
      title="Peak memory usage unaffected by use_threads",
      x = "use_threads=",
      y = "Peak memory usage (bytes)",
      caption = "In all cases shown, use_buffered_stream=true, prebuffer=false, and batch_size=65,536 (default)."
    )
```

![](readme_files/figure-gfm/use-threads-1.png)
