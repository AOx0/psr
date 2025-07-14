# `psr`

This Rust library provides functionalities for reading data from spreadsheet files (like Excel or ODS) using the `calamine` library and converting them into [Polars](https://pola.rs/) `DataFrame`s.

## Example

```rust
use psr::{read_sheet, ReaderResult, Xlsx, ReadSheetOptions};
use polars::prelude::DataFrame;

fn main() -> ReaderResult<()> {
    // Example of reading a specific sheet
    let df: DataFrame = read_sheet::<_, Xlsx>("path/to/your_file.xlsx", "SheetName")?;
    println!("Read DataFrame:\n{}", df);

    // Example with row skipping options
    let options = ReadSheetOptions { skip_first: 2, skip_last: 1 };
    let df_skipped: DataFrame = read_sheet_with_opts::<_, Xlsx>("path/to/your_file.xlsx", "SheetName", options)?;
    println!("Read DataFrame (with skipping):\n{}", df_skipped);

    Ok(())
}
```
