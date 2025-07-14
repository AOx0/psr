use psr::{Xlsx, read_sheet, read_sheets};
use std::env;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: {} <excel_file_path>", args[0]);
        eprintln!("Example: {} data.xlsx", args[0]);
        std::process::exit(1);
    }

    let excel_path = &args[1];

    println!("Opening Excel file: {excel_path}");

    // Read all sheets from the Excel file
    println!("\n=== Reading all sheets ===");
    let sheets = read_sheets::<Xlsx<_>, _>(excel_path)?;

    for (sheet_name, dataframe) in &sheets {
        println!("\nSheet: {sheet_name}");
        println!("Shape: {:?}", dataframe.shape());
        println!("Columns: {:?}", dataframe.get_column_names());
        println!("First 5 rows:");
        println!("{}", dataframe.head(Some(5)));
    }

    // If there are sheets, read the first one specifically
    if let Some((first_sheet_name, _)) = sheets.iter().next() {
        println!("\n=== Reading specific sheet: {first_sheet_name} ===");
        let specific_df = read_sheet::<_, Xlsx<_>>(excel_path, first_sheet_name)?;
        println!("Shape: {:?}", specific_df.shape());
        println!("{}", specific_df.head(Some(10)));
    }

    Ok(())
}
