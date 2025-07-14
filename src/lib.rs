#![deny(rust_2018_idioms, unsafe_code)]
#![deny(clippy::unwrap_used)]

use ::strings::sanitize_spaces;
pub use calamine::Xlsx;
use calamine::{Data, DataType as _, Range, Reader, open_workbook};
use chrono::NaiveDateTime;
use polars::prelude::*;
use std::{
    collections::HashMap,
    fs::File,
    io::BufReader,
    path::{Path, PathBuf},
};
use thiserror::Error;

#[rustfmt::skip] const INT: u8      = 0b1000_0000;
#[rustfmt::skip] const FLOAT: u8    = 0b0100_0000;
#[rustfmt::skip] const STRING: u8   = 0b0010_0000;
#[rustfmt::skip] const BOOL: u8     = 0b0001_0000;
#[rustfmt::skip] const DATETIME: u8 = 0b0000_1000;
#[rustfmt::skip] const EMPTY: u8    = 0b0000_0000;

fn ref_to_string(value: &Data) -> String {
    match value {
        Data::String(v) => v.to_owned(),
        _ => value.to_string(),
    }
}

/// Read a sheet with two columns as a `HashMap`<Column1, Column2>
///
///
/// # Errors
///
/// Returns an error if unable to open the workbook or sheet
pub fn read_set_from_sheet<R: Reader<BufReader<File>>>(
    path: impl AsRef<Path>,
    sheet: &str,
    has_header: bool,
) -> ReaderResult<HashMap<String, String>> {
    let mut excel: R = open_workbook(path.as_ref())
        .map_err(|e| ReaderError::OpenWorkbook(path.as_ref().to_path_buf(), format!("{e:?}")))?;
    let range = excel
        .worksheet_range(sheet)
        .map_err(|e| ReaderError::OpenWorksheet(sheet.to_string(), format!("{e:?}")))?;

    read_set_from_range(range, has_header)
}

fn read_set_from_range(
    range: Range<Data>,
    has_header: bool,
) -> ReaderResult<HashMap<String, String>> {
    let mut res = HashMap::new();
    let mut row = range.rows().filter_map(|c| {
        if let [k, v, ..] = c {
            Some((k, v))
        } else {
            None
        }
    });

    if has_header {
        row.next();
    }

    for (k, v) in row {
        let k = ref_to_string(k);
        let v = ref_to_string(v);

        let k = sanitize_spaces(k.trim());
        let v = sanitize_spaces(v.trim());

        res.insert(k, v);
    }

    Ok(res)
}

/// Read a sheet with two columns as a `HashMap`<Column1, Column2>
///
///
/// # Errors
///
/// Returns an error if unable to open the workbook or sheet
pub fn read_set_from_sheet_at<R: Reader<BufReader<File>>>(
    path: impl AsRef<Path>,
    pos: usize,
    has_header: bool,
) -> ReaderResult<HashMap<String, String>> {
    let mut excel: R = open_workbook(path.as_ref())
        .map_err(|e| ReaderError::OpenWorkbook(path.as_ref().to_path_buf(), format!("{e:?}")))?;
    let sheet_names = excel.sheet_names();
    let Some(sheet) = sheet_names.get(pos) else {
        return ReaderResult::Err(ReaderError::OpenWorksheet(
            format!("No such sheet at pos {pos:?}"),
            format!("A sheet in the nth position {pos:?} does not exist"),
        ));
    };
    let range = excel
        .worksheet_range(sheet)
        .map_err(|e| ReaderError::OpenWorksheet(sheet.to_string(), format!("{e:?}")))?;

    read_set_from_range(range, has_header)
}

#[derive(Error, Debug)]
pub enum ReaderError {
    #[error("failed to open workbook at `{0:?}` with `{1}`")]
    OpenWorkbook(PathBuf, String),
    #[error("failed to open worksheet `{0:?}` with `{1}`")]
    OpenWorksheet(String, String),
    #[error("sheet `{0:?}` does not have headers")]
    NoHeaders(String),
    #[error("failed to add column `{0}` with `{1}`")]
    AddColumn(String, String),
    #[error("Got errors while reading cells")]
    CellErrors(Vec<CellError>),
}

#[derive(Debug)]
pub struct CellError {
    pub header: String,
    pub row: usize,
    pub col: usize,
    pub error: calamine::CellErrorType,
}

pub type ReaderResult<T> = std::result::Result<T, ReaderError>;

/// Read all sheets from a path into a collection of `DataFrame`
///
/// # Errors
///
/// This function will return an error if theres an error opening sheets, workbooks or while
/// adding columns into dataframes
pub fn read_sheets<R, P>(path: P) -> ReaderResult<PlIndexMap<String, DataFrame>>
where
    R: Reader<BufReader<File>>,
    P: AsRef<Path>,
{
    let mut res = PlIndexMap::default();
    let mut excel: R = open_workbook(path.as_ref())
        .map_err(|e| ReaderError::OpenWorkbook(path.as_ref().to_path_buf(), format!("{e:?}")))?;
    let sheets = excel.sheet_names();

    for sheet in sheets {
        let df = read_sheet_from_sheets(&mut excel, &sheet, Options::default())?;
        res.insert(sheet, df);
    }

    Ok(res)
}

#[derive(Debug, Default, Clone, Copy)]
pub struct Options {
    pub skip_last: usize,
    pub skip_first: usize,
    /// When true, the reader treats empty strings as None values.
    pub empty_str_as_none: bool,
    /// When strict is true, the reader will return an error if the sheet does contain cells with errors, otherwise it will log them and place a None value instead.
    pub strict_cell_errors: bool,
}

/// Read a single sheet named `sheet` from the path `path` with options to skip rows.
///
/// This function opens the workbook at `path`, finds the sheet named `sheet`,
/// and reads its content into a Polars `DataFrame`.
///
/// # Errors
///
/// This function will return an error if:
/// - The workbook at the given path cannot be opened (`ReaderError::OpenWorkbook`).
/// - The specified sheet does not exist or cannot be accessed within the workbook (`ReaderError::OpenWorksheet`).
/// - The sheet does not contain a header row (`ReaderError::NoHeaders`).
/// - There is an issue adding a column to the `DataFrame`, possibly due to inconsistent
///   row counts after skipping (`ReaderError::AddColumn`).
pub fn read_sheet<P, R>(path: P, sheet: &str) -> ReaderResult<DataFrame>
where
    R: Reader<BufReader<File>>,
    P: AsRef<Path>,
{
    read_sheet_with_opts::<_, R>(path, sheet, Options::default())
}

/// Read a single sheet named `sheet` from the path `path` with options to skip rows.
///
/// This function opens the workbook at `path`, finds the sheet named `sheet`,
/// and reads its content into a Polars `DataFrame`. It allows skipping a specified
/// number of rows from the beginning or/and end after processing the header row via the `ReadSheetOptions` struct.
///
/// # Errors
///
/// This function will return an error if:
/// - The workbook at the given path cannot be opened (`ReaderError::OpenWorkbook`).
/// - The specified sheet does not exist or cannot be accessed within the workbook (`ReaderError::OpenWorksheet`).
/// - The sheet does not contain a header row (`ReaderError::NoHeaders`).
/// - There is an issue adding a column to the `DataFrame`, possibly due to inconsistent
///   row counts after skipping (`ReaderError::AddColumn`).
pub fn read_sheet_with_opts<P, R>(path: P, sheet: &str, opts: Options) -> ReaderResult<DataFrame>
where
    R: Reader<BufReader<File>>,
    P: AsRef<Path>,
{
    log::debug!("Reading sheet {} from {:?}", sheet, path.as_ref().display());
    let mut excel: R = open_workbook(path.as_ref())
        .map_err(|e| ReaderError::OpenWorkbook(path.as_ref().to_path_buf(), format!("{e:?}")))?;

    read_sheet_from_sheets(&mut excel, sheet, opts)
}

/// Read the nth sheet from the path `path`
///
/// # Errors
///
/// This function will return an error if theres an error opening sheets, workbooks or while
/// adding columns into dataframes
pub fn read_sheet_nth<P, R>(path: P, nth: usize) -> ReaderResult<DataFrame>
where
    R: Reader<BufReader<File>>,
    P: AsRef<Path>,
{
    let mut excel: R = open_workbook(path.as_ref())
        .map_err(|e| ReaderError::OpenWorkbook(path.as_ref().to_path_buf(), format!("{e:?}")))?;
    let sheet_names = excel.sheet_names();
    let sheet = sheet_names.get(nth).ok_or({
        ReaderError::OpenWorksheet(
            format!("No nth sheet {nth:?}"),
            format!(
                "The sheet {:?} does not have a nth sheet {nth}",
                path.as_ref().display()
            ),
        )
    })?;

    read_sheet_from_sheets(&mut excel, sheet, Options::default())
}

/// Reads a single sheet into a dataframe, detecting de data type for each column
///
/// # Errors
///
/// This function will return an error if there is a problem adding columns because of different sizes,
///  if the sheet does not have headers, etc.
///
/// # Panics
///
/// The function panics if values can not be generalized to a single type, this is a bug and must be
/// reported
fn read_sheet_from_sheets<R: Reader<BufReader<File>>>(
    excel: &mut R,
    sheet: &str,
    opts @ Options {
        skip_last,
        skip_first,
        ..
    }: Options,
) -> ReaderResult<DataFrame> {
    let mut df = DataFrame::default();

    log::debug!("Reading sheet {sheet}");
    let range = excel
        .worksheet_range(sheet)
        .map_err(|e| ReaderError::OpenWorksheet(sheet.to_string(), format!("{e:?}")))?;

    log::debug!("Sheet {sheet} has been read");
    let mut rows = range
        .rows()
        .filter(|row| row.first().is_some_and(|c| !c.is_empty()));

    let Some(header_row) = rows.next() else {
        return Err(ReaderError::NoHeaders(sheet.to_string()));
    };

    let headers = header_row.iter().map(|d| {
        if let Data::String(s) = d {
            s.to_string()
        } else {
            d.to_string()
        }
    });
    log::debug!(
        "Sheet {} has headers: {:?}",
        sheet,
        headers.clone().collect::<Vec<_>>()
    );

    let has_values = headers
        .clone()
        .enumerate()
        .map(|(n_col, _)| {
            let rows_len = rows.clone().count();
            rows.clone()
                .map(|row| &row[n_col])
                .take(rows_len - skip_last)
                .skip(skip_first)
                .count()
        })
        .any(|count| count > 0);

    if !has_values {
        for header in headers.clone() {
            let series = Series::new(header.clone().into(), Vec::<Option<String>>::new());
            df.with_column(series)
                .map_err(|e| ReaderError::AddColumn(header, format!("{e:?}")))?;
        }
        log::debug!("No values provided, empty sheet. Adding headers as columns of type String");
        return Ok(df);
    }

    let mut errors = vec![];
    for (n_col, header) in headers.enumerate() {
        let mut flags = EMPTY;
        let rows_len = rows.clone().count();
        let values = rows
            .clone()
            .map(|row| &row[n_col])
            .take(rows_len - skip_last)
            .skip(skip_first);
        let values_len = values.clone().count();

        for (n_row, value) in values.clone().enumerate() {
            flags |= match value {
                Data::Int(_) => INT,
                Data::Float(_) => FLOAT,
                Data::String(x) if x.trim().is_empty() => EMPTY,
                Data::String(_) | Data::DateTimeIso(_) | Data::DurationIso(_) => STRING,
                Data::Error(err) => {
                    log::error!(
                        "Error with col {header:?} (no. {n_col}) row {n_row} parsing value: {err}"
                    );
                    EMPTY
                }
                Data::Bool(_) => BOOL,
                Data::DateTime(_) => DATETIME,
                Data::Empty => EMPTY,
            };
        }

        let flags = if (flags & STRING) == STRING {
            STRING
        } else if ((flags & INT) | (flags & FLOAT)) == (INT | FLOAT) {
            FLOAT
        } else {
            flags
        };

        let dtype = match flags {
            FLOAT => DataType::Float64,
            INT => DataType::Int64,
            BOOL => DataType::Boolean,
            DATETIME => DataType::Date,
            _ => DataType::String,
        };

        let mut vec_int64: Vec<Option<i64>> = vec![];
        let mut vec_float64: Vec<Option<f64>> = vec![];
        let mut vec_string: Vec<Option<String>> = vec![];
        let mut vec_boolean: Vec<Option<bool>> = vec![];
        let mut vec_date: Vec<Option<NaiveDateTime>> = vec![];

        for (n_row, value) in values.cloned().enumerate() {
            if let Err(err) = populate_vectors(
                opts,
                n_row,
                value,
                &dtype,
                &mut vec_int64,
                &mut vec_float64,
                &mut vec_string,
                &mut vec_boolean,
                &mut vec_date,
            ) {
                errors.push(CellError {
                    row: n_row,
                    col: n_col,
                    error: err,
                    header: header.clone(),
                });
            }
        }

        if values_len != 0 {
            assert_eq!(
                [
                    u8::from(vec_boolean.iter().any(|v| v.is_some())),
                    u8::from(vec_date.iter().any(|v| v.is_some())),
                    u8::from(vec_int64.iter().any(|v| v.is_some())),
                    u8::from(vec_float64.iter().any(|v| v.is_some())),
                    u8::from(vec_string.iter().any(|v| v.is_some())),
                ]
                .iter()
                .sum::<u8>(),
                1,
                "Expected exactly one non-empty vector"
            );
        }

        let smol_header = PlSmallStr::from_str(&header);
        let series = match dtype {
            DataType::Boolean => Series::new(smol_header, vec_boolean),
            DataType::Int64 => Series::new(smol_header, vec_int64),
            DataType::Float64 => Series::new(smol_header, vec_float64),
            DataType::String => Series::new(smol_header, vec_string),
            DataType::Date => Series::new(smol_header, vec_date),
            _ => unreachable!(),
        };

        log::debug!("Adding column {header} with dtype {dtype:?}");
        df.with_column(series)
            .map_err(|e| ReaderError::AddColumn(header, format!("{e:?}")))?;
    }

    if errors.is_empty() {
        Ok(df)
    } else {
        Err(ReaderError::CellErrors(errors))
    }
}

#[allow(clippy::too_many_arguments)]
fn populate_vectors(
    Options {
        empty_str_as_none,
        strict_cell_errors,
        ..
    }: Options,
    n_row: usize,
    value: Data,
    dtype: &DataType,
    vec_int64: &mut Vec<Option<i64>>,
    vec_float64: &mut Vec<Option<f64>>,
    vec_string: &mut Vec<Option<String>>,
    vec_boolean: &mut Vec<Option<bool>>,
    vec_date: &mut Vec<Option<NaiveDateTime>>,
) -> Result<(), calamine::CellErrorType> {
    match value {
        Data::Int(v) if *dtype == DataType::Int64 => vec_int64.push(Some(v)),
        Data::Float(v) if *dtype == DataType::Float64 => vec_float64.push(Some(v)),
        Data::String(v) if *dtype == DataType::String => {
            let sanitize_spaces = ::strings::sanitize_spaces(&v);
            vec_string.push(
                (!sanitize_spaces.is_empty() || !empty_str_as_none).then_some(sanitize_spaces),
            );
        }
        Data::Bool(v) if *dtype == DataType::Boolean => vec_boolean.push(Some(v)),
        Data::DateTime(dur) if *dtype == DataType::Date => {
            vec_date.push(Some(
                match chrono::DateTime::from_timestamp_millis(
                    ((dur.as_f64() - 25569.0) * 86400.0 * 1000.0) as i64,
                ) {
                    Some(d) => d.naive_local(),
                    None => return Err(calamine::CellErrorType::Div0),
                },
            ));
        }
        Data::DateTimeIso(v) if *dtype == DataType::String => vec_string.push(Some(v.to_owned())),
        Data::DurationIso(v) if *dtype == DataType::String => vec_string.push(Some(v.to_owned())),
        Data::Error(e) if strict_cell_errors => {
            return Err(e);
        }
        _ => {
            if let Data::Error(e) = value {
                log::debug!("Inserting None instead of error {e} in row {n_row}");
            }

            match *dtype {
                DataType::Boolean => vec_boolean.push(None),
                DataType::Int64 => vec_int64.push(None),
                DataType::Float64 => vec_float64.push(None),
                DataType::String => vec_string.push(None),
                DataType::Date => vec_date.push(None),
                _ => unreachable!("We must assign only one of the above"),
            }
        }
    }

    Ok(())
}
