use std::fs;

use calamine::{open_workbook, DataType, Range, Reader, Xlsx};
use chrono::prelude::*;
use chrono_tz::US::Eastern;

struct Counter {
    station_name: String,
    datetime: NaiveDateTime,
    ped_in: Option<i32>,
    ped_out: Option<i32>,
    bike_in: Option<i32>,
    bike_out: Option<i32>,
}

// enum for stations so we can match against display when setting station from filename
enum Station {
    CVT,
    KellyDr,
    PineSt,
    SB,
    US202Parkway,
}

fn main() {
    // Crawl specific directory, printing filenames
    for entry in fs::read_dir("spreadsheets").unwrap() {
        // Get Excel file paths
        let path = &entry.unwrap().path();

        // SKip the file if no extension or not .xlsx
        match path.extension() {
            Some(v) if v.to_ascii_lowercase() != "xlsx" => {
                continue;
            }
            Some(_) => (),
            None => continue,
        }

        // set station from filename
        // TODO: for now it's partially done, matching on strings, and so proving that this will work, but I want use enum instead to make it less verbose
        let station: String = match path.file_stem() {
            Some(v) => {
                let new = v.to_ascii_lowercase();
                match new.to_ascii_lowercase().to_str() {
                    Some(v) if v.starts_with("cvt") => "CVT".to_string(),
                    Some(v) if v.starts_with("pine st") => "Pine Street".to_string(),
                    Some(_) => "other".to_string(),
                    None => continue,
                }
            }
            None => continue,
        };

        dbg!(&station);

        // Open the first worksheet of the workbook
        let mut wb: Xlsx<_> = open_workbook(path).expect("Could not open workbook.");
        let ws = wb.worksheet_range_at(0).unwrap().unwrap();

        dbg!(&wb.sheet_names());

        // Iterate through rows of the spreadsheet
        // TODO: remove the take - temporarily using to shorten the output in dbg
        // for row in ws.rows().take(5) {
        //     dbg!(&row);
        // }

        let dt = Eastern.with_ymd_and_hms(2023, 5, 6, 12, 30, 18);

        // determine data structure for each file

        // pull data from each file
    }

    // connect to Oracle

    // delete any data matching what we're about to enter (by date/station)

    // insert data into BIKEPED_TEST database

    // determine notification/confirmation system
}
