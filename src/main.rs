use std::collections::HashMap;
use std::env;
use std::fs;

use calamine::{open_workbook, DataType, Range, Reader, Xlsx};
use chrono::prelude::*;
use chrono_tz::US::Eastern;
use lazy_static::lazy_static;
use oracle::{Connection, Error, Version};

struct Counter {
    station_name: String,
    datetime: NaiveDateTime,
    ped_in: Option<i32>,
    ped_out: Option<i32>,
    bike_in: Option<i32>,
    bike_out: Option<i32>,
}

lazy_static! {
    static ref STATIONS: HashMap<&'static str, i32> =
        HashMap::from([
        ("CVT", 1), // Chester Valley Trail
        ("", 2), // Schuylkill River Trail (Pawlings Rd)
        ("", 3), // Cynwyd Heritage Trail
        ("", 4), // Wissahickon Trail
        ("Kelly Dr", 5), // Schuylkill River Trail (Kelly Dr)
        ("SB", 6), // Schuylkill River Trail (Schuykill Banks)
        ("", 7), // Delaware River Trail (Port Richmond)
        ("", 8), // Lawrence-Hopewell Trail
        ("US 202 Parkway", 9), // US 202 Parkway Trail
        ("", 10), // Monroe Township Trail
        ("", 11), // Cooper River Trail
        ("", 12), // Darby Creek Trail
        ("", 13), // Schuylkill River Trail (Spring Mill)
        ("", 14), // D&L Canal Trail (Tullytown)
        ("", 15), // D&L Canal Trail (Washington Crossing)
        ("", 16), // Schuylkill River Trail (Bartram's Garden)
        ("", 17), // Chelten Ave East Side Sidewalk
        ("", 18), // Chelten Ave West Side Sidewalk
        ("", 19), // Lancaster Ave North Side Sidewalk
        ("", 20), // Lancaster Ave South Side Sidewalk
        ("", 21), // N 5th St East Side Sidewalk
        ("", 22), // N 5th St West Side Sidewalk
        ("", 23), // D&L Canal Trail (Tinicum Park)
        ("Pine St", 24), // Pine St Bike Lanes
        ("", 25), // Spruce St Bike Lanes
        ("", 26), // Delaware River Trail (Waterfront)
        ("", 27), // WLHC - Laurel Hill East
        ("", 28), // WLHC - Pencoyd
    ]);
}

fn main() {
    // Oracle env vars and connection
    dotenvy::dotenv().expect("Unable to load .env file");
    let username = env::var("USERNAME").expect("Unable to load username from .env file.");
    let password = env::var("PASSWORD").expect("Unable to load password from .env file.");

    let conn = Connection::connect(username, password, "dvrpcprod_tp_tls").unwrap();

    let sql = "SELECT * FROM TBLHEADER";

    let rows = conn.query(sql, &[]);

    for row in rows.unwrap() {
        dbg!(row.unwrap());
    }

    // TODO: probably want to log these in place instead of creating vec to later print out
    let mut errors = vec![];

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

        // Set station from filename (format is "Station name here monthname YYYY") where "station
        // name here" can be any number of words separated by a space)
        let station: i32 = match path.file_stem() {
            Some(v) => {
                let stem: &str = v.to_str().unwrap().rsplitn(3, ' ').collect::<Vec<_>>()[2];
                match STATIONS.get(stem) {
                    Some(v) => *v,
                    None => {
                        errors.push(format!("No stations matches file {}", stem));
                        continue;
                    }
                }
            }
            None => continue,
        };

        dbg!(&station);

        // Open the first worksheet of the workbook
        let mut wb: Xlsx<_> = open_workbook(path).expect("Could not open workbook.");
        let ws = wb.worksheet_range_at(0).unwrap().unwrap();

        dbg!(&wb.sheet_names());

        // determine data structure for each file
        // Iterate through rows of the spreadsheet
        // first column: datetime (15 minute interval)
        // second column: total
        // third - end: pedestrian or bike; inbound/outbound
        // TODO: remove the take - temporarily using to shorten the output in dbg
        // for row in ws.rows().take(5) {
        //     dbg!(&row);
        // }

        let dt = Eastern.with_ymd_and_hms(2023, 5, 6, 12, 30, 18);

        // pull data from each file
    }

    // connect to Oracle

    // delete any data matching what we're about to enter (by date/station)

    // insert data into BIKEPED_TEST database

    // determine notification/confirmation system
}
