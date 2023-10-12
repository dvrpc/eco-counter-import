use std::collections::HashMap;
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::Write;

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
    // Set up file to hold errors and open data file, removing any existing file first.
    let error_filename = "errors.txt";
    fs::remove_file(error_filename).ok();
    let mut error_file = File::create(error_filename).expect("Unable to open file to hold errors.");

    // Open and process the CSV
    let data_file = match File::open("export.csv") {
        Ok(v) => v,
        Err(e) => {
            error_file
                .write_all(format!("Unable to open data file: {e}").as_bytes())
                .unwrap();
            return;
        }
    };

    let mut rdr = csv::ReaderBuilder::new()
        .flexible(true)
        .has_headers(false)
        .from_reader(data_file);

    let header = rdr.records().skip(1).take(1).collect::<Vec<_>>();

    dbg!(header);

    for result in rdr.records().skip(1).take(5) {
        let record = result.unwrap();
        dbg!(record);
    }

    // let dt = Eastern.with_ymd_and_hms(2023, 5, 6, 12, 30, 18);

    /*
    // connect to Oracle
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
    */

    // delete any data matching what we're about to enter (by date/station)

    // insert data into BIKEPED_TEST database

    // determine notification/confirmation system

    // If error file is empty, rm it
    if fs::read_to_string(error_filename).unwrap().is_empty() {
        fs::remove_file(error_filename).ok();
    }
}
