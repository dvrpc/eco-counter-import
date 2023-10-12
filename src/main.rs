use std::collections::HashMap;
use std::env;
use std::fs;

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

/*
You can also get a CSV, rather than a spreadsheet. I'm thinking that it'll just be easier to verify
that the header is correct and then grab data by expected order




*/

fn main() {
    // TODO: probably want to log these in place instead of creating vec to later print out
    // let mut errors = vec![];

    // Iterate through rows

    // let dt = Eastern.with_ymd_and_hms(2023, 5, 6, 12, 30, 18);

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

    // delete any data matching what we're about to enter (by date/station)

    // insert data into BIKEPED_TEST database

    // determine notification/confirmation system
}
