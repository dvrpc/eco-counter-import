use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;

use chrono::prelude::*;
use csv::StringRecord;
use lazy_static::lazy_static;
use oracle::{Connection, Error, Version};

#[derive(Debug)]
struct Count {
    station_id: i32,
    datetime: NaiveDateTime,
    total: Option<i32>,
    ped_in: Option<i32>,
    ped_out: Option<i32>,
    bike_in: Option<i32>,
    bike_out: Option<i32>,
}

impl Count {
    fn new(
        station_id: i32,
        datetime: NaiveDateTime,
        counts: &[Option<i32>],
        ped: bool,
        bike: bool,
    ) -> Self {
        let mut ped_in = None;
        let mut ped_out = None;
        let mut bike_in = None;
        let mut bike_out = None;

        // `counts` is a slice from the whole row, starting with total (index 0) and followed by
        // either a ped or bike pair (in/out) or both (usually both)

        // Both ped and bike
        if counts.len() == 5 {
            // maybe assert that both bike and ped are true?
            ped_in = counts[1];
            ped_out = counts[2];
            bike_in = counts[3];
            bike_out = counts[4];
        } else if counts.len() == 3 {
            // if bike && ped {
            //     // this would be an error
            // }
            if ped && !bike {
                ped_in = counts[1];
                ped_out = counts[2];
            }
            if !ped && bike {
                bike_in = counts[1];
                bike_out = counts[2];
            }
        }

        Self {
            station_id,
            datetime,
            total: counts[0],
            ped_in,
            ped_out,
            bike_in,
            bike_out,
        }
    }
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

const EXPECTED_HEADER: &[&str] = &[
    "Time",
    "Bartram's Garden",
    "Bartram's Garden Pedestrians NB - Bartram's Garden",
    "Bartram's Garden Pedestrians SB - Bartram's Garden",
    "Bartram's Garden Cyclists NB - Bartram's Garden",
    "Bartram's Garden Cyclists SB - Bartram's Garden",
    "Chester Valley Trail - East Whiteland Twp",
    "Chester Valley Trail - East Whiteland Twp CVT - EB - Pedestrian",
    "Chester Valley Trail - East Whiteland Twp CVT - WB - Pedestrian",
    "Chester Valley Trail - East Whiteland Twp CVT - EB - Bicycle",
    "Chester Valley Trail - East Whiteland Twp CVT - WB - Bicycle",
    "Cooper River Trail",
    "Cooper River Trail - EB Pedestrian",
    "Cooper River Trail - WB Pedestrian",
    "Cooper River Trail - EB Bicycle",
    "Cooper River Trail - WB Bicycle",
    "Cynwyd Heritage Trail",
    "Cynwyd Heritage Trail Pedestrian IN",
    "Cynwyd Heritage Trail Pedestrian OUT",
    "Cynwyd Heritage Trail CHT - WB - Bicycle",
    "Cynwyd Heritage Trail CHT - EB - Bicycle",
    "Darby Creek Trail",
    "Darby Creek Trail - Pedestrians - SB",
    "Darby Creek Trail - Pedestrians - NB",
    "Darby Creek Trail - Bicycle - SB",
    "Darby Creek Trail - Bicycle - NB",
    "Kelly Dr - Schuylkill River Trail",
    "Kelly Dr - Schuylkill River Trail Kelly Drive - Pedestrians - NB",
    "Kelly Dr - Schuylkill River Trail Kelly Drive - Pedestrians - SB",
    "Kelly Dr - Schuylkill River Trail Kelly Drive - Bicycle - NB",
    "Kelly Dr - Schuylkill River Trail Kelly Drive - Bicycle - SB",
    "Lawrence - Hopewell Trail",
    "Lawrence - Hopewell Trail LHT - Pedestrian - NB",
    "Lawrence - Hopewell Trail LHT - Pedestrian - SB",
    "Lawrence - Hopewell Trail LHT - Bicycle - NB",
    "Lawrence - Hopewell Trail LHT - Bicycle - SB",
    "Monroe Twp",
    "Monroe Twp Pedestrian IN",
    "Monroe Twp Pedestrian OUT",
    "Monroe Twp Monroe - Bicycle - EB",
    "Monroe Twp Monroe - Bicycle - WB",
    "Pawlings Rd - Schuylkill River Trail",
    "Pawlings Rd - Schuylkill River Trail Pawlings Rd - WB Pedestrian",
    "Pawlings Rd - Schuylkill River Trail Pawlings Rd - EB Pedestrian",
    "Pawlings Rd - Schuylkill River Trail Pawlings Rd - WB - Bicycle",
    "Pawlings Rd - Schuylkill River Trail Pawlings Rd - EB - Bicycle",
    "Pine St",
    "Pine St Pedestrian IN",
    "Pine St Pedestrian OUT",
    "Port Richmond",
    "Port Richmond - WB - Pedestrian",
    "Port Richmond - EB - Pedestrian",
    "Port Richmond - WB - Bicycle",
    "Port Richmond - EB - Bicycle",
    "Schuylkill Banks",
    "Schuylkill Banks - Pedestrian - NB",
    "Schuylkill Banks - Pedestrian - SB",
    "Schuylkill Banks - Bicycle - NB",
    "Schuylkill Banks - Bicycle - SB",
    "Spring Mill Station",
    "Spring Mill Station Pedestrians EB - To Philadelphia",
    "Spring Mill Station Pedestrians WB - To Conshohocken",
    "Spring Mill Station Cyclists EB - To Philadelphia",
    "Spring Mill Station Cyclists WB - To Conshohocken",
    "Spruce St",
    "Spruce St Pedestrian IN",
    "Spruce St Pedestrian OUT",
    "Tinicum Park - D&L Trail",
    "Tinicum Park - D&L Trail Hugh Moore Park - D&L Trail Pedestrians Wilkes-Barre (Bethlehem)",
    "Tinicum Park - D&L Trail Pedestrians Bristol (New Hope)",
    "Tinicum Park - D&L Trail Hugh Moore Park - D&L Trail Cyclists Wilkes-Barre (Bethlehem)",
    "Tinicum Park - D&L Trail Cyclists Bristol (New Hope)",
    "Tullytown",
    "Tullytown Pedestrians NB - Towards Trenton - IN",
    "Tullytown Pedestrians SB - Towards Tullytown - OUT",
    "Tullytown Cyclists NB - Towards Trenton - IN",
    "Tullytown Cyclists SB - Towards Tullytown - OUT",
    "US 202 Parkway Trail",
    "US 202 Parkway Trail US 202 Parkway - SB - Pedestrian",
    "US 202 Parkway Trail US 202 Parkway - NB - Pedestrian",
    "US 202 Parkway Trail US 202 Parkway - SB - Bicycle",
    "US 202 Parkway Trail US 202 Parkway - NB - Bicycle",
    "Washington Crossing",
    "Washington Crossing Pedestrians NB - To New Hope - IN",
    "Washington Crossing Pedestrians SB - To Yardley - OUT",
    "Washington Crossing Cyclists NB - To New Hope - IN",
    "Washington Crossing Cyclists SB - To Yardley - OUT",
    "Waterfront Display",
    "Waterfront Display Pedestrian IN",
    "Waterfront Display Pedestrian OUT",
    "Waterfront Display Cyclist IN",
    "Waterfront Display Cyclist OUT",
    "Wissahickon Trail",
    "Wissahickon Trail - Pedestrians - SB",
    "Wissahickon Trail - Pedestrians - NB",
    "Wissahickon Trail - Bicycles - SB",
    "Wissahickon Trail - Bicycles - NB",
    "",
];

fn main() {
    // Remove any existing error file, create new one to hold errors.
    let error_filename = "errors.txt";
    fs::remove_file(error_filename).ok();
    let mut error_file = File::create(error_filename).expect("Unable to open file to hold errors.");

    // Create CSV reader over file, verify header is what we expect it to be.
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

    let expected_header = StringRecord::from(EXPECTED_HEADER);
    let header: StringRecord = rdr.records().skip(1).take(1).next().unwrap().unwrap();

    if header != expected_header {
        error_file
            .write_all(b"Header in file does not match expected header.")
            .unwrap();
        return;
    }

    // Process data rows
    for result in rdr.records() {
        let record = result.unwrap();
        dbg!(&record);
        // first col is date, for all stations
        let datetime: String = record.iter().take(1).collect();
        let datetime = NaiveDateTime::parse_from_str(&datetime, "%b %e, %Y %l:%M %p").unwrap();
        dbg!(datetime);

        // now get counts, and convert to Options from &str
        let counts = record
            .iter()
            .map(|v| v.parse::<i32>().ok())
            .collect::<Vec<_>>();

        // extract each counter's data from full row of data
        let bartram = Count::new(16, datetime, &counts[1..=5], true, true);
        let chester_valley_trail = Count::new(1, datetime, &counts[6..=10], true, true);
        // let cooper_river_trail: Counter::new()
    }

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
