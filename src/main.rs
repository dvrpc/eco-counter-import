use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;

use chrono::prelude::*;
use csv::StringRecord;
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

const EXPECTED_HEADER: &[&str] = &[
    "Time",
    "Bartram's Garden", // 16 (locationid)
    "Bartram's Garden Pedestrians NB - Bartram's Garden",
    "Bartram's Garden Pedestrians SB - Bartram's Garden",
    "Bartram's Garden Cyclists NB - Bartram's Garden",
    "Bartram's Garden Cyclists SB - Bartram's Garden",
    "Chester Valley Trail - East Whiteland Twp", // 1
    "Chester Valley Trail - East Whiteland Twp CVT - EB - Pedestrian",
    "Chester Valley Trail - East Whiteland Twp CVT - WB - Pedestrian",
    "Chester Valley Trail - East Whiteland Twp CVT - EB - Bicycle",
    "Chester Valley Trail - East Whiteland Twp CVT - WB - Bicycle",
    "Cooper River Trail", // 11
    "Cooper River Trail - EB Pedestrian",
    "Cooper River Trail - WB Pedestrian",
    "Cooper River Trail - EB Bicycle",
    "Cooper River Trail - WB Bicycle",
    "Cynwyd Heritage Trail", // 3
    "Cynwyd Heritage Trail Pedestrian IN",
    "Cynwyd Heritage Trail Pedestrian OUT",
    "Cynwyd Heritage Trail CHT - WB - Bicycle",
    "Cynwyd Heritage Trail CHT - EB - Bicycle",
    "Darby Creek Trail", // 12
    "Darby Creek Trail - Pedestrians - SB",
    "Darby Creek Trail - Pedestrians - NB",
    "Darby Creek Trail - Bicycle - SB",
    "Darby Creek Trail - Bicycle - NB",
    "Kelly Dr - Schuylkill River Trail", // 5
    "Kelly Dr - Schuylkill River Trail Kelly Drive - Pedestrians - NB",
    "Kelly Dr - Schuylkill River Trail Kelly Drive - Pedestrians - SB",
    "Kelly Dr - Schuylkill River Trail Kelly Drive - Bicycle - NB",
    "Kelly Dr - Schuylkill River Trail Kelly Drive - Bicycle - SB",
    "Lawrence - Hopewell Trail", // 8
    "Lawrence - Hopewell Trail LHT - Pedestrian - NB",
    "Lawrence - Hopewell Trail LHT - Pedestrian - SB",
    "Lawrence - Hopewell Trail LHT - Bicycle - NB",
    "Lawrence - Hopewell Trail LHT - Bicycle - SB",
    "Monroe Twp", // 10
    "Monroe Twp Pedestrian IN",
    "Monroe Twp Pedestrian OUT",
    "Monroe Twp Monroe - Bicycle - EB",
    "Monroe Twp Monroe - Bicycle - WB",
    "Pawlings Rd - Schuylkill River Trail", // 2
    "Pawlings Rd - Schuylkill River Trail Pawlings Rd - WB Pedestrian",
    "Pawlings Rd - Schuylkill River Trail Pawlings Rd - EB Pedestrian",
    "Pawlings Rd - Schuylkill River Trail Pawlings Rd - WB - Bicycle",
    "Pawlings Rd - Schuylkill River Trail Pawlings Rd - EB - Bicycle",
    "Pine St",                // 24 "Pine St Bike Lanes"  - one-way, east-bound
    "Pine St Pedestrian IN",  // misnamed and empty, but total is all we need
    "Pine St Pedestrian OUT", // misnamed and empty, but total is all we need
    "Port Richmond",          // 7
    "Port Richmond - WB - Pedestrian",
    "Port Richmond - EB - Pedestrian",
    "Port Richmond - WB - Bicycle",
    "Port Richmond - EB - Bicycle",
    "Schuylkill Banks", // 6
    "Schuylkill Banks - Pedestrian - NB",
    "Schuylkill Banks - Pedestrian - SB",
    "Schuylkill Banks - Bicycle - NB",
    "Schuylkill Banks - Bicycle - SB",
    "Spring Mill Station", // 13
    "Spring Mill Station Pedestrians EB - To Philadelphia",
    "Spring Mill Station Pedestrians WB - To Conshohocken",
    "Spring Mill Station Cyclists EB - To Philadelphia",
    "Spring Mill Station Cyclists WB - To Conshohocken",
    "Spruce St",                // 25 "Spruce St Bike Lanes" - one-way, west-bound
    "Spruce St Pedestrian IN",  // misnamed and empty, but total is all we need
    "Spruce St Pedestrian OUT", // misnamed and empty, but total is all we need
    "Tinicum Park - D&L Trail", // 23
    "Tinicum Park - D&L Trail Hugh Moore Park - D&L Trail Pedestrians Wilkes-Barre (Bethlehem)",
    "Tinicum Park - D&L Trail Pedestrians Bristol (New Hope)",
    "Tinicum Park - D&L Trail Hugh Moore Park - D&L Trail Cyclists Wilkes-Barre (Bethlehem)",
    "Tinicum Park - D&L Trail Cyclists Bristol (New Hope)",
    "Tullytown", // 14
    "Tullytown Pedestrians NB - Towards Trenton - IN",
    "Tullytown Pedestrians SB - Towards Tullytown - OUT",
    "Tullytown Cyclists NB - Towards Trenton - IN",
    "Tullytown Cyclists SB - Towards Tullytown - OUT",
    "US 202 Parkway Trail", // 9
    "US 202 Parkway Trail US 202 Parkway - SB - Pedestrian",
    "US 202 Parkway Trail US 202 Parkway - NB - Pedestrian",
    "US 202 Parkway Trail US 202 Parkway - SB - Bicycle",
    "US 202 Parkway Trail US 202 Parkway - NB - Bicycle",
    "Washington Crossing", // 15
    "Washington Crossing Pedestrians NB - To New Hope - IN",
    "Washington Crossing Pedestrians SB - To Yardley - OUT",
    "Washington Crossing Cyclists NB - To New Hope - IN",
    "Washington Crossing Cyclists SB - To Yardley - OUT",
    "Waterfront Display", // 26
    "Waterfront Display Pedestrian IN",
    "Waterfront Display Pedestrian OUT",
    "Waterfront Display Cyclist IN",
    "Waterfront Display Cyclist OUT",
    "Wissahickon Trail", // 4
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

    let mut all_counts = vec![];

    // Process data rows
    for result in rdr.records() {
        let record = result.unwrap();

        // first col is date, for all stations
        let datetime: String = record.iter().take(1).collect();
        let datetime = NaiveDateTime::parse_from_str(&datetime, "%b %e, %Y %l:%M %p").unwrap();

        // now get counts, and convert to Options from &str
        let counts = record
            .iter()
            .map(|v| v.parse::<i32>().ok())
            .collect::<Vec<_>>();

        // extract each counter's data from full row of data
        let bartram = Count::new(16, datetime, &counts[1..=5], true, true);
        let chester_valley_trail = Count::new(1, datetime, &counts[6..=10], true, true);
        let cooper_river_trail = Count::new(11, datetime, &counts[11..=15], true, true);
        let cynwyd_heritage_tail = Count::new(3, datetime, &counts[16..=20], true, true);
        let darby_creek_trail = Count::new(12, datetime, &counts[21..=25], true, true);
        let kelly_dr = Count::new(5, datetime, &counts[26..=30], true, true);
        let lawrence_hopewell_tail = Count::new(8, datetime, &counts[31..=35], true, true);
        let monroe_twp = Count::new(10, datetime, &counts[36..=40], true, true);
        let pawlings_rd = Count::new(2, datetime, &counts[41..=45], true, true);
        let pine_st = Count::new(24, datetime, &counts[46..=48], false, true);
        let port_richmond = Count::new(7, datetime, &counts[49..=53], true, true);
        let schuylkill_banks = Count::new(6, datetime, &counts[54..=58], true, true);
        let spring_mill_station = Count::new(13, datetime, &counts[59..=63], true, true);
        let spruce_st = Count::new(25, datetime, &counts[64..=66], false, true);
        let tinicum_park = Count::new(23, datetime, &counts[67..=71], true, true);
        let tullytown = Count::new(14, datetime, &counts[72..=76], true, true);
        let us_202_parkway_trail = Count::new(9, datetime, &counts[77..=81], true, true);
        let washington_crossing = Count::new(15, datetime, &counts[82..=86], true, true);
        let waterfront_display = Count::new(26, datetime, &counts[87..=91], true, true);
        let wissahickon_trail = Count::new(4, datetime, &counts[92..=96], true, true);

        all_counts.push(bartram);
        all_counts.push(chester_valley_trail);
        all_counts.push(cooper_river_trail);
        all_counts.push(cynwyd_heritage_tail);
        all_counts.push(darby_creek_trail);
        all_counts.push(kelly_dr);
        all_counts.push(lawrence_hopewell_tail);
        all_counts.push(monroe_twp);
        all_counts.push(pawlings_rd);
        all_counts.push(pine_st);
        all_counts.push(port_richmond);
        all_counts.push(schuylkill_banks);
        all_counts.push(spring_mill_station);
        all_counts.push(spruce_st);
        all_counts.push(tinicum_park);
        all_counts.push(tullytown);
        all_counts.push(us_202_parkway_trail);
        all_counts.push(washington_crossing);
        all_counts.push(waterfront_display);
        all_counts.push(wissahickon_trail);
    }

    dbg!(all_counts);
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
