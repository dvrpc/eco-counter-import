use std::collections::HashMap;
use std::env;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::thread;
use std::time;

use chrono::prelude::*;
use crossbeam::channel;
use csv::StringRecord;
use log::{debug, error, info};
use oracle::sql_type::Timestamp;
use oracle::{pool::PoolBuilder, Connection, Error as OracleError, Statement};
use simplelog::*;

#[derive(Debug, Clone)]
struct IndividualCount {
    location_id: i32,
    datetime: NaiveDateTime,
    total: Option<i32>,
    ped_in: Option<i32>,
    ped_out: Option<i32>,
    bike_in: Option<i32>,
    bike_out: Option<i32>,
}

impl IndividualCount {
    fn new(
        location_id: i32,
        datetime: NaiveDateTime,
        counts: &[Option<i32>],
        ped: bool,
        bike: bool,
    ) -> Result<IndividualCount, CountError> {
        let mut ped_in = None;
        let mut ped_out = None;
        let mut bike_in = None;
        let mut bike_out = None;

        // `counts` is a slice from the whole row, starting with total (index 0) and followed by
        // either a ped or bike pair (in/out) or both (usually both)
        if counts.len() == 5 {
            if !bike && !ped {
                return Err(CountError::TooMany);
            }
            ped_in = counts[1];
            ped_out = counts[2];
            bike_in = counts[3];
            bike_out = counts[4];
        } else if counts.len() == 3 {
            if bike && ped {
                return Err(CountError::TooFew);
            }
            if ped && !bike {
                ped_in = counts[1];
                ped_out = counts[2];
            }
            if !ped && bike {
                bike_in = counts[1];
                bike_out = counts[2];
            }
        } else {
            return Err(CountError::UnexpectedNumber);
        }

        Ok(Self {
            location_id,
            datetime,
            total: counts[0],
            ped_in,
            ped_out,
            bike_in,
            bike_out,
        })
    }
}

// This will catch any misconfiguration between the bools/counts provided in new().
#[derive(Debug)]
enum CountError {
    TooFew,
    TooMany,
    UnexpectedNumber,
}

impl fmt::Display for CountError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CountError::TooFew => {
                write!(f, "Misconfiguration of count: expected more fields.")
            }
            CountError::TooMany => {
                write!(f, "Misconfiguration of count: expected fewer fields.")
            }
            CountError::UnexpectedNumber => {
                write!(f, "Expected 3 or 5 fields, got different amount.")
            }
        }
    }
}

#[derive(Debug, Clone)]
struct AggregatedCount {
    location_id: i32,
    date: NaiveDate,
    total_ped: Option<i32>,
    total_bike: Option<i32>,
    total: Option<i32>,
}

impl AggregatedCount {
    fn new(
        location_id: i32,
        date: NaiveDate,
        total_ped: Option<i32>,
        total_bike: Option<i32>,
        total: Option<i32>,
    ) -> Self {
        Self {
            location_id,
            date,
            total_ped,
            total_bike,
            total,
        }
    }
}

// Threads are limited to this number in order to limit number of concurrent connections to
// database, otherwise this could easily triple to improve performance.
const NUM_THREADS: usize = 10;

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

const TIME_BETWEEN_LOOPS: u64 = 15;

fn main() {
    // Load file containing environment variables, panic if it doesn't exist.
    dotenvy::dotenv().expect("Unable to load .env file.");

    // Get env var for path where CSV and log will be, panic if it doesn't exist.
    let storage_path =
        env::var("PATH_TO_CSV_AND_LOG").expect("Unable to load storage path from .env file.");

    // Set up logging, panic if it fails.
    let config = ConfigBuilder::new().set_time_format_rfc3339().build();
    CombinedLogger::init(vec![
        TermLogger::new(
            LevelFilter::Debug,
            config.clone(),
            TerminalMode::Mixed,
            ColorChoice::Auto,
        ),
        WriteLogger::new(
            LevelFilter::Info,
            config,
            OpenOptions::new()
                .append(true)
                .create(true)
                .open(format!("{storage_path}/log.txt"))
                .expect("Could not open log file."),
        ),
    ])
    .expect("Could not configure logging.");

    // create closure to remove CSV file
    let remove_csv = || {
        // Remove the csv
        info!("Deleting CSV file.");
        fs::remove_file(format!("{storage_path}/export.csv")).ok()
    };

    // Oracle env vars
    let username = match env::var("USERNAME") {
        Ok(v) => v,
        Err(e) => {
            error!("Unable to load username from .env file: {e}.");
            return;
        }
    };
    let password = match env::var("PASSWORD") {
        Ok(v) => v,
        Err(e) => {
            error!("Unable to load password from .env file: {e}.");
            return;
        }
    };

    'mainloop: loop {
        // Open CSV file and create reader over it, or wait and try again
        let data_file = match File::open(format!("{storage_path}/export.csv")) {
            Ok(v) => v,
            Err(_) => {
                debug!("CSV file not located to import data from.");
                thread::sleep(time::Duration::from_secs(TIME_BETWEEN_LOOPS));
                continue 'mainloop;
            }
        };

        // Elapsed time will be logged.
        let start = time::Instant::now();
        info!("Import started.");

        // Create CSV reader over file, verify header is what we expect it to be.
        let mut rdr = csv::ReaderBuilder::new()
            .flexible(true)
            .has_headers(false)
            .from_reader(data_file);

        let expected_header = StringRecord::from(EXPECTED_HEADER);
        let header: StringRecord = match rdr.records().skip(1).take(1).next() {
            Some(v) => match v {
                Ok(v) => v,
                Err(e) => {
                    error!("Could not parse header: {e}");
                    remove_csv();
                    continue 'mainloop;
                }
            },
            None => {
                error!("Header not found.");
                remove_csv();
                continue 'mainloop;
            }
        };

        if header != expected_header {
            error!("Header file does match expected header.");
            remove_csv();
            continue 'mainloop;
        }

        /*
          Loop over all records in the CSV, extracting dates into one vector (in order to delete any
          existing records with that date to prevent adding duplicates) and everything into another
          vector (to be processed/entered into database after deletes complete).
          Separating the delete/insertion allows for far fewer deletes (one per day of month rather
          than one per record).
        */
        info!("Extracting counts from CSV file.");
        let mut dates = vec![];
        let mut all_counts = vec![];

        for result in rdr.records() {
            let record = match result {
                Ok(v) => v,
                Err(e) => {
                    error!("Could not read row from CSV: {e}.");
                    remove_csv();
                    continue 'mainloop;
                }
            };

            // Extract date from datetime, in the format our database expects (DD-MON-YY).
            let datetime = &record[0];
            let datetime = match NaiveDateTime::parse_from_str(datetime, "%b %e, %Y %l:%M %p") {
                Ok(v) => v,
                Err(e) => {
                    error!("Could not parse date ({datetime}) from record: {e}.");
                    remove_csv();
                    continue 'mainloop;
                }
            };

            dates.push(datetime.format("%d-%b-%y").to_string().to_uppercase());

            // Extract everything, by particular location/count, converting to Options from &str.
            let counts = record
                .iter()
                .map(|v| v.parse::<i32>().ok())
                .collect::<Vec<_>>();

            // Creation of `IndividualCount`s could possibly result in out-of-bounds error, so
            // check length first before trying to create them, in order to log error and continue
            // running the program.
            if counts.len() != EXPECTED_HEADER.len() {
                error!(
                    "Incorrect number of fields in row. Expected {}, found {}.",
                    EXPECTED_HEADER.len(),
                    counts.len()
                );
                remove_csv();
                continue 'mainloop;
            }
            // Create counts.
            let current_location = "Bartram";
            let count = match IndividualCount::new(16, datetime, &counts[1..=5], true, true) {
                Ok(v) => v,
                Err(e) => {
                    error!("Error creating count for {}: {}", current_location, e);
                    remove_csv();
                    continue 'mainloop;
                }
            };
            all_counts.push(count);
            let current_location = "Chester Valley Trail";
            let count = match IndividualCount::new(1, datetime, &counts[6..=10], true, true) {
                Ok(v) => v,
                Err(e) => {
                    error!("Error creating count for {}: {}", current_location, e);
                    remove_csv();
                    continue 'mainloop;
                }
            };
            all_counts.push(count);
            let current_location = "Cooper River Trail";
            let count = match IndividualCount::new(11, datetime, &counts[11..=15], true, true) {
                Ok(v) => v,
                Err(e) => {
                    error!("Error creating count for {}: {}", current_location, e);
                    remove_csv();
                    continue 'mainloop;
                }
            };
            all_counts.push(count);
            let current_location = "Cynwyd Heritage Trail";
            let count = match IndividualCount::new(3, datetime, &counts[16..=20], true, true) {
                Ok(v) => v,
                Err(e) => {
                    error!("Error creating count for {}: {}", current_location, e);
                    remove_csv();
                    continue 'mainloop;
                }
            };
            all_counts.push(count);
            let current_location = "Darby Creek Trail";
            let count = match IndividualCount::new(12, datetime, &counts[21..=25], true, true) {
                Ok(v) => v,
                Err(e) => {
                    error!("Error creating count for {}: {}", current_location, e);
                    remove_csv();
                    continue 'mainloop;
                }
            };
            all_counts.push(count);
            let current_location = "Kelly Dr";
            let count = match IndividualCount::new(5, datetime, &counts[26..=30], true, true) {
                Ok(v) => v,
                Err(e) => {
                    error!("Error creating count for {}: {}", current_location, e);
                    remove_csv();
                    continue 'mainloop;
                }
            };
            all_counts.push(count);
            let current_location = "Lawrence Hopewell trail";
            let count = match IndividualCount::new(8, datetime, &counts[31..=35], true, true) {
                Ok(v) => v,
                Err(e) => {
                    error!("Error creating count for {}: {}", current_location, e);
                    remove_csv();
                    continue 'mainloop;
                }
            };
            all_counts.push(count);
            let current_location = "Monroe Twp";
            let count = match IndividualCount::new(10, datetime, &counts[36..=40], true, true) {
                Ok(v) => v,
                Err(e) => {
                    error!("Error creating count for {}: {}", current_location, e);
                    remove_csv();
                    continue 'mainloop;
                }
            };
            all_counts.push(count);
            let current_location = "Pawlings Rd";
            let count = match IndividualCount::new(2, datetime, &counts[41..=45], true, true) {
                Ok(v) => v,
                Err(e) => {
                    error!("Error creating count for {}: {}", current_location, e);
                    remove_csv();
                    continue 'mainloop;
                }
            };
            all_counts.push(count);
            let current_location = "Pine Street";
            let count = match IndividualCount::new(24, datetime, &counts[46..=48], false, true) {
                Ok(v) => v,
                Err(e) => {
                    error!("Error creating count for {}: {}", current_location, e);
                    remove_csv();
                    continue 'mainloop;
                }
            };
            all_counts.push(count);
            let current_location = "Port Richmond";
            let count = match IndividualCount::new(7, datetime, &counts[49..=53], true, true) {
                Ok(v) => v,
                Err(e) => {
                    error!("Error creating count for {}: {}", current_location, e);
                    remove_csv();
                    continue 'mainloop;
                }
            };
            all_counts.push(count);
            let current_location = "Schuylkill Banks";
            let count = match IndividualCount::new(6, datetime, &counts[54..=58], true, true) {
                Ok(v) => v,
                Err(e) => {
                    error!("Error creating count for {}: {}", current_location, e);
                    remove_csv();
                    continue 'mainloop;
                }
            };
            all_counts.push(count);
            let current_location = "Spring Mill Station";
            let count = match IndividualCount::new(13, datetime, &counts[59..=63], true, true) {
                Ok(v) => v,
                Err(e) => {
                    error!("Error creating count for {}: {}", current_location, e);
                    remove_csv();
                    continue 'mainloop;
                }
            };
            all_counts.push(count);
            let current_location = "Spruce St";
            let count = match IndividualCount::new(25, datetime, &counts[64..=66], false, true) {
                Ok(v) => v,
                Err(e) => {
                    error!("Error creating count for {}: {}", current_location, e);
                    remove_csv();
                    continue 'mainloop;
                }
            };
            all_counts.push(count);
            let current_location = "Tinicum Park";
            let count = match IndividualCount::new(23, datetime, &counts[67..=71], true, true) {
                Ok(v) => v,
                Err(e) => {
                    error!("Error creating count for {}: {}", current_location, e);
                    remove_csv();
                    continue 'mainloop;
                }
            };
            all_counts.push(count);
            let current_location = "Tullytown";
            let count = match IndividualCount::new(14, datetime, &counts[72..=76], true, true) {
                Ok(v) => v,
                Err(e) => {
                    error!("Error creating count for {}: {}", current_location, e);
                    remove_csv();
                    continue 'mainloop;
                }
            };
            all_counts.push(count);
            let current_location = "US 202 Parkway Trail";
            let count = match IndividualCount::new(9, datetime, &counts[77..=81], true, true) {
                Ok(v) => v,
                Err(e) => {
                    error!("Error creating count for {}: {}", current_location, e);
                    remove_csv();
                    continue 'mainloop;
                }
            };
            all_counts.push(count);
            let current_location = "Washington Cross";
            let count = match IndividualCount::new(15, datetime, &counts[82..=86], true, true) {
                Ok(v) => v,
                Err(e) => {
                    error!("Error creating count for {}: {}", current_location, e);
                    remove_csv();
                    continue 'mainloop;
                }
            };
            all_counts.push(count);
            let current_location = "Waterfront Display";
            let count = match IndividualCount::new(26, datetime, &counts[87..=91], true, true) {
                Ok(v) => v,
                Err(e) => {
                    error!("Error creating count for {}: {}", current_location, e);
                    remove_csv();
                    continue 'mainloop;
                }
            };
            all_counts.push(count);
            let current_location = "Wissahickon Trail";
            let count = match IndividualCount::new(4, datetime, &counts[92..=96], true, true) {
                Ok(v) => v,
                Err(e) => {
                    error!("Error creating count for {}: {}", current_location, e);
                    remove_csv();
                    continue 'mainloop;
                }
            };
            all_counts.push(count);
        }

        // Now take this data in `all_counts`, and sum by date/location_id
        let mut daily_counts = HashMap::new();

        for count in all_counts.clone() {
            let date = count.datetime.date();

            // running_xxx are the running totals that are (possibly) updated on each loop
            let (running_ped, running_bike, running_total) = daily_counts
                .entry((count.location_id, date))
                .or_insert((None, None, None));

            // sum ped in/out
            let mut ped_total = None;

            if let Some(v) = count.ped_in {
                ped_total = Some(v);
            }
            if let Some(v) = count.ped_out {
                if let Some(w) = ped_total {
                    ped_total = Some(w + v)
                } else {
                    ped_total = Some(v)
                }
            }
            // now add it to our running sum
            if let Some(v) = ped_total {
                if let Some(w) = running_ped {
                    *w += v
                } else {
                    *running_ped = Some(v)
                }
            }

            // sum bike in/out
            let mut bike_total = None;

            if let Some(v) = count.bike_in {
                bike_total = Some(v);
            }
            if let Some(v) = count.bike_out {
                if let Some(w) = bike_total {
                    bike_total = Some(w + v)
                } else {
                    bike_total = Some(v)
                }
            }
            // now add it to our running sum
            if let Some(v) = bike_total {
                if let Some(w) = running_bike {
                    *w += v
                } else {
                    *running_bike = Some(v)
                }
            }

            // sum total
            let mut total: Option<i32> = None;

            if let Some(v) = count.total {
                total = Some(v);
            }
            // add total to running total
            if let Some(v) = total {
                if let Some(w) = running_total {
                    *w += v
                } else {
                    *running_total = Some(v)
                }
            }
        }

        // Flatten that hashmap into a vec.
        let mut flattened_daily_counts = vec![];
        for ((location_id, date), (ped_total, bike_total, total)) in daily_counts {
            flattened_daily_counts.push(AggregatedCount::new(
                location_id,
                date,
                ped_total,
                bike_total,
                total,
            ));
        }

        dates.sort();
        dates.dedup();

        // Create connection pool.
        let pool = match PoolBuilder::new(username.clone(), password.clone(), "dvrpcprod_tp_tls")
            .max_connections(NUM_THREADS as u32)
            .build()
        {
            Ok(v) => v,
            Err(e) => {
                error!("Unable to create connection pool: {e}");
                remove_csv();
                continue 'mainloop;
            }
        };

        // Create a channel to handle moving dates into threads
        let (tx, rx) = channel::unbounded();

        // Create thread to send dates through the channel
        let sender_thread_handle = thread::spawn(move || {
            for date in dates {
                match tx.send(date) {
                    Ok(_) => (),
                    Err(e) => {
                        error!("Error sending date to channel: {e}.");
                        return;
                    }
                }
            }
        });

        // Fork: spawn new threads, with each one adding a receiver, taking a date from the channel,
        // and deleting existing records for that date.
        info!("Deleting all existing records w/ same date from tables TBLCOUNTDATA & TBLHEADER).");
        let mut receiver_thread_handles = vec![];
        let num_deletes = Arc::new(AtomicUsize::new(0));
        for _ in 0..NUM_THREADS {
            let num_deletes = num_deletes.clone();
            let receiver = rx.clone();
            let conn = pool.get().unwrap();

            receiver_thread_handles.push(thread::spawn(move || {
                while let Ok(date) = receiver.recv() {
                    // Delete from TBLCOUNTDATA and TBLHEADER.
                    // If error, log it and then propagate it to main thread.
                    conn.execute(
                        "delete from TBLCOUNTDATA where to_char(COUNTDATE, 'DD-MON-YY')=:1",
                        &[&date],
                    )
                    .map_err(|e| {
                        error!("Error deleting existing records from TBLCOUNTDATA for {date}: {e}");
                    })
                    .unwrap();

                    conn.execute(
                        "delete from TBLHEADER where to_char(COUNTDATE, 'DD-MON-YY')=:1",
                        &[&date],
                    )
                    .map_err(|e| {
                        error!("Error deleting existing records from TBLHEADER for {date}: {e}");
                    })
                    .unwrap();

                    // Commit. If error, log it and then propagate it to main thread.
                    conn.commit()
                        .map_err(|e| {
                            error!(
                                "Error committing deletion of existing record for {date} from db: {e}"
                            )
                        })
                        .unwrap();
                    // Increment number of counts (for reporting).
                    num_deletes.fetch_add(1, Ordering::Relaxed);
                }
                })
            );
        }

        // Join: wait for delete sender/receiver threads to finish
        match sender_thread_handle.join() {
            Ok(_) => (),
            Err(e) => {
                error!("{:?}", e);
                remove_csv();
                continue 'mainloop;
            }
        }
        for handle in receiver_thread_handles {
            match handle.join() {
                Ok(_) => (),
                Err(e) => {
                    error!("{:?}", e);
                    remove_csv();
                    continue 'mainloop;
                }
            }
        }

        // Create a channel to handle moving all_counts into threads
        let (tx, rx) = channel::unbounded();

        // Create thread to send Counts through the channel
        let sender_thread_handle = thread::spawn(move || {
            for count in all_counts {
                match tx.send(count) {
                    Ok(_) => (),
                    Err(e) => {
                        error!("Error sending individual data to channel: {e}.");
                        return;
                    }
                }
            }
        });

        // Fork: spawn new threads, with each one adding a receiver, taking a Count from the channel,
        // and inserting it into the database.
        info!("Inserting individual counts into database.");
        let mut receiver_thread_handles = vec![];
        let num_individual_inserts = Arc::new(AtomicUsize::new(0));
        for _ in 0..NUM_THREADS {
            let num_individual_inserts = num_individual_inserts.clone();
            let receiver = rx.clone();
            let conn = pool.get().unwrap();
            receiver_thread_handles.push(thread::spawn(move || {
                while let Ok(count) = receiver.recv() {
                    // Insert. If error, log it and then propagate it to main thread.
                    insert_individual_count(&conn, count)
                        .map_err(|e| {
                            error!("Could not insert count: {e}");
                        })
                        .unwrap();

                    // Increment number of counts (for reporting).
                    num_individual_inserts.fetch_add(1, Ordering::Relaxed);
                }

                // Commit. If error, log it and then propagate it to main thread.
                conn.commit()
                    .map_err(|e| error!("Error committing insert to database: {e}"))
                    .unwrap();
            }));
        }

        // Join: wait for insert sender/receiver threads to finish
        match sender_thread_handle.join() {
            Ok(_) => (),
            Err(e) => {
                error!("{:?}", e);
                remove_csv();
                continue 'mainloop;
            }
        }
        for handle in receiver_thread_handles {
            match handle.join() {
                Ok(_) => (),
                Err(e) => {
                    error!("{:?}", e);
                    remove_csv();
                    continue 'mainloop;
                }
            }
        }

        // Create a channel to handle moving flattened_daily_counts into threads
        let (tx, rx) = channel::unbounded();

        // Create thread to send counts through the channel
        let sender_thread_handle = thread::spawn(move || {
            for count in flattened_daily_counts {
                match tx.send(count) {
                    Ok(_) => (),
                    Err(e) => {
                        error!("Error sending aggregated data to channel: {e}.");
                        return;
                    }
                }
            }
        });

        // Fork: spawn new threads, with each one adding a receiver, taking a count from the channel,
        // and inserting it into the database.
        info!("Inserting aggregated counts into database.");
        let mut receiver_thread_handles = vec![];
        let num_aggregated_inserts = Arc::new(AtomicUsize::new(0));
        for _ in 0..NUM_THREADS {
            let num_aggregated_inserts = num_aggregated_inserts.clone();
            let receiver = rx.clone();
            let conn = pool.get().unwrap();
            receiver_thread_handles.push(thread::spawn(move || {
                while let Ok(count) = receiver.recv() {
                    // Insert. If error, log it and then propagate it to main thread.
                    insert_aggregated_count(&conn, count)
                        .map_err(|e| {
                            error!("Could not insert count: {e}");
                        })
                        .unwrap();

                    // Increment number of counts (for reporting).
                    num_aggregated_inserts.fetch_add(1, Ordering::Relaxed);
                }

                // Commit. If error, log it and then propagate it to main thread.
                conn.commit()
                    .map_err(|e| error!("Error committing insert to database: {e}"))
                    .unwrap();
            }));
        }

        // Join: wait for insert sender/receiver threads to finish.
        match sender_thread_handle.join() {
            Ok(_) => (),
            Err(e) => {
                error!("{:?}", e);
                remove_csv();
                continue 'mainloop;
            }
        }
        for handle in receiver_thread_handles {
            match handle.join() {
                Ok(_) => (),
                Err(e) => {
                    error!("{:?}", e);
                    remove_csv();
                    continue 'mainloop;
                }
            }
        }

        info!("Import completed successfully.");
        info!("Records for {:?} dates deleted.", num_deletes);
        info!("{:?} individual counts inserted.", num_individual_inserts);
        info!("{:?} aggregated counts inserted.", num_aggregated_inserts);
        info!("Elapsed time: {:?}", start.elapsed());

        // Remove the csv
        remove_csv();

        // Wait to try again
        thread::sleep(time::Duration::from_secs(TIME_BETWEEN_LOOPS));
    }
}

fn insert_individual_count(
    conn: &Connection,
    count: IndividualCount,
) -> Result<Statement, OracleError> {
    // convert datetime
    let oracle_dt = Timestamp::new(
        count.datetime.year(),
        count.datetime.month(),
        count.datetime.day(),
        count.datetime.hour(),
        count.datetime.minute(),
        count.datetime.second(),
        0,
    );

    conn.execute("insert into TBLCOUNTDATA (locationid, countdate, total, pedin, pedout, bikein, bikeout, counttime) values (:1, :2, :3, :4, :5, :6, :7, :8)",
        &[
            &count.location_id,
            &oracle_dt,
            &count.total,
            &count.ped_in,
            &count.ped_out,
            &count.bike_in,
            &count.bike_out,
            &oracle_dt,
        ],
    )
}

fn insert_aggregated_count(
    conn: &Connection,
    count: AggregatedCount,
) -> Result<Statement, OracleError> {
    // convert datetime
    let oracle_dt = Timestamp::new(
        count.date.year(),
        count.date.month(),
        count.date.day(),
        0,
        0,
        0,
        0,
    );

    conn.execute("insert into TBLHEADER (locationid, countdate, totalped, totalbike, total) values (:1, :2, :3, :4, :5)",
        &[
            &count.location_id,
            &oracle_dt,
            &count.total_ped,
            &count.total_bike,
            &count.total,
        ],
    )
}
