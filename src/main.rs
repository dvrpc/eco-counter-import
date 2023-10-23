use std::env;
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
use oracle::sql_type::Timestamp;
use oracle::{pool::PoolBuilder, Connection, Error, Statement};
use tracing::{debug, error, info};
use tracing_subscriber::{filter, prelude::*};

#[derive(Debug)]
struct Count {
    location_id: i32,
    datetime: NaiveDateTime,
    total: Option<i32>,
    ped_in: Option<i32>,
    ped_out: Option<i32>,
    bike_in: Option<i32>,
    bike_out: Option<i32>,
}

impl Count {
    fn new(
        location_id: i32,
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
            location_id,
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
    /*
      TODO:
        [ ] determine notification/confirmation system
      Stretch:
        [ ] format time better. See <https://github.com/tokio-rs/tracing/blob/master/tracing-subscriber/src/fmt/time/time_crate.rs>
    */

    // Set up logging/report.
    let log_file = OpenOptions::new()
        .append(true)
        .create(true)
        .open("log.txt")
        .expect("Unable to open log.");

    // Remove any existing report file, create new one.
    let report_filename = "report.txt";
    fs::remove_file(report_filename).ok();
    let report_file = File::create(report_filename).expect("Unable to open report file.");

    // Configure various logs.
    let stdout_log = tracing_subscriber::fmt::layer();
    let log = tracing_subscriber::fmt::layer()
        .with_writer(log_file)
        .with_ansi(false);
    let report = tracing_subscriber::fmt::layer()
        .with_writer(report_file)
        .with_ansi(false);

    // stdout/log get everything, excluding those whose name contains "report_only"
    // report gets everything INFO and above (so not DEBUG or TRACE)
    tracing_subscriber::registry()
        .with(
            stdout_log
                .with_filter(filter::filter_fn(|metadata| {
                    !metadata.name().contains("report_only")
                }))
                .and_then(log),
        )
        .with(report.with_filter(filter::LevelFilter::INFO))
        .init();

    // Elapsed time will be logged.
    let start = time::Instant::now();
    debug!("Import started.");

    // Oracle env vars
    if let Err(e) = dotenvy::dotenv() {
        error!("Unable to load .env file: {e}.");
        return;
    }
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

    // Create CSV reader over file, verify header is what we expect it to be.
    let data_file = match File::open("export.csv") {
        Ok(v) => v,
        Err(e) => {
            error!("Unable to open data file: {e}");
            return;
        }
    };
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
                return;
            }
        },
        None => {
            error!("Header not found.");
            return;
        }
    };

    if header != expected_header {
        error!("Header file does match expected header.");
        return;
    }

    /*
      Loop over all records in the CSV, extracting dates into one vector (in order to delete any
      existing records with that date to prevent adding duplicates) and everything into another
      vector (to be processed/entered into database after deletes complete).
      Separating the delete/insertion allows for far fewer deletes (one per day of month rather
      than one per record).
    */
    debug!("Extracting counts from CSV file.");
    let mut dates = vec![];
    let mut all_counts = vec![];

    for result in rdr.records() {
        let record = match result {
            Ok(v) => v,
            Err(e) => {
                error!("Could not row from CSV: {e}.");
                return;
            }
        };

        // Extract date from datetime, in the format our database expects (DD-MON-YY).
        let datetime = &record[0];
        let datetime = match NaiveDateTime::parse_from_str(datetime, "%b %e, %Y %l:%M %p") {
            Ok(v) => v,
            Err(e) => {
                error!("Could not parse date ({datetime}) from record: {e}.");
                return;
            }
        };

        dates.push(datetime.format("%d-%b-%y").to_string().to_uppercase());

        // Extract everything, by particular location/count, converting to Options from &str.
        let counts = record
            .iter()
            .map(|v| v.parse::<i32>().ok())
            .collect::<Vec<_>>();
        // Bartram
        all_counts.push(Count::new(16, datetime, &counts[1..=5], true, true));
        // Chester Valley Trail
        all_counts.push(Count::new(1, datetime, &counts[6..=10], true, true));
        // Cooper River Trail
        all_counts.push(Count::new(11, datetime, &counts[11..=15], true, true));
        // Cynwyd Heritage Trail
        all_counts.push(Count::new(3, datetime, &counts[16..=20], true, true));
        // Darby Creek Trail
        all_counts.push(Count::new(12, datetime, &counts[21..=25], true, true));
        // Kelly Dr
        all_counts.push(Count::new(5, datetime, &counts[26..=30], true, true));
        // Lawrence Hopewell trail
        all_counts.push(Count::new(8, datetime, &counts[31..=35], true, true));
        // Monroe Twp
        all_counts.push(Count::new(10, datetime, &counts[36..=40], true, true));
        // Pawlings Rd
        all_counts.push(Count::new(2, datetime, &counts[41..=45], true, true));
        // Pine St
        all_counts.push(Count::new(24, datetime, &counts[46..=48], false, true));
        // Port Richmond
        all_counts.push(Count::new(7, datetime, &counts[49..=53], true, true));
        // Schuylkill Banks
        all_counts.push(Count::new(6, datetime, &counts[54..=58], true, true));
        // Spring Mill Station
        all_counts.push(Count::new(13, datetime, &counts[59..=63], true, true));
        // Spruce St
        all_counts.push(Count::new(25, datetime, &counts[64..=66], false, true));
        // Tinicum Park
        all_counts.push(Count::new(23, datetime, &counts[67..=71], true, true));
        // Tullytown
        all_counts.push(Count::new(14, datetime, &counts[72..=76], true, true));
        // US 202 Parkway Trail
        all_counts.push(Count::new(9, datetime, &counts[77..=81], true, true));
        // Washington Cross
        all_counts.push(Count::new(15, datetime, &counts[82..=86], true, true));
        // Waterfront Display
        all_counts.push(Count::new(26, datetime, &counts[87..=91], true, true));
        // Wissahickon Trail
        all_counts.push(Count::new(4, datetime, &counts[92..=96], true, true));
    }

    debug!("Deleting all existing records with same date.");
    dates.sort();
    dates.dedup();
    let pool = PoolBuilder::new(username.clone(), password.clone(), "dvrpcprod_tp_tls")
        .max_connections(50)
        .build()
        .unwrap();

    // Create threads to delete all rows associated with each date
    let mut delete_thread_handles = vec![];
    for date in dates {
        let conn = pool.get().unwrap();
        delete_thread_handles.push(thread::spawn(move || {
            if let Err(e) = conn.execute(
                "delete from TBLCOUNTDATA where to_char(COUNTDATE, 'DD-MON-YY')=:1",
                &[&date],
            ) {
                error!("Error deleting existing records from db for {date}: {e}");
                return;
            }

            match conn.commit() {
                Ok(_) => (),
                Err(e) => {
                    error!("Error committing deletion of existing records from db: {e}");
                    return;
                }
            }
        }));
    }

    for handle in delete_thread_handles {
        match handle.join() {
            Ok(_) => (),
            Err(e) => {
                error!("Error joining delete thread: {:?}.", e);
                return;
            }
        }
    }

    // Create a channel to handle moving data into threads
    let (tx, rx) = channel::unbounded();

    // Create thread to send Counts through the channel
    let sender_thread_handle = thread::spawn(move || {
        for count in all_counts {
            match tx.send(count) {
                Ok(_) => (),
                Err(e) => {
                    error!("Error sending data to channel: {e}.");
                    return;
                }
            }
        }
    });

    // Fork: spawn new threads, with each one adding a receiver, taking a Count from the channel,
    // and inserting it into the database.
    debug!("Inserting counts into database.");
    let mut receiver_thread_handles = vec![];
    let num_inserts = Arc::new(AtomicUsize::new(0));
    for _ in 0..20 {
        let num_inserts = num_inserts.clone();
        let receiver = rx.clone();
        let conn = pool.get().unwrap();
        receiver_thread_handles.push(thread::spawn(move || {
            while let Ok(count) = receiver.recv() {
                match insert(&conn, count) {
                    Ok(_) => (),
                    Err(e) => {
                        error!("Could not insert count: {e}");
                        return;
                    }
                }
                // Increment number of counts (for reporting).
                num_inserts.fetch_add(1, Ordering::Relaxed);
            }
            match conn.commit() {
                Ok(_) => (),
                Err(e) => {
                    error!("Error committing to database: {e}.");
                    return;
                }
            }
        }));
    }

    // Join: wait for all threads to finish.
    match sender_thread_handle.join() {
        Ok(_) => (),
        Err(e) => {
            error!("Error joining sender thread: {:?}.", e);
            return;
        }
    }
    for handle in receiver_thread_handles {
        match handle.join() {
            Ok(_) => (),
            Err(e) => {
                error!("Error joining receiver thread: {:?}.", e);
                return;
            }
        }
    }

    info!(name: "report_only", "Import completed successfully.");
    info!("{:?} counts inserted.", num_inserts);
    info!("Elapsed time: {:?}", start.elapsed());
}

fn insert(conn: &Connection, count: Count) -> Result<Statement, Error> {
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
