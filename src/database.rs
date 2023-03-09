use diesel::{Connection, PgConnection, RunQueryDsl};

use schema::finished_logs;
use schema::starting_logs;

use crate::database::model::{FinishedLog, NewFinishedLog, NewStartingLog, StartingLog};

pub mod model;
pub mod schema;

pub fn establish_connection() -> PgConnection {
    let database_url = &dotenv::var("DATABASE_URL").expect("DATABASE_URL must be set");
    match PgConnection::establish(database_url) {
        Ok(t) => t,
        Err(e) => panic!("Failed to connect to database {}", e),
    }
}

pub fn save_starting_logs(logs: &Vec<NewStartingLog>, connection: &mut PgConnection) {
    let split_logs = split_logs_starting(logs);
    for split in split_logs {
        diesel::insert_into(starting_logs::table)
            .values(split)
            .execute(connection)
            .expect("Error saving log");
    }
}

pub fn save_finished_logs(logs: &Vec<NewFinishedLog>, connection: &mut PgConnection) {
    let split_logs = split_logs_finished(logs);
    println!("split_logs size:{}", split_logs.len());
    for split in split_logs {
        println!("Split size: {}", split.len());
        println!("Split: {:?}", split);
        diesel::insert_into(finished_logs::table)
            .values(split)
            .execute(connection)
            .expect("Error saving log");
    }
}

fn split_logs_finished(logs: &Vec<NewFinishedLog>) -> Vec<&[NewFinishedLog]> {
    let max_size = 65534;
    let num_splits = (logs.len() + max_size - 1) / max_size;
    let mut result = Vec::with_capacity(num_splits);
    let mut start = 0;
    for _ in 0..num_splits {
        let end = start + max_size;
        result.push(&logs[start..end.min(logs.len())]);
        start = end;
    }
    result
}


fn split_logs_starting(logs: &Vec<NewStartingLog>) -> Vec<&[NewStartingLog]> {
    let max_size = 65534;
    let num_splits = (logs.len() + max_size - 1) / max_size;
    let mut result = Vec::with_capacity(num_splits);
    let mut start = 0;
    for _ in 0..num_splits {
        let end = start + max_size;
        result.push(&logs[start..end.min(logs.len())]);
        start = end;
    }
    result
}
