use diesel::{Connection, PgConnection, RunQueryDsl};

use schema::finished_logs;
use schema::starting_logs;

use crate::database::model::{NewFinishedLog, NewStartingLog};

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
    let split_logs = split_logs(logs, 2);
    for split in split_logs {
        diesel::insert_into(starting_logs::table)
            .values(split)
            .execute(connection)
            .expect("Error saving log");
    }
}

pub fn save_finished_logs(logs: &Vec<NewFinishedLog>, connection: &mut PgConnection) {
    let split_logs = split_logs(logs, 2);
    for split in split_logs {
        diesel::insert_into(finished_logs::table)
            .values(split)
            .execute(connection)
            .expect("Error saving log");
    }
}

fn split_logs<T: Clone>(logs: &Vec<T>, parameters: usize) -> Vec<&[T]> {
    // turns out diesel isn't capped on n of elements passed but n of parameters of these elements
    let max_size = 65535 / parameters;
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
