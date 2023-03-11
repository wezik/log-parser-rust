use std::collections::VecDeque;
use std::fs::File;
use std::io::BufReader;
use std::sync::mpsc;
use std::time::SystemTime;

use crate::parser::task::Tracker;
use crate::{NewFinishedLog, NewStartingLog};

mod progress_styles;
mod task;

pub fn parse(path: &str) {
    let (s_tracker, r_tracker) = mpsc::channel::<Tracker>();
    let (s_reader, r_reader) = mpsc::sync_channel::<VecDeque<char>>(2048000);
    let (s_start_logs, r_start_logs) = mpsc::sync_channel::<NewStartingLog>(512000);
    let (s_finish_logs, r_finish_logs) = mpsc::sync_channel::<NewFinishedLog>(512000);

    let file = File::open(path).expect("Failed to open file");
    let file_size = file
        .metadata()
        .expect("Failed to get metadata for file")
        .len();
    let buf_reader = BufReader::new(file);

    let start = SystemTime::now();

    let tracker = task::track_progress(r_tracker, file_size);
    let reader = task::read(s_reader, s_tracker.clone(), buf_reader);
    let interpreter = task::interpret(r_reader, s_start_logs, s_finish_logs, s_tracker.clone());
    let start_saver = task::save_starts(r_start_logs, s_tracker.clone());
    let finish_saver = task::save_finishes(r_finish_logs, s_tracker);

    tracker.join().unwrap();
    reader.join().unwrap();
    interpreter.join().unwrap();
    start_saver.join().unwrap();
    finish_saver.join().unwrap();

    let end = SystemTime::now();
    let duration = end.duration_since(start).unwrap();

    println!(
        "Recorded time: {},{:09}",
        duration.as_secs(),
        duration.subsec_nanos()
    );
}
