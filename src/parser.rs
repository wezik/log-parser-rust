use crate::{NewFinishedLog, NewStartingLog};
use std::sync::mpsc;

mod task;

pub fn parse() {
    let (starting_sx, starting_rx) = mpsc::sync_channel::<NewStartingLog>(256000);
    let (finished_sx, finished_rx) = mpsc::sync_channel::<NewFinishedLog>(256000);

    let (sx, rx) = mpsc::sync_channel::<Vec<char>>(10);

    let reader = task::read(sx);
    let interpreter = task::interpret(rx, starting_sx, finished_sx);
    let starting_saver = task::save_starts(starting_rx);
    let finished_saver = task::save_finishes(finished_rx);

    reader.join().unwrap();
    interpreter.join().unwrap();
    starting_saver.join().unwrap();
    finished_saver.join().unwrap();
}
