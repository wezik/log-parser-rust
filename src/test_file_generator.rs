use std::fs::File;
use std::io::{BufWriter, Write};

use rand::seq::SliceRandom;

pub fn run(amount: u128) {
    let mut writer = BufWriter::new(File::create("test.txt").unwrap());
    generate_logs(amount, &mut writer);
    println!("flushing");
    writer.flush().unwrap();
    println!("done");
}

fn write_to_file(writer: &mut BufWriter<File>, logs: &mut Vec<String>) {
    println!("--- writing into file");
    let log_threshold_base = logs.len() / 5;
    let mut log_threshold = log_threshold_base;
    for i in 0..logs.len() {
        if i == log_threshold {
            println!("--- written {} out of {}", i, log_threshold_base * 5);
            log_threshold += log_threshold_base;
        }
        writer.write(logs.pop().unwrap().as_bytes()).unwrap();
    }
    println!("--- done");
}

fn generate_logs(amount: u128, writer: &mut BufWriter<File>) {
    let amount =  amount / 2;
    let started = "STARTED";
    let finished = "FINISHED";
    let flag_point = (amount / 10 * 7) as u128;
    let batch = 5000000;
    let mut log_threshold = batch;
    let mut logs = Vec::new();
    for i in 0..=amount {
        if i == log_threshold {
            println!("created {} out of {}", i * 2, amount * 2);
            log_threshold += batch;
            println!("-- shuffling");
            logs.shuffle(&mut rand::thread_rng());
            println!("-- done");
            write_to_file(writer, &mut logs);
        }
        let start = format!("{{\"id\":\"{}\", \"state\":\"{}\", \"type\":\"TEST\", \"host\":\"1234\", \"timestamp\":\"0\"}}", i, started);
        let finish = if i < flag_point {
            format!("{{\"id\":\"{}\", \"state\":\"{}\", \"type\":\"TEST\", \"host\":\"1234\", \"timestamp\":\"999\"}}", i, finished)
        } else {
            format!("{{\"id\":\"{}\", \"state\":\"{}\", \"type\":\"TEST\", \"host\":\"1234\", \"timestamp\":\"0\"}}", i, finished)
        };
        logs.push(start);
        logs.push(finish);
    }
    println!("done");
}
