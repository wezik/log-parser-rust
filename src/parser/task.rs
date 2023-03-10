use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read};
use std::sync::mpsc::{Receiver, Sender, SyncSender};
use std::thread::{spawn, JoinHandle};

use crate::{database, NewFinishedLog, NewStartingLog};

fn create_file_reader(path: &str) -> BufReader<File> {
    let file = File::open(path).expect("Failed to open file");
    BufReader::new(file)
}

pub fn read(output: SyncSender<Vec<char>>) -> JoinHandle<()> {
    spawn(move || {
        let mut reader = create_file_reader("test.txt");

        let mut buffer = [0; 1];
        let mut log_buffer = Vec::new();

        let closing_bracket = b'}';

        loop {
            match reader.read_exact(&mut buffer) {
                Ok(()) => {
                    for b in buffer {
                        if b == closing_bracket {
                            log_buffer.push(b as char);
                            output
                                .send(log_buffer)
                                .expect("Failed to send read to interpreter");
                            log_buffer = vec![];
                        } else {
                            log_buffer.push(b as char);
                        }
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    break;
                }
                Err(e) => {
                    panic!("Failed to read file: {}", e);
                }
            }
        }
    })
}

pub fn interpret(
    output: Receiver<Vec<char>>,
    output_start: SyncSender<NewStartingLog>,
    output_finish: SyncSender<NewFinishedLog>,
) -> JoinHandle<()> {
    spawn(move || {
        let mut variables = Vec::new();
        let mut var_buffer = String::new();

        for log in output {
            for c in log {
                match c {
                    c if c == '{' || c == '"' || c == ' ' => {}
                    c if c == '}' => {
                        variables.push(var_buffer);
                        var_buffer = String::new();
                        send_variables(&variables, &output_start, &output_finish);
                        variables.clear();
                    }
                    c if c == ',' => {
                        variables.push(var_buffer);
                        var_buffer = String::new();
                    }
                    _ => {
                        var_buffer.push(c);
                    }
                }
            }
        }
        fn send_variables(
            vars: &Vec<String>,
            starting_sx: &SyncSender<NewStartingLog>,
            finished_sx: &SyncSender<NewFinishedLog>,
        ) {
            let mut map = HashMap::new();
            for var in vars {
                let split: Vec<&str> = var.split(':').collect();
                map.insert(split[0], split[1]);
            }
            let state = map.get("state").expect("Failed to map logs");
            if state == &"STARTED" {
                let id = map.get("id").expect("Failed to map logs");
                let timestamp = map.get("timestamp").expect("Failed to map logs");
                starting_sx
                    .send(NewStartingLog {
                        log_id: map_to_i64(id),
                        timestamp: map_to_i64(timestamp),
                    })
                    .expect("Failed to send starting logs");
            } else if state == &"FINISHED" {
                let id = map.get("id").expect("Failed to map logs");
                let timestamp = map.get("timestamp").expect("Failed to map logs");
                finished_sx
                    .send(NewFinishedLog {
                        log_id: map_to_i64(id),
                        timestamp: map_to_i64(timestamp),
                    })
                    .expect("Failed to send finished logs");
            } else {
                panic!("Logs contained incorrect states");
            }
        }

        fn map_to_i64(s: &str) -> i64 {
            s.parse()
                .expect("Failed to map interpret string as a number")
        }
    })
}

pub fn save_starts(input: Receiver<NewStartingLog>) -> JoinHandle<()> {
    spawn(move || {
        let mut connection = database::establish_connection();
        let batch_size = 65534;
        let mut batch = Vec::with_capacity(batch_size);
        for log in input {
            batch.push(log);
            if batch.len() >= batch_size {
                database::save_starting_logs(&batch, &mut connection);
                batch.clear();
            }
        }
        database::save_starting_logs(&batch, &mut connection);
    })
}

pub fn save_finishes(input: Receiver<NewFinishedLog>) -> JoinHandle<()> {
    spawn(move || {
        let mut connection = database::establish_connection();
        let batch_size = 65534;
        let mut batch = Vec::with_capacity(batch_size);
        for log in input {
            batch.push(log);
            if batch.len() >= batch_size {
                database::save_finished_logs(&batch, &mut connection);
                batch.clear();
            }
        }
        database::save_finished_logs(&batch, &mut connection);
    })
}
