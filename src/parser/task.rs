use diesel::PgConnection;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read};
use std::sync::mpsc::{Receiver, Sender, SyncSender};
use std::thread::{spawn, JoinHandle};

use indicatif::{MultiProgress, ProgressBar};

use crate::parser::progress_styles;
use crate::{database, NewFinishedLog, NewStartingLog};

pub fn read(
    s_reader: SyncSender<Vec<char>>,
    s_tracker: Sender<Tracker>,
    mut reader: BufReader<File>,
) -> JoinHandle<()> {
    spawn(move || {
        let mut buffer = [0; 1];
        let mut log_buffer = Vec::new();

        let closing_bracket = b'}';

        let mut bytes_read = 0;
        let mut logs_found = 0;
        let tracker_delay = 4194304;

        loop {
            match reader.read_exact(&mut buffer) {
                Ok(()) => {
                    for b in buffer {
                        bytes_read += 1;
                        if b == closing_bracket {
                            logs_found += 1;
                            log_buffer.push(b as char);
                            s_reader
                                .send(log_buffer)
                                .expect("Failed to send read to interpreter");
                            log_buffer = vec![];
                        } else {
                            log_buffer.push(b as char);
                        }
                        if bytes_read >= tracker_delay {
                            s_tracker
                                .send(Tracker {
                                    bytes_read,
                                    logs_found,
                                    logs_saved: 0,
                                    logs_message: None,
                                })
                                .expect("Failed to send tracking data");
                            bytes_read = 0;
                            logs_found = 0;
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
        s_tracker
            .send(Tracker {
                bytes_read,
                logs_found,
                logs_saved: 0,
                logs_message: None,
            })
            .expect("Failed to send tracking data");
    })
}

pub fn interpret(
    r_reader: Receiver<Vec<char>>,
    s_start: SyncSender<NewStartingLog>,
    s_finish: SyncSender<NewFinishedLog>,
    s_tracker: Sender<Tracker>,
) -> JoinHandle<()> {
    spawn(move || {
        let mut variables = Vec::new();
        let mut var_buffer = String::new();

        let mut logs_parsed = 0;
        let tracker_delay = 32767;

        for chars in r_reader {
            logs_parsed += 1;
            for c in chars {
                match c {
                    c if c == '{' || c == '"' || c == ' ' => {}
                    c if c == '}' => {
                        variables.push(var_buffer);
                        var_buffer = String::new();

                        let map = map_from_variables(variables);
                        let state = get(&map, "state");

                        if logs_parsed >= tracker_delay {
                            s_tracker
                                .send(Tracker {
                                    bytes_read: 0,
                                    logs_found: 0,
                                    logs_saved: 0,
                                    logs_message: Some(format!(
                                        "parsing -> ID [{}]",
                                        get(&map, "id")
                                    )),
                                })
                                .expect("Failed to send tracking data");
                            logs_parsed = 0;
                        }

                        if state == "STARTED" {
                            send_variables(&s_start, map_to_start(map));
                        } else if state == "FINISHED" {
                            send_variables(&s_finish, map_to_finish(map));
                        }

                        variables = Vec::new();
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

        s_tracker
            .send(Tracker {
                bytes_read: 0,
                logs_found: 0,
                logs_saved: 0,
                logs_message: Some("parsing done".to_string()),
            })
            .expect("Failed to send tracking data");

        fn map_from_variables(variables: Vec<String>) -> HashMap<String, String> {
            let mut map = HashMap::new();
            for var in variables {
                let split: Vec<&str> = var.split(':').collect();
                map.insert(split[0].to_string(), split[1].to_string());
            }
            map
        }

        fn send_variables<T>(sender: &SyncSender<T>, log: T) {
            sender
                .send(log)
                .expect("Failed to send log from interpreter");
        }

        fn map_to_start(map: HashMap<String, String>) -> NewStartingLog {
            NewStartingLog {
                timestamp: str_to_i64(get(&map, "timestamp")),
                log_id: str_to_i64(get(&map, "id")),
            }
        }

        fn map_to_finish(map: HashMap<String, String>) -> NewFinishedLog {
            NewFinishedLog {
                timestamp: str_to_i64(get(&map, "timestamp")),
                log_id: str_to_i64(get(&map, "id")),
            }
        }

        fn str_to_i64(s: &str) -> i64 {
            s.parse()
                .unwrap_or_else(|_| panic!("Failed to convert {} to i64", s))
        }

        fn get<'a>(from: &'a HashMap<String, String>, s: &str) -> &'a str {
            from.get(s)
                .unwrap_or_else(|| panic!("Failed to get {} from map", s))
        }
    })
}

fn save_loop<T, U>(r_logs: Receiver<T>, s_tracker: Sender<Tracker>, save_logs: U) -> JoinHandle<()>
where
    T: Send + 'static,
    U: Fn(&Vec<T>, &mut PgConnection) + Send + Sync + 'static,
{
    spawn(move || {
        let mut connection = database::establish_connection();
        let batch_size = 65535;
        let mut batch = Vec::with_capacity(batch_size);
        for log in r_logs {
            batch.push(log);
            if batch.len() >= batch_size {
                save_logs(&batch, &mut connection);
                s_tracker
                    .send(Tracker {
                        bytes_read: 0,
                        logs_found: 0,
                        logs_saved: batch.len() as u64,
                        logs_message: None,
                    })
                    .expect("Failed to send tracking data");
                batch.clear();
            }
        }
        save_logs(&batch, &mut connection);
        s_tracker
            .send(Tracker {
                bytes_read: 0,
                logs_found: 0,
                logs_saved: batch.len() as u64,
                logs_message: None,
            })
            .expect("Failed to send tracking data");
        batch.clear();
    })
}

pub fn save_starts(r_logs: Receiver<NewStartingLog>, s_tracker: Sender<Tracker>) -> JoinHandle<()> {
    save_loop(r_logs, s_tracker, database::save_starting_logs)
}

pub fn save_finishes(
    r_logs: Receiver<NewFinishedLog>, s_tracker: Sender<Tracker>,
) -> JoinHandle<()> {
    save_loop(r_logs, s_tracker, database::save_finished_logs)
}

pub struct Tracker {
    bytes_read: u64,
    logs_found: u64,
    logs_saved: u64,
    logs_message: Option<String>,
}

pub fn track_progress(r_tracking: Receiver<Tracker>, file_size: u64) -> JoinHandle<()> {
    spawn(move || {
        let progress_tracker = MultiProgress::new();

        let pb_read = progress_tracker.add(ProgressBar::new(file_size));
        pb_read.set_style(progress_styles::get_pb_reader());

        let pb_interpreter = progress_tracker.insert_after(&pb_read, ProgressBar::new(0));
        pb_interpreter.set_style(progress_styles::get_pb_interpreter());

        let pb_saved = progress_tracker.insert_after(&pb_interpreter, ProgressBar::new(0));
        pb_saved.set_style(progress_styles::get_pb_style());

        for tracker in r_tracking {
            pb_read.inc(tracker.bytes_read);
            pb_interpreter.inc_length(tracker.logs_found);
            pb_saved.inc_length(tracker.logs_found);
            pb_saved.inc(tracker.logs_saved);
            if tracker.logs_message.is_some() {
                pb_interpreter.set_message(tracker.logs_message.unwrap());
            }
            pb_read.tick();
            pb_interpreter.tick();
            pb_saved.tick();
        }
    })
}
