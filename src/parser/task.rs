use std::collections::VecDeque;
use std::fs::File;
use std::io::{BufReader, Read};
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender, SyncSender};
use std::thread::{spawn, JoinHandle};

use ::serde::Deserialize;
use diesel::PgConnection;
use indicatif::{MultiProgress, ProgressBar};

use crate::parser::progress_styles;
use crate::{database, NewFinishedLog, NewStartingLog};

pub fn read(
    s_reader: SyncSender<VecDeque<char>>,
    s_tracker: Sender<Tracker>,
    mut reader: BufReader<File>,
) -> JoinHandle<()> {
    spawn(move || {
        let mut buffer = [0; 8000];
        let mut log_buffer = VecDeque::new();

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
                            log_buffer.push_back(b as char);
                            s_reader
                                .send(log_buffer)
                                .expect("Failed to send read to interpreter");
                            log_buffer = VecDeque::new();
                        } else {
                            log_buffer.push_back(b as char);
                        }
                        if bytes_read >= tracker_delay {
                            s_tracker
                                .send(Tracker::BytesRead(Some(bytes_read)))
                                .expect("Failed to send tracking data");
                            s_tracker
                                .send(Tracker::LogsFound(logs_found))
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
            .send(Tracker::BytesRead(None))
            .expect("Failed to send tracking data");
        s_tracker
            .send(Tracker::LogsFound(logs_found))
            .expect("Failed to send tracking data");
    })
}

pub fn interpret(
    r_reader: Receiver<VecDeque<char>>,
    s_start: SyncSender<NewStartingLog>,
    s_finish: SyncSender<NewFinishedLog>,
    s_tracker: Sender<Tracker>,
) -> JoinHandle<()> {
    spawn(move || {
        #[derive(Deserialize)]
        struct Log {
            id: String,
            timestamp: String,
            state: String,
        }

        let mut logs_parsed = 0;
        let tracker_delay = 32767;

        for chars in r_reader {
            logs_parsed += 1;
            let json_str = chars.iter().collect();
            let log = string_to_log(json_str);

            if logs_parsed >= tracker_delay {
                s_tracker
                    .send(Tracker::LogsMessage(Some(format!(
                        "parsing -> ID [{}]",
                        log.id
                    ))))
                    .expect("Failed to send tracking data");
                s_tracker
                    .send(Tracker::LogsParsed(logs_parsed))
                    .expect("Failed to send tracking data");
                logs_parsed = 0;
            }

            if log.state == "STARTED" {
                send_variables(&s_start, log_to_start(log));
            } else if log.state == "FINISHED" {
                send_variables(&s_finish, log_to_finish(log));
            }
        }

        s_tracker
            .send(Tracker::LogsMessage(None))
            .expect("Failed to send tracking data");

        fn string_to_log(s: String) -> Log {
            serde_json::from_str::<Log>(&s).expect("Failed to map json to string")
        }

        fn send_variables<T>(sender: &SyncSender<T>, log: T) {
            sender
                .send(log)
                .expect("Failed to send log from interpreter");
        }

        fn log_to_start(log: Log) -> NewStartingLog {
            NewStartingLog {
                log_id: str_to_i64(log.id),
                timestamp: str_to_i64(log.timestamp),
            }
        }

        fn log_to_finish(log: Log) -> NewFinishedLog {
            NewFinishedLog {
                log_id: str_to_i64(log.id),
                timestamp: str_to_i64(log.timestamp),
            }
        }

        fn str_to_i64(s: String) -> i64 {
            s.parse()
                .unwrap_or_else(|_| panic!("Failed to convert {} to i64", s))
        }
    })
}

fn spawn_saving_task<T, U>(
    r_logs: Receiver<Vec<T>>,
    s_tracker: Sender<Tracker>,
    save_logs: U,
) -> JoinHandle<()>
where
    T: Send + 'static,
    U: Fn(&Vec<T>, &mut PgConnection) + Send + Sync + 'static,
{
    spawn(move || {
        let mut connection = database::establish_connection();
        for batch in r_logs {
            save_logs(&batch, &mut connection);
            s_tracker
                .send(Tracker::LogsSaved(batch.len() as u64))
                .expect("Failed to send tracking data");
        }
    })
}

fn run_save_tasks<T, U>(
    r_logs: Receiver<T>,
    s_tracker: Sender<Tracker>,
    save_logs: U,
) -> JoinHandle<()>
where
    T: Send + 'static,
    U: Fn(&Vec<T>, &mut PgConnection) + Send + Sync + 'static + Clone,
{
    let (sender_s1, receiver_s1) = mpsc::channel::<Vec<T>>();
    let (sender_s2, receiver_s2) = mpsc::channel::<Vec<T>>();

    spawn_saving_task(receiver_s1, s_tracker.clone(), save_logs.clone());
    spawn_saving_task(receiver_s2, s_tracker.clone(), save_logs.clone());

    spawn(move || {
        let mut i = 0;
        let channels = vec![sender_s1, sender_s2];

        fn get_channel<T>(i: usize, channels: &[Sender<Vec<T>>]) -> &Sender<Vec<T>> {
            channels
                .get(i)
                .expect("Failed to retrieve saving task from pool")
        }

        let batch_size = 65535;
        let mut batch = Vec::with_capacity(batch_size);
        for log in r_logs {
            batch.push(log);
            if batch.len() >= batch_size {
                if i >= channels.len() {
                    i = 0;
                }
                get_channel(i, &channels)
                    .send(batch)
                    .expect("Failed sending saving channels");
                i += 1;
                batch = Vec::with_capacity(batch_size);
            }
        }
        if i >= channels.len() {
            i = 0;
        }
        if !batch.is_empty() {
            get_channel(i, &channels)
                .send(batch)
                .expect("Failed sending saving channels");
        }
    })
}

pub fn save_starts(r_logs: Receiver<NewStartingLog>, s_tracker: Sender<Tracker>) -> JoinHandle<()> {
    run_save_tasks(r_logs, s_tracker, database::save_starting_logs)
}

pub fn save_finishes(
    r_logs: Receiver<NewFinishedLog>,
    s_tracker: Sender<Tracker>,
) -> JoinHandle<()> {
    run_save_tasks(r_logs, s_tracker, database::save_finished_logs)
}

pub enum Tracker {
    LogsMessage(Option<String>),
    BytesRead(Option<u64>),
    LogsParsed(u64),
    LogsSaved(u64),
    LogsFound(u64),
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
            match tracker {
                Tracker::LogsMessage(val) => match val {
                    Some(v) => {
                        pb_interpreter.set_message(v);
                    }
                    None => {
                        pb_interpreter.finish_with_message("parsing done");
                    }
                },
                Tracker::BytesRead(val) => match val {
                    Some(v) => {
                        pb_read.inc(v);
                    }
                    None => {
                        pb_read.finish();
                    }
                },
                Tracker::LogsSaved(val) => {
                    pb_saved.inc(val);
                }
                Tracker::LogsFound(val) => {
                    pb_interpreter.inc_length(val);
                }
                Tracker::LogsParsed(val) => {
                    pb_saved.inc_length(val);
                }
            }
        }
    })
}
