use std::fmt::Write;
use std::ops::{Div, Rem};

use indicatif::{ProgressState, ProgressStyle};

pub fn get_pb_reader() -> ProgressStyle {
    ProgressStyle::with_template(
        "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes}",
    )
    .unwrap()
    .progress_chars("#>-")
}

pub fn get_pb_interpreter() -> ProgressStyle {
    ProgressStyle::with_template("{spinner:.green} Total logs: {total} {msg}")
        .unwrap()
        .with_key("total", |state: &ProgressState, w: &mut dyn Write| {
            let len = state.len().unwrap();
            fn shorten(n: u64) -> String {
                if n > 1000000 {
                    return format!("{}.{} M", n.div(1000000), n.div(100000).rem(10));
                }
                n.to_string()
            }
            write!(w, "{}", shorten(len)).unwrap();
        })
        .progress_chars("#>-")
}

pub fn get_pb_style() -> ProgressStyle {
    ProgressStyle::with_template(
        "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {total}",
    )
    .unwrap()
    .with_key("total", |state: &ProgressState, w: &mut dyn Write| {
        let pos = state.pos();
        let len = state.len().unwrap();
        fn shorten(n: u64) -> String {
            if n > 1000000 {
                return format!("{}.{} M", n.div(1000000), n.div(100000).rem(10));
            }
            n.to_string()
        }
        write!(
            w,
            "{}/{} ({:.1}s)",
            shorten(pos),
            shorten(len),
            state.eta().as_secs_f64()
        )
        .unwrap();
    })
    .progress_chars("#>-")
}
