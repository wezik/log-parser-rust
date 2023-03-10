use crate::database::model::{NewFinishedLog, NewStartingLog};

mod database;
mod parser;
#[cfg(debug_assertions)]
mod test_file_generator;

fn main() {
    #[cfg(debug_assertions)]
    {
        //20000000 records would produce ~3 GB file
        test_file_generator::run(20000000);
    }

    parser::parse("test_m.txt");

    println!("Hello world");
}
