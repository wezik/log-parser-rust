#[cfg(debug_assertions)]
mod test_file_generator;
mod database;

fn main() {
    #[cfg(debug_assertions)]
    {
        //20000000 records would produce ~3 GB file
        test_file_generator::run(20000000);
    }
    println!("Hello world");
}
