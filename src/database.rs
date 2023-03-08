use diesel::{Connection, PgConnection};

pub mod model;
pub mod schema;

pub fn establish_connection() -> PgConnection {
    let database_url = &dotenv::var("DATABASE_URL").expect("DATABASE_URL must be set");
    match PgConnection::establish(database_url) {
        Ok(t) => t,
        Err(e) => panic!("Failed to connect to database {}", e),
    }
}
