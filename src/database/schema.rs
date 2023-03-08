// @generated automatically by Diesel CLI.

diesel::table! {
    finished_logs (id) {
        id -> Int8,
        timestamp -> Nullable<Int8>,
    }
}

diesel::table! {
    starting_logs (id) {
        id -> Int8,
        timestamp -> Nullable<Int8>,
    }
}

diesel::allow_tables_to_appear_in_same_query!(finished_logs, starting_logs,);
