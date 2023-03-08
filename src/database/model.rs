﻿// Generated by diesel_ext

#![allow(unused)]
#![allow(clippy::all)]

use diesel::Queryable;

#[derive(Queryable, Debug)]
pub struct FinishedLog {
    pub id: i64,
    pub timestamp: Option<i64>,
}

#[derive(Queryable, Debug)]
pub struct StartingLog {
    pub id: i64,
    pub timestamp: Option<i64>,
}
