//! Connector

use sqlx::mssql::MssqlPoolOptions;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::postgres::PgPoolOptions;

pub enum PoolOptions {
    Mssql(MssqlPoolOptions),
    Mysql(MySqlPoolOptions),
    Postgres(PgPoolOptions),
}
