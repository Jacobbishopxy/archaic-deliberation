//! Connector

use std::future::Future;
use std::str::FromStr;

use anyhow::{anyhow, Result};
use futures::stream::BoxStream;
use futures::{future::BoxFuture, TryStreamExt};
use sqlx::mssql::{MssqlPool, MssqlPoolOptions, MssqlRow};
use sqlx::mysql::{MySqlPool, MySqlPoolOptions, MySqlRow};
use sqlx::postgres::{PgPool, PgPoolOptions, PgRow};

pub enum DB {
    MsSql,
    MySql,
    Postgres,
}

impl FromStr for DB {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "mssql" => Ok(DB::MsSql),
            "mysql" => Ok(DB::MySql),
            "postgres" => Ok(DB::Postgres),
            _ => Err(anyhow!(
                "must be one of the following: mssql/mysql/postgres"
            )),
        }
    }
}

pub struct Castigate<T: SqlMeta> {
    conn_str: String,
    pool_options: Option<T>,
}

impl<T: SqlMeta> Castigate<T> {
    pub fn new<S: Into<String>>(conn_str: S) -> Self {
        Self {
            conn_str: conn_str.into(),
            pool_options: None,
        }
    }

    pub async fn connect(&mut self) -> Result<()> {
        match self.pool_options.as_ref() {
            None => {
                let p = T::new(&self.conn_str).await?;
                self.pool_options = Some(p);
                Ok(())
            }
            Some(_) => Err(anyhow!("Castigate has already been connected!")),
        }
    }

    pub async fn disconnect(&mut self) -> Result<()> {
        match self.pool_options.take() {
            None => Err(anyhow!("Castigate has not establish a connection yet!")),
            Some(p) => p.close().await,
        }
    }
}

pub trait SqlMeta: Sized {
    type FutS<'a>: Future<Output = Result<Self>>
    where
        Self: 'a;

    type Fut<'a>: Future<Output = Result<()>>
    where
        Self: 'a;

    fn new(conn_str: &str) -> Self::FutS<'_>;

    fn close(&self) -> Self::Fut<'_>;

    fn is_connected(&self) -> bool;
}

impl SqlMeta for MssqlPool {
    type FutS<'a> = impl Future<Output = Result<Self>>;

    type Fut<'a> = impl Future<Output = Result<()>>;

    fn new(conn_str: &str) -> Self::FutS<'_> {
        async move {
            let pg = MssqlPoolOptions::new().connect(conn_str).await?;
            Ok(pg)
        }
    }

    fn close(&self) -> Self::Fut<'_> {
        async move {
            MssqlPool::close(self).await;
            Ok(())
        }
    }

    fn is_connected(&self) -> bool {
        !self.is_closed()
    }
}

impl SqlMeta for MySqlPool {
    type FutS<'a> = impl Future<Output = Result<Self>>;

    type Fut<'a> = impl Future<Output = Result<()>>;

    fn new(conn_str: &str) -> Self::FutS<'_> {
        async move {
            let pg = MySqlPoolOptions::new().connect(conn_str).await?;
            Ok(pg)
        }
    }

    fn close(&self) -> Self::Fut<'_> {
        async move {
            MySqlPool::close(self).await;
            Ok(())
        }
    }

    fn is_connected(&self) -> bool {
        !self.is_closed()
    }
}

impl SqlMeta for PgPool {
    type FutS<'a> = impl Future<Output = Result<Self>>;

    type Fut<'a> = impl Future<Output = Result<()>>;

    fn new(conn_str: &str) -> Self::FutS<'_> {
        async move {
            let pg = PgPoolOptions::new().connect(conn_str).await?;
            Ok(pg)
        }
    }

    fn close(&self) -> Self::Fut<'_> {
        async move {
            PgPool::close(self).await;
            Ok(())
        }
    }

    fn is_connected(&self) -> bool {
        !self.is_closed()
    }
}

#[tokio::test]
async fn connection_success() {
    let url = "postgres://postgres:password@localhost/test";

    let pg = PgPoolOptions::new();
    let mut pool = pg.connect(url).await.expect("Connection success!");

    let mut stream = sqlx::query("SELECT * FROM users")
        .map(|row: PgRow| {
            // TODO
        })
        .fetch(&mut pool);

    while let Some(row) = stream.try_next().await.expect("Next success") {
        // TODO
    }

    let res = sqlx::query("SELECT * FROM users")
        .map(|row: PgRow| {
            // TODO
        })
        .fetch_all(&mut pool)
        .await;
}
