//! Connector

use std::future::Future;
use std::str::FromStr;

use anyhow::{anyhow, Result};
use futures::future::BoxFuture;
use sqlx::mssql::{MssqlPool, MssqlPoolOptions, MssqlRow};
use sqlx::mysql::{MySqlPool, MySqlPoolOptions, MySqlRow};
use sqlx::postgres::{PgPool, PgPoolOptions, PgRow};
use sqlx::{FromRow, Row};

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

//
pub struct Castigate<T: SqlMeta> {
    conn_str: String,
    pool_options: Option<T>,
}

const CONN_E_ERR: &str = "Castigate has already been connected!";
const CONN_N_ERR: &str = "Castigate has not establish a connection yet!";

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
            Some(_) => Err(anyhow!(CONN_E_ERR)),
        }
    }

    pub async fn disconnect(&mut self) -> Result<()> {
        match self.pool_options.take() {
            None => Err(anyhow!(CONN_N_ERR)),
            Some(p) => p.close().await,
        }
    }

    pub async fn query<'a, D: Send + Unpin + 'a>(
        &'a self,
        sql: &'a str,
        pipe: PipeFn<T::RowType, D>,
    ) -> Result<Vec<D>> {
        match self.pool_options.as_ref() {
            Some(p) => p.query(sql, pipe).await,
            None => Err(anyhow!(CONN_N_ERR)),
        }
    }

    pub async fn query_one<'a, D: Send + Unpin + 'a>(
        &'a self,
        sql: &'a str,
        pipe: PipeFn<T::RowType, D>,
    ) -> Result<D> {
        match self.pool_options.as_ref() {
            Some(p) => p.query_one(sql, pipe).await,
            None => Err(anyhow!(CONN_N_ERR)),
        }
    }

    pub async fn query_as<'a, D: Send + Unpin + for<'r> FromRow<'r, T::RowType>>(
        &'a self,
        sql: &'a str,
    ) -> Result<Vec<D>> {
        match self.pool_options.as_ref() {
            Some(p) => T::query_as(p, sql).await,
            None => Err(anyhow!(CONN_N_ERR)),
        }
    }

    pub async fn query_as_one<'a, D: Send + Unpin + for<'r> FromRow<'r, T::RowType>>(
        &'a self,
        sql: &'a str,
    ) -> Result<D> {
        match self.pool_options.as_ref() {
            Some(p) => T::query_one_as(p, sql).await,
            None => Err(anyhow!(CONN_N_ERR)),
        }
    }
}

pub type PipeFn<I, O> = fn(I) -> Result<O>;

pub trait SqlMeta: Sized {
    type FutSelf<'a>: Future<Output = Result<Self>>
    where
        Self: 'a;

    type FutNil<'a>: Future<Output = Result<()>>
    where
        Self: 'a;

    type RowType: Row;

    fn new(conn_str: &str) -> Self::FutSelf<'_>;

    fn close(&self) -> Self::FutNil<'_>;

    fn is_closed(&self) -> bool;

    fn query<'a, T: Send + Unpin + 'a>(
        &'a self,
        sql: &'a str,
        pipe: PipeFn<Self::RowType, T>,
    ) -> BoxFuture<'a, Result<Vec<T>>>;

    fn query_one<'a, T: Send + Unpin + 'a>(
        &'a self,
        sql: &'a str,
        pipe: PipeFn<Self::RowType, T>,
    ) -> BoxFuture<'a, Result<T>>;

    fn query_as<'a, T: Send + Unpin + for<'r> FromRow<'r, Self::RowType>>(
        &'a self,
        sql: &'a str,
    ) -> BoxFuture<'a, Result<Vec<T>>>;

    fn query_one_as<'a, T: Send + Unpin + for<'r> FromRow<'r, Self::RowType>>(
        &'a self,
        sql: &'a str,
    ) -> BoxFuture<'a, Result<T>>;
}

impl SqlMeta for MssqlPool {
    type FutSelf<'a> = impl Future<Output = Result<Self>>;

    type FutNil<'a> = impl Future<Output = Result<()>>;

    type RowType = MssqlRow;

    fn new(conn_str: &str) -> Self::FutSelf<'_> {
        async move {
            let pg = MssqlPoolOptions::new().connect(conn_str).await?;
            Ok(pg)
        }
    }

    fn close(&self) -> Self::FutNil<'_> {
        async move {
            MssqlPool::close(self).await;
            Ok(())
        }
    }

    fn is_closed(&self) -> bool {
        MssqlPool::is_closed(self)
    }

    fn query<'a, T: Send + Unpin + 'a>(
        &'a self,
        sql: &'a str,
        pipe: PipeFn<Self::RowType, T>,
    ) -> BoxFuture<'a, Result<Vec<T>>> {
        let q = async move {
            sqlx::query(sql)
                .try_map(|r| Ok(pipe(r).map_err(|e| anyhow!(e))))
                .fetch_all(self)
                .await
                .map_err(|e| anyhow!(e))
                .and_then(|r| r.into_iter().collect::<Result<Vec<T>>>())
        };
        Box::pin(q)
    }

    fn query_one<'a, T: Send + Unpin + 'a>(
        &'a self,
        sql: &'a str,
        pipe: PipeFn<Self::RowType, T>,
    ) -> BoxFuture<'a, Result<T>> {
        let q = async move {
            sqlx::query(sql)
                .try_map(|r| Ok(pipe(r).map_err(|e| anyhow!(e))))
                .fetch_one(self)
                .await
                .map_err(|e| anyhow!(e))
                .and_then(|r| r)
        };
        Box::pin(q)
    }

    fn query_as<'a, T: Send + Unpin + for<'r> FromRow<'r, Self::RowType>>(
        &'a self,
        sql: &'a str,
    ) -> BoxFuture<'a, Result<Vec<T>>> {
        let q = async move {
            sqlx::query_as::<_, T>(sql)
                .fetch_all(self)
                .await
                .map_err(|e| anyhow!(e))
        };
        Box::pin(q)
    }

    fn query_one_as<'a, T: Send + Unpin + for<'r> FromRow<'r, Self::RowType>>(
        &'a self,
        sql: &'a str,
    ) -> BoxFuture<'a, Result<T>> {
        let q = async move {
            sqlx::query_as::<_, T>(sql)
                .fetch_one(self)
                .await
                .map_err(|e| anyhow!(e))
        };
        Box::pin(q)
    }
}

impl SqlMeta for MySqlPool {
    type FutSelf<'a> = impl Future<Output = Result<Self>>;

    type FutNil<'a> = impl Future<Output = Result<()>>;

    type RowType = MySqlRow;

    fn new(conn_str: &str) -> Self::FutSelf<'_> {
        async move {
            let pg = MySqlPoolOptions::new().connect(conn_str).await?;
            Ok(pg)
        }
    }

    fn close(&self) -> Self::FutNil<'_> {
        async move {
            MySqlPool::close(self).await;
            Ok(())
        }
    }

    fn is_closed(&self) -> bool {
        MySqlPool::is_closed(self)
    }

    fn query<'a, T: Send + Unpin + 'a>(
        &'a self,
        sql: &'a str,
        pipe: PipeFn<Self::RowType, T>,
    ) -> BoxFuture<'a, Result<Vec<T>>> {
        let q = async move {
            sqlx::query(sql)
                .try_map(|r| Ok(pipe(r).map_err(|e| anyhow!(e))))
                .fetch_all(self)
                .await
                .map_err(|e| anyhow!(e))
                .and_then(|r| r.into_iter().collect::<Result<Vec<T>>>())
        };
        Box::pin(q)
    }

    fn query_one<'a, T: Send + Unpin + 'a>(
        &'a self,
        sql: &'a str,
        pipe: PipeFn<Self::RowType, T>,
    ) -> BoxFuture<'a, Result<T>> {
        let q = async move {
            sqlx::query(sql)
                .try_map(|r| Ok(pipe(r).map_err(|e| anyhow!(e))))
                .fetch_one(self)
                .await
                .map_err(|e| anyhow!(e))
                .and_then(|r| r)
        };
        Box::pin(q)
    }

    fn query_as<'a, T: Send + Unpin + for<'r> FromRow<'r, Self::RowType>>(
        &'a self,
        sql: &'a str,
    ) -> BoxFuture<'a, Result<Vec<T>>> {
        let q = async move {
            sqlx::query_as::<_, T>(sql)
                .fetch_all(self)
                .await
                .map_err(|e| anyhow!(e))
        };
        Box::pin(q)
    }

    fn query_one_as<'a, T: Send + Unpin + for<'r> FromRow<'r, Self::RowType>>(
        &'a self,
        sql: &'a str,
    ) -> BoxFuture<'a, Result<T>> {
        let q = async move {
            sqlx::query_as::<_, T>(sql)
                .fetch_one(self)
                .await
                .map_err(|e| anyhow!(e))
        };
        Box::pin(q)
    }
}

impl SqlMeta for PgPool {
    type FutSelf<'a> = impl Future<Output = Result<Self>>;

    type FutNil<'a> = impl Future<Output = Result<()>>;

    type RowType = PgRow;

    fn new(conn_str: &str) -> Self::FutSelf<'_> {
        async move {
            let pg = PgPoolOptions::new().connect(conn_str).await?;
            Ok(pg)
        }
    }

    fn close(&self) -> Self::FutNil<'_> {
        async move {
            PgPool::close(self).await;
            Ok(())
        }
    }

    fn is_closed(&self) -> bool {
        PgPool::is_closed(self)
    }

    fn query<'a, T: Send + Unpin + 'a>(
        &'a self,
        sql: &'a str,
        pipe: PipeFn<Self::RowType, T>,
    ) -> BoxFuture<'a, Result<Vec<T>>> {
        let q = async move {
            sqlx::query(sql)
                .try_map(|r| Ok(pipe(r).map_err(|e| anyhow!(e))))
                .fetch_all(self)
                .await
                .map_err(|e| anyhow!(e))
                .and_then(|r| r.into_iter().collect::<Result<Vec<T>>>())
        };
        Box::pin(q)
    }

    fn query_one<'a, T: Send + Unpin + 'a>(
        &'a self,
        sql: &'a str,
        pipe: PipeFn<Self::RowType, T>,
    ) -> BoxFuture<'a, Result<T>> {
        let q = async move {
            sqlx::query(sql)
                .try_map(|r| Ok(pipe(r).map_err(|e| anyhow!(e))))
                .fetch_one(self)
                .await
                .map_err(|e| anyhow!(e))
                .and_then(|r| r)
        };
        Box::pin(q)
    }

    fn query_as<'a, T: Send + Unpin + for<'r> FromRow<'r, Self::RowType>>(
        &'a self,
        sql: &'a str,
    ) -> BoxFuture<'a, Result<Vec<T>>> {
        let q = async move {
            sqlx::query_as::<_, T>(sql)
                .fetch_all(self)
                .await
                .map_err(|e| anyhow!(e))
        };
        Box::pin(q)
    }

    fn query_one_as<'a, T: Send + Unpin + for<'r> FromRow<'r, Self::RowType>>(
        &'a self,
        sql: &'a str,
    ) -> BoxFuture<'a, Result<T>> {
        let q = async move {
            sqlx::query_as::<_, T>(sql)
                .fetch_one(self)
                .await
                .map_err(|e| anyhow!(e))
        };
        Box::pin(q)
    }
}

#[cfg(test)]
mod test_connector {
    use sqlx::Row;

    use super::*;

    const URL: &str = "postgres://root:secret@localhost:5432/dev";

    #[tokio::test]
    async fn query_success() {
        #[allow(dead_code)]
        #[derive(Debug)]
        struct User {
            email: String,
            nickname: String,
            hash: String,
            role: String,
        }

        impl User {
            fn new(email: String, nickname: String, hash: String, role: String) -> Self {
                User {
                    email,
                    nickname,
                    hash,
                    role,
                }
            }

            fn from_pg_row(row: PgRow) -> Result<Self> {
                let email: String = row.try_get(0)?;
                let nickname: String = row.try_get(1)?;
                let hash: String = row.try_get(2)?;
                let role: String = row.try_get(3)?;

                Ok(Self::new(email, nickname, hash, role))
            }
        }

        let mut ct = Castigate::<PgPool>::new(URL);

        ct.connect().await.expect("Connection success");

        let sql = "SELECT * FROM users";

        let res = ct.query(sql, User::from_pg_row).await;

        println!("{:?}", res);

        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn query_as_success() {
        #[allow(dead_code)]
        #[derive(sqlx::FromRow, Debug)]
        struct Users {
            email: String,
            nickname: String,
            hash: String,
            role: String,
        }

        let mut ct = Castigate::<PgPool>::new(URL);
        ct.connect().await.expect("Connection success");

        let sql = "SELECT * FROM users";

        let res = ct.query_as::<Users>(sql).await;

        println!("{:?}", res);

        assert!(res.is_ok());
    }
}
