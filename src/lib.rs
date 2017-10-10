#![feature(conservative_impl_trait)]

extern crate futures;
extern crate tokio_core;
extern crate tokio_postgres;

use std::error;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use futures::task::{self, Task};
use futures::{Async, Future, Poll};
use tokio_core::reactor::Handle;
use tokio_postgres::error::Error;
use tokio_postgres::{Connection, TlsMode};
use tokio_postgres::params::{ConnectParams, IntoConnectParams};

pub struct InnerPool {
    params: ConnectParams,
    queue: VecDeque<Task>,
    conns: VecDeque<Connection>,
    size: usize,
}

#[derive(Clone)]
pub struct Pool {
    handle: Handle,
    inner: Arc<Mutex<InnerPool>>,
}

#[derive(Clone)]
pub struct PoolRemote(Arc<Mutex<InnerPool>>);

enum Conn {
    Now(Connection),
    Future(Box<Future<Item = Connection, Error = Error>>),
    None,
}

impl Pool {
    pub fn new<T>(params: T, handle: Handle, size: usize) -> Result<Self, Box<error::Error + 'static + Send + Sync>>
    where
        T: IntoConnectParams,
    {
        let params = params.into_connect_params()?;

        Ok(Pool {
            handle: handle,
            inner: Arc::new(Mutex::new(InnerPool {
                params: params,
                queue: VecDeque::new(),
                conns: VecDeque::new(),
                size: size,
            })),
        })
    }

    fn acquire(&self) -> Conn {
        let mut inner = self.inner.lock().unwrap();
        if inner.size == 0 {
            return Conn::None;
        }

        inner.size -= 1;

        match inner.conns.pop_front() {
            Some(conn) => Conn::Now(conn),
            None => {
                let conn = Connection::connect(inner.params.clone(), TlsMode::None, &self.handle);
                Conn::Future(conn)
            }
        }
    }

    fn release(&self, conn: Connection) {
        let mut inner = self.inner.lock().unwrap();
        inner.conns.push_back(conn);
        inner.size += 1;

        if let Some(task) = inner.queue.pop_front() {
            task.notify();
        }
    }

    fn connection_lost(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.size += 1;
    }

    fn enqueue(&self, task: Task) {
        let mut inner = self.inner.lock().unwrap();
        inner.queue.push_back(task);
    }

    pub fn with_connection<F, R, I, E>(&self, f: F) -> impl Future<Item = I, Error = E>
    where
        F: FnOnce(Connection) -> R,
        R: Future<Item = (I, Connection), Error = (E, Option<Connection>)>,
        E: From<Error>,
    {
        let pool1 = self.clone();
        let pool2 = self.clone();
        let conn = FutureConnection {
            pool: self.clone(),
            conn: None,
        };
        let fut = conn.map_err(move |err| {
            let mut inner = pool1.inner.lock().unwrap();
            inner.size += 1;

            E::from(err)
        }).and_then(move |conn| {
                f(conn).then(move |res| match res {
                    Ok((result, conn)) => {
                        pool2.release(conn);
                        Ok(result)
                    }
                    Err((err, Some(conn))) => {
                        pool2.release(conn);
                        Err(err)
                    }
                    Err((err, None)) => {
                        pool2.connection_lost();
                        Err(err)
                    }
                })
            });
        fut
    }

    pub fn remote(&self) -> PoolRemote {
        PoolRemote(self.inner.clone())
    }
}

impl PoolRemote {
    pub fn into_pool(self, handle: Handle) -> Pool {
        Pool {
            handle: handle,
            inner: self.0,
        }
    }
}

struct FutureConnection {
    pool: Pool,
    conn: Option<Box<Future<Item = Connection, Error = Error>>>,
}

impl Future for FutureConnection {
    type Item = Connection;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if let Some(ref mut conn) = self.conn {
            return conn.poll();
        }

        match self.pool.acquire() {
            Conn::Now(conn) => Ok(Async::Ready(conn)),
            Conn::Future(mut conn) => {
                let result = conn.poll();
                if let Ok(Async::NotReady) = result {
                    self.conn = Some(conn);
                }
                result
            }
            Conn::None => {
                self.pool.enqueue(task::current());
                Ok(Async::NotReady)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use futures::Future;
    use tokio_core::reactor::Core;
    use {Conn, FutureConnection, Pool};

    #[test]
    fn acquire() {
        let mut core = Core::new().expect("unable to initialize the main event loop");
        let connection_string = "postgresql://brillen@localhost/brillen2";
        let pool = Pool::new(connection_string, core.handle(), 20).unwrap();
        let conn = pool.acquire();
        match conn {
            Conn::Now(_) => unreachable!(),
            Conn::Future(conn) => {
                core.run(conn).expect("error running the event loop");
                assert!(true);
            }
            Conn::None => unreachable!(),
        }
    }

    #[test]
    fn future_conn() {
        let mut core = Core::new().expect("unable to initialize the main event loop");
        let connection_string = "postgresql://brillen@localhost/brillen2";
        let pool = Pool::new(connection_string, core.handle(), 20).unwrap();

        let conn = FutureConnection {
            pool: pool.clone(),
            conn: None,
        };
        let called = RefCell::new(false);
        let srv = conn.map(|_| {
            let mut called = called.borrow_mut();
            *called = true;
        }).map_err(|_| unreachable!());
        core.run(srv).expect("error running the event loop");
        assert_eq!(true, *called.borrow());
    }
}
