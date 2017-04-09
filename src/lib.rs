extern crate tokio_postgres;
extern crate tokio_core;
extern crate futures;

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, LockResult, MutexGuard};
use futures::task::{Task, park};
use futures::{Future, Poll, Async, BoxFuture};
use tokio_core::reactor::{Remote};
use tokio_postgres::error::ConnectError;
use tokio_postgres::{Connection, TlsMode};

#[derive(Debug)]
pub enum Error {
    Todo,
}

pub struct InnerPool {
    remote: Remote,
    queue: VecDeque<Task>,
    conns: VecDeque<Connection>,
    size: i32,
}

pub struct Pool(Arc<Mutex<InnerPool>>);

enum Conn {
    Now(Connection),
    Future(BoxFuture<Connection, ConnectError>),
    None
}

impl Pool {
    pub fn new(remote: Remote, size: i32) -> Self {
        Pool(Arc::new(Mutex::new(InnerPool{
            remote: remote,
            queue: VecDeque::new(),
            conns: VecDeque::new(),
            size: size,
        })))
    }

    #[inline]
    fn lock(&self) -> LockResult<MutexGuard<InnerPool>> {
        self.0.lock()
    }

    fn acquire(&self) -> Conn {
        let mut inner = self.lock().unwrap();
        if inner.size == 0 {
            return Conn::None;
        }

        inner.size -= 1;

        match inner.conns.pop_front() {
            Some(conn) => Conn::Now(conn),
            None => {
                // TODO: no unwrap
                let conn = Connection::connect("postgresql://brillen@localhost/brillen2",
                                   TlsMode::None,
                                   &inner.remote.handle().unwrap());

                Conn::Future(conn)
            }
        }
    }

    fn release(&self, conn: Connection) {
        let mut inner = self.lock().unwrap();
        inner.conns.push_back(conn);
        inner.size += 1;

        if let Some(task) = inner.queue.pop_front() {
            task.unpark();
        }
    }

    fn connection_lost(&self) {
        let mut inner = self.lock().unwrap();
        inner.size += 1;
    }

    fn enqueue(&self, task: Task) {
        let mut inner = self.lock().unwrap();
        inner.queue.push_back(task);
    }

    pub fn with_connection<F, R, I, E>(&self, f: F) -> Box<Future<Item=I, Error=E> + Send + 'static>
        where F: FnOnce(Connection) -> R + Send + 'static,
              R: Future<Item = (I, Connection), Error = (E, Option<Connection>)> + Send + 'static,
              I: Send + 'static,
              E: From<Error> + Send + 'static
    {
        let pool1 = self.clone();
        let pool2 = self.clone();
        let conn = FutureConnection{
            pool: self.clone(),
            conn: None,
        };
        conn
            .map_err(move |err| {
                let mut inner = pool1.lock().unwrap();
                inner.size += 1;
                println!("Postgres Error: {:?}", err);
                E::from(Error::Todo)
            })
            .and_then(move |conn| {
                f(conn).then(move |res| {
                    match res {
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
                    }
                })
            })
        .boxed()
    }
}

impl Clone for Pool {
    fn clone(&self) -> Self {
        Pool(self.0.clone())
    }
}

struct FutureConnection {
    pool: Pool,
    conn: Option<BoxFuture<Connection, ConnectError>>,
}

impl Future for FutureConnection {
    type Item = Connection;
    type Error = ConnectError;

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
            },
            Conn::None => {
                self.pool.enqueue(park());
                Ok(Async::NotReady)
            }
        }
    }
}
