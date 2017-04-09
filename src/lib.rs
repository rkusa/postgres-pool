extern crate tokio_postgres;
extern crate tokio_core;
extern crate futures;

use std::fmt;
use std::error;
use std::error::Error as _Error;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, LockResult, MutexGuard};
use futures::task::{Task, park};
use futures::{Future, Poll, Async, BoxFuture};
use tokio_core::reactor::{Remote};
use tokio_postgres::error::ConnectError;
use tokio_postgres::{Connection, TlsMode};
use tokio_postgres::params::{ConnectParams, IntoConnectParams};

pub struct InnerPool {
    params: ConnectParams,
    remote: Remote,
    queue: VecDeque<Task>,
    conns: VecDeque<Connection>,
    size: i32,
}

pub struct Pool(Arc<Mutex<InnerPool>>);

enum Conn {
    Now(Connection),
    Future(BoxFuture<Connection, ConnectError>),
    Err(Error),
    None
}

impl Pool {
    pub fn new<T>(params: T, remote: Remote, size: i32) -> Result<Self, ConnectError>
        where T: IntoConnectParams
    {
        let params = params.into_connect_params().map_err(ConnectError::ConnectParams)?;

        Ok(Pool(Arc::new(Mutex::new(InnerPool{
            params: params,
            remote: remote,
            queue: VecDeque::new(),
            conns: VecDeque::new(),
            size: size,
        }))))
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
                if let Some(handle) = inner.remote.handle() {
                    let conn = Connection::connect(inner.params.clone(),
                                       TlsMode::None,
                                       &handle);
                    Conn::Future(conn)
                } else {
                    Conn::Err(Error::EventLoop)
                }
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

                E::from(err)
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
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if let Some(ref mut conn) = self.conn {
            return conn.poll().map_err(Error::Connect);
        }

        match self.pool.acquire() {
            Conn::Now(conn) => Ok(Async::Ready(conn)),
            Conn::Future(mut conn) => {
                let result = conn.poll().map_err(Error::Connect);
                if let Ok(Async::NotReady) = result {
                    self.conn = Some(conn);
                }
                result
            },
            Conn::Err(err) => Err(err),
            Conn::None => {
                self.pool.enqueue(park());
                Ok(Async::NotReady)
            }
        }
    }
}

#[derive(Debug)]
pub enum Error {
    Connect(ConnectError),
    EventLoop
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}", self.description())
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::Connect(ref err) => err.description(),
            Error::EventLoop => "Event loop not running",
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            Error::Connect(ref err) => Some(err as &error::Error),
            _ => None,
        }
    }
}

