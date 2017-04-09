extern crate tokio_postgres;
extern crate tokio_core;
extern crate futures;

use std::fmt;
use std::error;
use std::error::Error as _Error;
use std::collections::VecDeque;
use std::rc::Rc;
use std::cell::RefCell;
use futures::task::{Task, park};
use futures::{Future, Poll, Async, BoxFuture};
use tokio_core::reactor::Handle;
use tokio_postgres::error::ConnectError;
use tokio_postgres::{Connection, TlsMode};
use tokio_postgres::params::{ConnectParams, IntoConnectParams};

pub struct InnerPool {
    params: ConnectParams,
    handle: Handle,
    queue: VecDeque<Task>,
    conns: VecDeque<Connection>,
    size: i32,
}

pub struct Pool(Rc<RefCell<InnerPool>>);

enum Conn {
    Now(Connection),
    Future(BoxFuture<Connection, ConnectError>),
    None,
}

impl Pool {
    pub fn new<T>(params: T, handle: Handle, size: i32) -> Result<Self, ConnectError>
        where T: IntoConnectParams
    {
        let params = params.into_connect_params()
            .map_err(ConnectError::ConnectParams)?;

        Ok(Pool(Rc::new(RefCell::new(InnerPool{
            params: params,
            handle: handle,
            queue: VecDeque::new(),
            conns: VecDeque::new(),
            size: size,
        }))))
    }

    fn acquire(&self) -> Conn {
        let mut inner = self.0.borrow_mut();
        if inner.size == 0 {
            return Conn::None;
        }

        inner.size -= 1;

        match inner.conns.pop_front() {
            Some(conn) => Conn::Now(conn),
            None => {
                let conn = Connection::connect(inner.params.clone(), TlsMode::None, &inner.handle);
                Conn::Future(conn)
            }
        }
    }

    fn release(&self, conn: Connection) {
        let mut inner = self.0.borrow_mut();
        inner.conns.push_back(conn);
        inner.size += 1;

        if let Some(task) = inner.queue.pop_front() {
            task.unpark();
        }
    }

    fn connection_lost(&self) {
        let mut inner = self.0.borrow_mut();
        inner.size += 1;
    }

    fn enqueue(&self, task: Task) {
        let mut inner = self.0.borrow_mut();
        inner.queue.push_back(task);
    }

    pub fn with_connection<F, R, I, E>(&self, f: F) -> Box<Future<Item=I, Error=E> + 'static>
        where F: FnOnce(Connection) -> R + 'static,
              R: Future<Item = (I, Connection), Error = (E, Option<Connection>)> + 'static,
              I: 'static,
              E: From<Error> + 'static
    {
        let pool1 = self.clone();
        let pool2 = self.clone();
        let conn = FutureConnection {
            pool: self.clone(),
            conn: None,
        };
        let fut = conn.map_err(move |err| {
                let mut inner = pool1.0.borrow_mut();
                inner.size += 1;

                E::from(Error::Connect(err))
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
            });
        Box::new(fut)
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
            }
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
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            Error::Connect(ref err) => Some(err as &error::Error),
        }
    }
}
