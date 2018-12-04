use std::io::Error;

use futures::Future;

pub type IoFuture<T> = Box<Future<Item = T, Error = Error> + Send>;
