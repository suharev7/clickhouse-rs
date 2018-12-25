use std::io::Error;

use futures::Future;

pub type IoFuture<T> = Box<dyn Future<Item = T, Error = Error> + Send>;
