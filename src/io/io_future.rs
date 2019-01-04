use crate::errors::Error;

use futures::Future;

pub type BoxFuture<T> = Box<dyn Future<Item = T, Error = Error> + Send>;
