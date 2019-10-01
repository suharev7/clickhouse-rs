use crate::errors::Error;

use futures::{Future, Stream};

pub type BoxFuture<T> = Box<dyn Future<Item = T, Error = Error> + Send>;
pub type BoxStream<T> = Box<dyn Stream<Item = T, Error = Error> + Send>;
