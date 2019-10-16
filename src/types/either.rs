use crate::errors::Error;
use tokio::prelude::*;

pub(crate) enum Either<T, L, R>
where
    L: Future<Item = T, Error = Error> + Send,
    R: Future<Item = T, Error = Error> + Send,
{
    Left(L),
    Right(R),
}

impl<T, L, R> Future for Either<T, L, R>
where
    L: Future<Item = T, Error = Error> + Send,
    R: Future<Item = T, Error = Error> + Send,
{
    type Item = T;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self {
            Either::Left(inner) => inner.poll(),
            Either::Right(inner) => inner.poll(),
        }
    }
}
