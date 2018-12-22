use std::fmt;
use std::io::{Error, ErrorKind};
use std::marker::PhantomData;
use std::time::{Duration, Instant};

use tokio::prelude::*;
use tokio_timer::{Delay, Error as TimerError};

use crate::io::IoFuture;

enum RetryState<H> {
    Checking(IoFuture<H>),
    Reconnecting(IoFuture<H>),
    Sleeping(Delay),
}

#[derive(Debug)]
enum RetryPoll<H: fmt::Debug> {
    Checking(Poll<H, Error>),
    Reconnecting(Poll<H, Error>),
    Sleeping(Poll<(), TimerError>),
}

pub(crate) struct RetryGuard<H, C, R> {
    check: C,
    reconnect: R,
    state: RetryState<H>,
    attempt: usize,
    max_attempt: usize,
    duration: Duration,
    phantom: PhantomData<H>,
}

impl<H, C, R> RetryGuard<H, C, R>
where
    C: Fn(H) -> IoFuture<H>,
    R: Fn() -> IoFuture<H>,
{
    pub(crate) fn new(
        handle: H,
        check: C,
        reconnect: R,
        max_attempt: usize,
        duration: Duration,
    ) -> RetryGuard<H, C, R> {
        RetryGuard {
            state: RetryState::Checking(check(handle)),
            check,
            reconnect,
            attempt: 0,
            max_attempt,
            duration,
            phantom: PhantomData,
        }
    }
}

impl<T: fmt::Debug> RetryState<T> {
    fn poll(&mut self) -> RetryPoll<T> {
        match self {
            RetryState::Checking(ref mut inner) => RetryPoll::Checking(inner.poll()),
            RetryState::Reconnecting(ref mut inner) => RetryPoll::Reconnecting(inner.poll()),
            RetryState::Sleeping(delay) => RetryPoll::Sleeping(delay.poll()),
        }
    }
}

impl<H, C, R> Future for RetryGuard<H, C, R>
where
    C: Fn(H) -> IoFuture<H>,
    R: Fn() -> IoFuture<H>,
    H: fmt::Debug,
{
    type Item = H;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.state.poll() {
            RetryPoll::Checking(Err(err)) => {
                if self.attempt >= self.max_attempt {
                    return Err(err);
                }
                let future = (self.reconnect)();
                self.state = RetryState::Reconnecting(future);
                self.attempt += 1;
                self.poll()
            }
            RetryPoll::Checking(result) => result,
            RetryPoll::Reconnecting(Err(err)) => {
                if self.attempt >= self.max_attempt {
                    return Err(err);
                }
                let deadline = Instant::now() + self.duration;
                let future = Delay::new(deadline);
                self.state = RetryState::Sleeping(future);
                self.attempt += 1;
                self.poll()
            }
            RetryPoll::Reconnecting(Ok(Async::NotReady)) => Ok(Async::NotReady),
            RetryPoll::Reconnecting(Ok(Async::Ready(handle))) => {
                let future = (self.check)(handle);
                self.state = RetryState::Checking(future);
                self.poll()
            }
            RetryPoll::Sleeping(Ok(Async::Ready(_))) => {
                let future = (self.reconnect)();
                self.state = RetryState::Reconnecting(future);
                self.poll()
            }
            RetryPoll::Sleeping(Ok(Async::NotReady)) => Ok(Async::NotReady),
            RetryPoll::Sleeping(Err(err)) => Err(Error::new(ErrorKind::Other, err)),
        }
    }
}

#[cfg(test)]
mod test {
    use std::io::{Error, ErrorKind};
    use std::sync::Mutex;
    use std::time::{Duration, Instant};

    use tokio::prelude::*;

    use crate::io::IoFuture;

    use super::RetryGuard;

    lazy_static! {
        static ref MUTEX: Mutex<u8> = Mutex::new(42);
    }

    static mut RECONNECT_ATTEMPT: usize = 0;

    fn check(h: u8) -> IoFuture<u8> {
        if h == 1 {
            let err = Error::new(ErrorKind::Other, "[check] it's fine");
            Box::new(future::err(err))
        } else {
            Box::new(future::ok(42))
        }
    }

    fn reconnect() -> IoFuture<u8> {
        let attempt = unsafe {
            RECONNECT_ATTEMPT += 1;
            RECONNECT_ATTEMPT
        };
        if attempt > 2 {
            Box::new(future::ok(2))
        } else {
            let err = Error::new(ErrorKind::Other, "[reconnect] it's fine");
            Box::new(future::err(err))
        }
    }

    fn bad_reconnect() -> IoFuture<u8> {
        unsafe { RECONNECT_ATTEMPT += 1 }
        let err = Error::new(ErrorKind::Other, "[bar_reconnect] it's fine");
        Box::new(future::err(err))
    }

    /// Same as `tokio::run`, but will panic if future panics and will return the result
    /// of future execution.
    fn run<F, T, U>(future: F) -> Result<T, U>
    where
        F: Future<Item = T, Error = U> + Send + 'static,
        T: Send + 'static,
        U: Send + 'static,
    {
        let mut runtime = tokio::runtime::Runtime::new().unwrap();
        let result = runtime.block_on(future);
        runtime.shutdown_on_idle().wait().unwrap();
        result
    }

    #[test]
    fn test_success_reconnection() {
        let _lock = MUTEX.lock().unwrap();
        unsafe { RECONNECT_ATTEMPT = 0 }

        let start = Instant::now();
        let duration = Duration::from_millis(100);
        let max_attempt = 3_usize;
        let done = Box::new(RetryGuard::new(
            1_u8,
            check,
            reconnect,
            max_attempt,
            duration,
        ));
        let v = run(done).unwrap();
        assert_eq!(v, 42);

        let spent = start.elapsed();
        assert!(spent >= Duration::from_millis(200));

        let attempt = unsafe { RECONNECT_ATTEMPT };
        assert_eq!(attempt, 3);
    }

    #[test]
    fn test_fail_reconnection() {
        let _lock = MUTEX.lock().unwrap();
        unsafe { RECONNECT_ATTEMPT = 0 }

        let start = Instant::now();
        let duration = Duration::from_millis(100);
        let max_attempt = 3_usize;
        let done = Box::new(RetryGuard::new(
            1_u8,
            check,
            bad_reconnect,
            max_attempt,
            duration,
        ));
        run(done).unwrap_err();

        let spent = start.elapsed();
        assert!(spent >= Duration::from_millis(200));

        let attempt = unsafe { RECONNECT_ATTEMPT };
        assert_eq!(attempt, 3);
    }

    #[test]
    fn test_without_reconnection() {
        let _lock = MUTEX.lock().unwrap();
        unsafe { RECONNECT_ATTEMPT = 0 }

        let start = Instant::now();
        let duration = Duration::from_millis(100);
        let max_attempt = 3_usize;
        let done = Box::new(RetryGuard::new(
            0_u8,
            check,
            bad_reconnect,
            max_attempt,
            duration,
        ));
        let actual = run(done).unwrap();
        assert_eq!(actual, 42);

        let spent = start.elapsed();
        assert!(spent < Duration::from_millis(50));

        let attempt = unsafe { RECONNECT_ATTEMPT };
        assert_eq!(attempt, 0);
    }
}
