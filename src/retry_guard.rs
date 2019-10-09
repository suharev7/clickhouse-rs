use std::{
    fmt,
    marker::PhantomData,
    time::{Duration, Instant},
};

use tokio::prelude::*;
use tokio_timer::{Delay, Error as TimerError};

use crate::{errors::Error, io::BoxFuture};

enum RetryState<H> {
    Check(BoxFuture<H>),
    Reconnect(BoxFuture<H>),
    Sleep(Delay),
}

#[derive(Debug)]
enum RetryPoll<H: fmt::Debug> {
    Check(Poll<H, Error>),
    Reconnect(Poll<H, Error>),
    Sleep(Poll<(), TimerError>),
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
    C: Fn(H) -> BoxFuture<H>,
    R: Fn() -> BoxFuture<H>,
{
    pub(crate) fn new(
        handle: H,
        check: C,
        reconnect: R,
        max_attempt: usize,
        duration: Duration,
    ) -> RetryGuard<H, C, R> {
        Self {
            state: RetryState::Check(check(handle)),
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
            RetryState::Check(ref mut inner) => RetryPoll::Check(inner.poll()),
            RetryState::Reconnect(ref mut inner) => RetryPoll::Reconnect(inner.poll()),
            RetryState::Sleep(delay) => RetryPoll::Sleep(delay.poll()),
        }
    }
}

impl<H, C, R> Future for RetryGuard<H, C, R>
where
    C: Fn(H) -> BoxFuture<H>,
    R: Fn() -> BoxFuture<H>,
    H: fmt::Debug,
{
    type Item = H;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.state.poll() {
            RetryPoll::Check(Err(err)) => {
                warn!("[check] {}", err);
                if self.attempt >= self.max_attempt {
                    return Err(err);
                }
                let future = (self.reconnect)();
                self.state = RetryState::Reconnect(future);
                self.attempt += 1;
                self.poll()
            }
            RetryPoll::Check(result) => result,
            RetryPoll::Reconnect(Err(err)) => {
                if self.attempt >= self.max_attempt {
                    return Err(err);
                }
                let deadline = Instant::now() + self.duration;
                let future = Delay::new(deadline);
                self.state = RetryState::Sleep(future);
                self.attempt += 1;
                self.poll()
            }
            RetryPoll::Reconnect(Ok(Async::NotReady)) | RetryPoll::Sleep(Ok(Async::NotReady)) => {
                Ok(Async::NotReady)
            }
            RetryPoll::Reconnect(Ok(Async::Ready(handle))) => {
                let future = (self.check)(handle);
                self.state = RetryState::Check(future);
                self.poll()
            }
            RetryPoll::Sleep(Ok(Async::Ready(_))) => {
                let future = (self.reconnect)();
                self.state = RetryState::Reconnect(future);
                self.poll()
            }
            RetryPoll::Sleep(Err(err)) => Err(err.into()),
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Mutex;
    use std::time::{Duration, Instant};

    use tokio::prelude::*;

    use crate::errors::Error;
    use crate::io::BoxFuture;

    use super::RetryGuard;

    lazy_static! {
        static ref MUTEX: Mutex<()> = Mutex::new(());
    }

    static mut RECONNECT_ATTEMPT: usize = 0;

    fn check(h: u8) -> BoxFuture<u8> {
        if h == 1 {
            let err: Error = "[check] it's fine".into();
            Box::new(future::err(err))
        } else {
            Box::new(future::ok(42))
        }
    }

    fn reconnect() -> BoxFuture<u8> {
        let attempt = unsafe {
            RECONNECT_ATTEMPT += 1;
            RECONNECT_ATTEMPT
        };
        if attempt > 2 {
            Box::new(future::ok(2))
        } else {
            let err: Error = "[reconnect] it's fine".into();
            Box::new(future::err(err))
        }
    }

    fn bad_reconnect() -> BoxFuture<u8> {
        unsafe { RECONNECT_ATTEMPT += 1 }
        let err: Error = "[bar_reconnect] it's fine".into();
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
