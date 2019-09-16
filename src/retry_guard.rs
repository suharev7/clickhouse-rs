use std::{
    future::Future,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use tokio::timer::Interval;

use pin_project::{pin_project, project};

use crate::errors::Result;

#[pin_project]
enum RetryState<CF, RF> {
    Check(#[pin] CF),
    Reconnect(#[pin] RF),
    Sleep(Interval),
}

#[derive(Debug)]
enum RetryPoll<H> {
    Check(Poll<Result<H>>),
    Reconnect(Poll<Result<H>>),
    Sleep(Poll<()>),
}

#[pin_project]
pub(crate) struct RetryGuard<H, C, R, CF, RF> {
    check: C,
    reconnect: R,
    #[pin]
    state: RetryState<CF, RF>,
    attempt: usize,
    max_attempt: usize,
    duration: Duration,
    phantom: PhantomData<H>,
}

impl<H, C, R, CF, RF> RetryGuard<H, C, R, CF, RF>
where
    C: Fn(H) -> CF,
    R: Fn() -> RF,
    CF: Future<Output = Result<H>>,
    RF: Future<Output = Result<H>>,
{
    pub(crate) fn new(
        handle: H,
        check: C,
        reconnect: R,
        max_attempt: usize,
        duration: Duration,
    ) -> RetryGuard<H, C, R, CF, RF> {
        RetryGuard {
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

impl<H, CF, RF> RetryState<CF, RF>
where
    CF: Future<Output = Result<H>>,
    RF: Future<Output = Result<H>>,
{
    #[project]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> RetryPoll<H> {
        #[project]
        match self.project() {
            RetryState::Check(inner) => RetryPoll::Check(inner.poll(cx)),
            RetryState::Reconnect(inner) => RetryPoll::Reconnect(inner.poll(cx)),
            RetryState::Sleep(ref mut inner) => {
                RetryPoll::Sleep(match inner.poll_next(cx) {
                    Poll::Ready(_) => Poll::Ready(()),
                    Poll::Pending => Poll::Pending,
                })
            },
        }
    }
}

impl<H, C, R, CF, RF> RetryGuard<H, C, R, CF, RF> {
    fn set_state(self: Pin<&mut Self>, state: RetryState<CF, RF>, attempt_inc: usize) {
        let this = unsafe { self.get_unchecked_mut() };
        this.state = state;
        this.attempt += attempt_inc;
    }
}

impl<H, C, R, CF, RF> Future for RetryGuard<H, C, R, CF, RF>
where
    C: Fn(H) -> CF,
    R: Fn() -> RF,
    CF: Future<Output = Result<H>>,
    RF: Future<Output = Result<H>>,
{
    type Output = Result<H>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project().state.poll(cx) {
            RetryPoll::Check(Poll::Ready(Err(err))) => {
                if self.attempt >= self.max_attempt {
                    return Poll::Ready(Err(err));
                }
                let future = (self.reconnect)();
                self.as_mut().set_state(RetryState::Reconnect(future), 1);
                self.poll(cx)
            }
            RetryPoll::Check(result) => result,
            RetryPoll::Reconnect(Poll::Ready(Err(err))) => {
                if self.attempt >= self.max_attempt {
                    return Poll::Ready(Err(err));
                }
                let future =  Interval::new_interval(self.duration);

                self.as_mut().set_state(RetryState::Sleep(future), 1);
                self.poll(cx)
            }
            RetryPoll::Reconnect(Poll::Pending) => {
                Poll::Pending
            },
            RetryPoll::Reconnect(Poll::Ready(Ok(handle))) => {
                let future = (self.check)(handle);

                self.as_mut().set_state(RetryState::Check(future), 0);
                self.poll(cx)
            }
            RetryPoll::Sleep(Poll::Ready(())) => {
                let future = (self.reconnect)();

                self.as_mut().set_state(RetryState::Reconnect(future), 0);
                self.poll(cx)
            }
            RetryPoll::Sleep(Poll::Pending) => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod test {
    use std::{
        io,
        sync::Mutex,
        time::Instant,
    };

    use lazy_static::lazy_static;

    use crate::errors::Error;

    use super::*;

    lazy_static! {
        static ref MUTEX: Mutex<()> = Mutex::new(());
    }

    static mut RECONNECT_ATTEMPT: usize = 0;

    async fn check(h: u8) -> Result<u8> {
        if h == 1 {
            let err = io::Error::new(io::ErrorKind::Other, "[check] it's fine");
            Err(err.into())
        } else {
            Ok(42)
        }
    }

    async fn reconnect() -> Result<u8> {
        let attempt = unsafe {
            RECONNECT_ATTEMPT += 1;
            RECONNECT_ATTEMPT
        };
        if attempt > 2 {
            Ok(2)
        } else {
            let err: Error = From::from("[reconnect] it's fine");
            Err(err)
        }
    }

    async fn bad_reconnect() -> Result<u8> {
        unsafe { RECONNECT_ATTEMPT += 1 }
        let err = io::Error::new(io::ErrorKind::Other, "[bar_reconnect] it's fine");
        Err(err.into())
    }

    #[test]
    fn test_success_reconnection() -> io::Result<()> {
        let _lock = MUTEX.lock().unwrap();
        let mut rt = tokio::runtime::current_thread::Runtime::new().unwrap();
        rt.block_on(async {
            unsafe { RECONNECT_ATTEMPT = 0 }

            let start = Instant::now();
            let duration = Duration::from_millis(100);
            let max_attempt = 3_usize;
            let v = RetryGuard::new(
                1_u8,
                |h| Box::pin(check(h)),
                || Box::pin(reconnect()),
                max_attempt,
                duration,
            ).await?;
            assert_eq!(v, 42);

            let spent = start.elapsed();
            assert!(spent >= Duration::from_millis(200));

            let attempt = unsafe { RECONNECT_ATTEMPT };
            assert_eq!(attempt, 3);

            Ok(())
        })
    }

    #[test]
    fn test_fail_reconnection() -> io::Result<()> {
        let _lock = MUTEX.lock().unwrap();
        let mut rt = tokio::runtime::current_thread::Runtime::new().unwrap();
        rt.block_on(async {
            unsafe { RECONNECT_ATTEMPT = 0 }

            let start = Instant::now();
            let duration = Duration::from_millis(100);
            let max_attempt = 3_usize;
            let ret = RetryGuard::new(
                1_u8,
                |h| Box::pin(check(h)),
                || Box::pin(bad_reconnect()),
                max_attempt,
                duration,
            ).await;

            ret.unwrap_err();

            let spent = start.elapsed();
            assert!(spent >= Duration::from_millis(200));

            let attempt = unsafe { RECONNECT_ATTEMPT };
            assert_eq!(attempt, 3);

            Ok(())
        })
    }

    #[test]
    fn test_without_reconnection() -> io::Result<()> {
        let _lock = MUTEX.lock().unwrap();

        let mut rt = tokio::runtime::current_thread::Runtime::new().unwrap();
        rt.block_on(async {
            unsafe { RECONNECT_ATTEMPT = 0 }

            let start = Instant::now();
            let duration = Duration::from_millis(100);
            let max_attempt = 3_usize;

            let actual = RetryGuard::new(
                0_u8,
                |h| Box::pin(check(h)),
                || Box::pin(bad_reconnect()),
                max_attempt,
                duration,
            ).await?;
            assert_eq!(actual, 42);

            let spent = start.elapsed();
            assert!(spent < Duration::from_millis(50));

            let attempt = unsafe { RECONNECT_ATTEMPT };
            assert_eq!(attempt, 0);

            Ok(())
        })
    }
}
