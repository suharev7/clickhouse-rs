use std::{mem, time::Duration};

use log::warn;

use crate::{errors::Result, types::OptionsSource, Client, ClientHandle, Pool};

pub(crate) async fn retry_guard(
    handle: &mut ClientHandle,
    source: &OptionsSource,
    pool: Option<Pool>,
    max_attempt: usize,
    duration: Duration,
) -> Result<()> {
    let mut attempt = 0;
    let mut skip_check = false;

    loop {
        if skip_check {
            skip_check = false;
        } else {
            match check(handle).await {
                Ok(()) => return Ok(()),
                Err(err) => {
                    if attempt >= max_attempt {
                        return Err(err);
                    }
                }
            }
        }

        match reconnect(handle, source, pool.clone()).await {
            Ok(()) => continue,
            Err(err) => {
                skip_check = true;
                if attempt >= max_attempt {
                    return Err(err);
                }

                #[cfg(feature = "async_std")]
                {
                    use async_std::task;
                    task::sleep(duration).await;
                }

                #[cfg(not(feature = "async_std"))]
                {
                    tokio::time::sleep(duration).await;
                }
            }
        }

        attempt += 1;
    }
}

async fn check(c: &mut ClientHandle) -> Result<()> {
    c.ping().await
}

async fn reconnect(c: &mut ClientHandle, source: &OptionsSource, pool: Option<Pool>) -> Result<()> {
    warn!("[reconnect]");
    let mut nc = match pool {
        None => Client::open(source.clone(), pool).await?,
        Some(p) => p.get_handle().await?,
    };
    mem::swap(c, &mut nc);
    Ok(())
}
