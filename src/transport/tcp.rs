//! Raw TCP transport pieces: dialing with timeout and SO settings, and the
//! listener used by `rtransfer serve` (+ SSH-bootstrapped servers).

use std::net::IpAddr;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::net::{TcpListener, TcpStream};

/// Bind `bind:port`; bind the resolver when port 0 and return the actual port.
pub async fn listen(bind: &str, port: u16) -> Result<(TcpListener, u16)> {
    let addr: IpAddr = bind
        .parse()
        .with_context(|| format!("bad bind address {bind:?} (use an IP like 0.0.0.0)"))?;
    let listener = TcpListener::bind((addr, port))
        .await
        .with_context(|| format!("bind {bind}:{port} failed"))?;
    let local = listener.local_addr()?;
    Ok((listener, local.port()))
}

/// Connect with timeout, nodelay, and 65 KiB+ default buffers.
pub async fn connect(host: &str, port: u16, timeout: Duration) -> Result<TcpStream> {
    let fut = TcpStream::connect((host, port));
    let stream = tokio::time::timeout(timeout, fut)
        .await
        .with_context(|| format!("timeout connecting to {host}:{port}"))?
        .with_context(|| format!("cannot connect to {host}:{port}"))?;
    stream.set_nodelay(true).ok();
    Ok(stream)
}

/// Turn a stream into a [`crate::linkmux::Link`].
pub fn as_link(s: TcpStream) -> crate::linkmux::Link {
    Box::new(s)
}
