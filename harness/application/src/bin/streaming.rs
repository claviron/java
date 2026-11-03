//! Isolated real-Link streaming acceptance. No listener, load test or timers.
use claviron::orbit::transport::{
    Exchange, Pending, TRANSPORT_BUFFER_BYTES, TransportClient,
};
use std::io::Write;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;
fn signal(value: &str) -> Result<()> {
    println!("{value}");
    std::io::stdout().flush()?;
    Ok(())
}
fn send(
    client: &TransportClient,
    path: &[u8],
    upload: bool,
) -> Result<Pending> {
    Ok(client.send(
        "stream",
        Exchange {
            method: "POST".into(),
            scheme: "http".into(),
            raw_path: path.to_vec(),
            headers: vec![(b"host".to_vec(), b"example.test".to_vec())],
            ..Default::default()
        },
        upload,
    )?)
}
async fn command(
    input: &mut tokio::io::Lines<BufReader<tokio::io::Stdin>>,
    expected: &str,
) -> Result<()> {
    if input.next_line().await?.as_deref() != Some(expected) {
        return Err(format!("expected {expected}").into());
    }
    Ok(())
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let fleet = std::env::args().nth(1).ok_or("usage: streaming <fleet>")?;
    let client = TransportClient::join(&fleet, "stream-peer", 1)?;
    let transfer =
        TRANSPORT_BUFFER_BYTES.checked_mul(4).ok_or("transfer overflow")?;
    signal(&format!("PEER_READY {transfer}"))?;
    let mut input = BufReader::new(tokio::io::stdin()).lines();
    command(&mut input, "run").await?;
    let (_upload, mut response, reply) =
        send(&client, b"/write", false)?.split();
    if reply.head().await?.status != 200 {
        return Err("stream head failed".into());
    }
    signal("WRITE_HEAD")?;
    command(&mut input, "read").await?;
    let mut bytes = Vec::new();
    response.read_to_end(&mut bytes).await?;
    if bytes != vec![42; transfer] {
        return Err("write bytes changed across FFM or FIN".into());
    }
    drop(response);
    drop(_upload);

    let (upload, response, reply) =
        send(&client, b"/write-cancel", false)?.split();
    if reply.head().await?.status != 200 {
        return Err("cancel head failed".into());
    }
    signal("WRITE_CANCEL_HEAD")?;
    command(&mut input, "cancel-write").await?;
    drop(response);
    drop(upload);
    command(&mut input, "write-cancelled").await?;

    let (upload, response, reply) =
        send(&client, b"/read-cancel", true)?.split();
    if reply.head().await?.status != 200 {
        return Err("read cancel head failed".into());
    }
    signal("READ_HEAD")?;
    command(&mut input, "cancel-read").await?;
    drop(response);
    drop(upload);
    command(&mut input, "read-cancelled").await?;

    let (_upload, mut response, reply) =
        send(&client, b"/read-abort", true)?.split();
    let _ = reply.head().await; // Head may race the intentionally immediate reset.
    let mut bytes = Vec::new();
    if response.read_to_end(&mut bytes).await.is_ok() {
        return Err("explicit read cancel became normal EOF".into());
    }
    drop(response);
    drop(_upload);
    command(&mut input, "read-aborted").await?;

    let (_upload, mut response, reply) =
        send(&client, b"/producer-fail", false)?.split();
    let _ = reply.head().await;
    let mut bytes = Vec::new();
    if response.read_to_end(&mut bytes).await.is_ok() {
        return Err("producer failure became successful EOF".into());
    }
    drop(response);
    drop(_upload);

    let (_upload, mut response, reply) =
        send(&client, b"/echo", false)?.split();
    if reply.head().await?.status != 200 {
        return Err("empty upload head failed".into());
    }
    let mut empty = Vec::new();
    response.read_to_end(&mut empty).await?;
    if !empty.is_empty() {
        return Err("EOF produced data".into());
    }
    drop(response);
    drop(_upload);

    let (mut upload, mut response, reply) =
        send(&client, b"/echo", true)?.split();
    if reply.head().await?.status != 200 {
        return Err("duplex head failed".into());
    }
    let payload = (0..transfer).map(|i| i as u8).collect::<Vec<_>>();
    let sent = payload.clone();
    let writer = async {
        upload.write_all(&sent).await?;
        upload.shutdown().await?;
        Ok::<_, Error>(())
    };
    let reader = async {
        let mut bytes = Vec::new();
        response.read_to_end(&mut bytes).await?;
        Ok::<_, Error>(bytes)
    };
    let (_, returned) = tokio::try_join!(writer, reader)?;
    if returned != payload {
        return Err("full-duplex echo changed bytes or EOF".into());
    }
    drop(response);
    drop(upload);
    let (_upload, mut response, reply) =
        send(&client, b"/plain", false)?.split();
    if reply.head().await?.status != 200 {
        return Err("healthy request failed after stream cancellations".into());
    }
    let mut bytes = Vec::new();
    response.read_to_end(&mut bytes).await?;
    if bytes != b"healthy" {
        return Err("healthy request failed".into());
    }
    drop(response);
    drop(_upload);
    signal("HTTP_CHECKS_PASSED")?;

    let (mut upload, mut response, reply) =
        send(&client, b"/drain", true)?.split();
    if reply.head().await?.status != 200 {
        return Err("drain head failed".into());
    }
    signal("DRAIN_HEAD")?;
    command(&mut input, "drain").await?;
    let writer = async {
        upload.write_all(&payload).await?;
        upload.shutdown().await?;
        Ok::<_, Error>(())
    };
    let reader = async {
        let mut bytes = Vec::new();
        response.read_to_end(&mut bytes).await?;
        Ok::<_, Error>(bytes)
    };
    let (_, returned) = tokio::try_join!(writer, reader)?;
    if returned != payload {
        return Err(
            "withdrawal truncated an accepted streaming exchange".into()
        );
    }
    drop(response);
    drop(upload);
    signal("STREAMING_CHECKS_PASSED")?;
    command(&mut input, "stop").await?;
    client.leave();
    signal("PEER_DRAINED")?;
    Ok(())
}
