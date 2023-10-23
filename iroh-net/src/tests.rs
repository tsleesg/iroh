//!
use std::time::Duration;

use crate::{MagicEndpoint, derp::DerpMode, PeerAddr, AddrInfo};

const ALPN: &[u8] = b"echo-test".as_slice();

async fn make_ep() -> anyhow::Result<MagicEndpoint> {
    MagicEndpoint::builder()
        .derp_mode(DerpMode::Default)
        .alpns(vec![ALPN.into()])
        .bind(0).await
}

#[tokio::test]
async fn connect_by_peer_and_region() {
    let ep1 = make_ep().await.unwrap();
    while let None = ep1.my_derp().await {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let addr = PeerAddr {
        peer_id: ep1.peer_id(),
        info: AddrInfo {
            derp_region: ep1.my_derp().await,
            direct_addresses: Default::default(),
        }
    };
    let n = 100;
    let accept = tokio::spawn(async move {
        let mut msg = Vec::new();
        for _ in 0..n {
            let conn = ep1.accept().await.unwrap().await?;
            let (mut send, mut recv) = conn.accept_bi().await?;
            msg = recv.read_to_end(1024).await?;
            send.write_all(&msg).await?;
            send.finish().await?;
        }
        anyhow::Ok(msg)
    });
    for i in 0..n {
        let ep2 = make_ep().await.unwrap();
        let addr = addr.clone();
        let msg = format!("hello {}", i);
        println!("sending {}", msg);
        let msg = msg.into_bytes();
        let expected = msg.clone();
        let connect = tokio::spawn(async move {
            let conn = ep2.connect(addr, ALPN).await.unwrap();
            let (mut send, mut recv) = conn.open_bi().await?;
            send.write_all(&msg).await?;
            send.finish().await?;
            let msg = recv.read_to_end(1024).await?;
            anyhow::Ok(msg)
        });
        let echoed = connect.await.unwrap().unwrap();
        assert_eq!(echoed, expected);
    }
    let received = accept.await.unwrap().unwrap();
    let expected = format!("hello {}", (n-1)).into_bytes();
    assert_eq!(received, expected);
}