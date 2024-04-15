use std::{net::SocketAddr, sync::{Arc, Mutex}, time::Instant};

use anyhow::{Context, Result};
use clap::Parser;
use quinn::{Connection, Endpoint, TokioRuntime};
use tokio::sync::Semaphore;
use tracing::{info, trace};

use crate::{bind_socket, drain_stream, handle_client_stream, parse_byte_size, send_data_on_stream, ClientStats};

pub const ALPN: &[u8] = b"n0/quinn-bench/0";

#[derive(Parser, Debug, Clone, Copy)]
#[clap(name = "quinn")]
pub struct Opt {
    /// The total number of clients which should be created
    #[clap(long = "clients", short = 'c', default_value = "1")]
    pub clients: usize,
    /// The total number of streams which should be created
    #[clap(long = "streams", short = 'n', default_value = "1")]
    pub streams: usize,
    /// The amount of concurrent streams which should be used
    #[clap(long = "max_streams", short = 'm', default_value = "1")]
    pub max_streams: usize,
    /// Number of bytes to transmit from server to client
    ///
    /// This can use SI prefixes for sizes. E.g. 1M will transfer 1MiB, 10G
    /// will transfer 10GiB.
    #[clap(long, default_value = "1G", value_parser = parse_byte_size)]
    pub download_size: u64,
    /// Number of bytes to transmit from client to server
    ///
    /// This can use SI prefixes for sizes. E.g. 1M will transfer 1MiB, 10G
    /// will transfer 10GiB.
    #[clap(long, default_value = "0", value_parser = parse_byte_size)]
    pub upload_size: u64,
    /// Show connection stats the at the end of the benchmark
    #[clap(long = "stats")]
    pub stats: bool,
    /// Whether to use the unordered read API
    #[clap(long = "unordered")]
    pub read_unordered: bool,
    /// Starting guess for maximum UDP payload size
    #[clap(long, default_value = "1200")]
    pub initial_mtu: u16,

    /// Send buffer size in bytes
    #[clap(long, default_value = "2097152")]
    send_buffer_size: usize,
    /// Receive buffer size in bytes
    #[clap(long, default_value = "2097152")]
    recv_buffer_size: usize,
}

/// Creates a server endpoint which runs on the given runtime
pub fn server_endpoint(rt: &tokio::runtime::Runtime, opt: &Opt) -> (SocketAddr, quinn::Endpoint) {
    let (key, cert) = {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
            (
                rustls::PrivateKey(cert.serialize_private_key_der()),
                vec![rustls::Certificate(cert.serialize_der().unwrap())],
            )
    };

    let mut crypto = rustls::ServerConfig::builder()
    .with_cipher_suites(PERF_CIPHER_SUITES)
    .with_safe_default_kx_groups()
    .with_protocol_versions(&[&rustls::version::TLS13])
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(cert, key)
    .unwrap();
    crypto.alpn_protocols = vec![ALPN.to_vec()];

    let transport = transport_config(opt);

    let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(crypto));
    server_config.transport_config(Arc::new(transport));

    let addr = SocketAddr::new("127.0.0.1".parse().unwrap(), 0);

    let socket = bind_socket(addr, opt.send_buffer_size, opt.recv_buffer_size).unwrap();

    let _guard = rt.enter();
    rt.block_on(async move {
        let ep = quinn::Endpoint::new(
            Default::default(),
            Some(server_config),
            socket,
            Arc::new(TokioRuntime),
        ).unwrap();
        let addr = ep.local_addr().unwrap();
        (addr, ep)
    })
}

pub fn transport_config(opt: &Opt) -> quinn::TransportConfig {
    // High stream windows are chosen because the amount of concurrent streams
    // is configurable as a parameter.
    let mut config = quinn::TransportConfig::default();
    config.max_concurrent_uni_streams(opt.max_streams.try_into().unwrap());
    config.initial_mtu(opt.initial_mtu);
    config
}

pub static PERF_CIPHER_SUITES: &[rustls::SupportedCipherSuite] = &[
    rustls::cipher_suite::TLS13_AES_128_GCM_SHA256,
    rustls::cipher_suite::TLS13_AES_256_GCM_SHA384,
    rustls::cipher_suite::TLS13_CHACHA20_POLY1305_SHA256,
];

pub async fn server(endpoint: quinn::Endpoint, opt: Opt) -> Result<()> {
    let mut server_tasks = Vec::new();

    // Handle only the expected amount of clients
    for _ in 0..opt.clients {
        let handshake = endpoint.accept().await.unwrap();
        let connection = handshake.await.context("handshake failed")?;

        server_tasks.push(tokio::spawn(async move {
            loop {
                let (mut send_stream, mut recv_stream) = match connection.accept_bi().await {
                    Err(quinn::ConnectionError::ApplicationClosed(_)) => break,
                    Err(e) => {
                        eprintln!("accepting stream failed: {e:?}");
                        break;
                    }
                    Ok(stream) => stream,
                };
                trace!("stream established");

                tokio::spawn(async move {
                    drain_stream(&mut recv_stream, opt.read_unordered).await?;
                    send_data_on_stream(&mut send_stream, opt.download_size).await?;
                    Ok::<_, anyhow::Error>(())
                });
            }

            if opt.stats {
                println!("\nServer connection stats:\n{:#?}", connection.stats());
            }
        }));
    }

    // Await all the tasks. We have to do this to prevent the runtime getting dropped
    // and all server tasks to be cancelled
    for handle in server_tasks {
        if let Err(e) = handle.await {
            eprintln!("Server task error: {e:?}");
        };
    }

    Ok(())
}


pub async fn client(server_addr: SocketAddr, opt: Opt) -> Result<ClientStats> {
    let (endpoint, connection) = connect_client(server_addr, opt).await?;

    let start = Instant::now();

    let connection = Arc::new(connection);

    let mut stats = ClientStats::default();
    let mut first_error = None;

    let sem = Arc::new(Semaphore::new(opt.max_streams));
    let results = Arc::new(Mutex::new(Vec::new()));
    for _ in 0..opt.streams {
        let permit = sem.clone().acquire_owned().await.unwrap();
        let results = results.clone();
        let connection = connection.clone();
        tokio::spawn(async move {
            let result =
                handle_client_stream(connection, opt.upload_size, opt.read_unordered).await;
            info!("stream finished: {:?}", result);
            results.lock().unwrap().push(result);
            drop(permit);
        });
    }

    // Wait for remaining streams to finish
    let _ = sem.acquire_many(opt.max_streams as u32).await.unwrap();

    for result in results.lock().unwrap().drain(..) {
        match result {
            Ok((upload_result, download_result)) => {
                stats.upload_stats.stream_finished(upload_result);
                stats.download_stats.stream_finished(download_result);
            }
            Err(e) => {
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
        }
    }

    stats.upload_stats.total_duration = start.elapsed();
    stats.download_stats.total_duration = start.elapsed();

    // Explicit close of the connection, since handles can still be around due
    // to `Arc`ing them
    connection.close(0u32.into(), b"Benchmark done");

    endpoint.close(0u32.into(), b"");

    if opt.stats {
        println!("\nClient connection stats:\n{:#?}", connection.stats());
    }

    match first_error {
        None => Ok(stats),
        Some(e) => Err(e),
    }
}

/// Create a client endpoint and client connection
pub async fn connect_client(
    // rt: &tokio::runtime::Runtime,
    server_addr: SocketAddr,
    opt: Opt,
) -> Result<(Endpoint, Connection)> {
    // let endpoint = MagicEndpoint::builder()
    //     .alpns(vec![ALPN.to_vec()])
    //     .relay_mode(RelayMode::Disabled)
    //     .transport_config(transport_config(&opt))
    //     .bind(0)
    //     .await
    //     .unwrap();

    // // TODO: We don't support passing client transport config currently
    // // let mut client_config = quinn::ClientConfig::new(Arc::new(crypto));
    // // client_config.transport_config(Arc::new(transport_config(&opt)));

    // let connection = endpoint
    //     .connect(server_addr, ALPN)
    //     .await
    //     .context("unable to connect")?;
    // trace!("connected");


    let mut crypto = rustls::ClientConfig::builder()
    .with_cipher_suites(PERF_CIPHER_SUITES)
    .with_safe_default_kx_groups()
    .with_protocol_versions(&[&rustls::version::TLS13])
    .unwrap()
    .with_custom_certificate_verifier(SkipServerVerification::new())
    .with_no_client_auth();
    crypto.alpn_protocols = vec![ALPN.to_vec()];

    let transport = transport_config(&opt);

    let mut config = quinn::ClientConfig::new(Arc::new(crypto));
    config.transport_config(Arc::new(transport));

    let addr = SocketAddr::new("127.0.0.1".parse().unwrap(), 0);

    let socket = bind_socket(addr, opt.send_buffer_size, opt.recv_buffer_size).unwrap();

    // let _guard = rt.enter();
    // rt.block_on(async move {
        let ep = quinn::Endpoint::new(
            Default::default(),
            None,
            socket,
            Arc::new(TokioRuntime),
        ).unwrap();
        // let addr = ep.local_addr().unwrap();
        // (addr, ep)
        let connection = ep
        .connect_with(config, server_addr, "local")?
        .await
        .context("connecting").unwrap();
        Ok((ep, connection))
    // })

    // Ok((endpoint, connection))
}

struct SkipServerVerification;

impl SkipServerVerification {
    fn new() -> Arc<Self> {
        Arc::new(Self)
    }
}

impl rustls::client::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::Certificate,
        _intermediates: &[rustls::Certificate],
        _server_name: &rustls::ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        _now: std::time::SystemTime,
    ) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::ServerCertVerified::assertion())
    }
}