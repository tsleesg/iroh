use std::{
    collections::BTreeMap,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    path::PathBuf,
    sync::Arc,
};

use anyhow::{Context, Result};
use clap::Parser;
use iroh::{
    bytes::util::runtime,
    client::quic::RPC_ALPN,
    net::{
        defaults::default_derp_map,
        derp::{DerpMap, DerpMode},
        key::{PublicKey, SecretKey},
    },
    node::{Node, StaticTokenAuthHandler},
    rpc_protocol::{ProviderRequest, ProviderResponse, ProviderService},
    util::{fs::load_secret_key, path::IrohPaths},
};
use quic_rpc::{transport::quinn::QuinnServerEndpoint, ServiceEndpoint};
const MAX_RPC_CONNECTIONS: u32 = 16;
const MAX_RPC_STREAMS: u64 = 1024;

#[derive(Parser, Debug, Clone)]
#[clap(version, verbatim_doc_comment)]
pub struct Cli {
    #[clap(subcommand)]
    pub command: Commands,
}

#[derive(Parser, Debug, Clone)]
pub enum Commands {
    Start {
        /// Number of nodes to start.
        #[clap(long)]
        nodes: usize,
        /// Root path of where the data will be stored.
        #[clap(long)]
        root: PathBuf,
    },
}

#[derive(Default)]
struct Nodes {
    nodes: BTreeMap<PublicKey, NodeWrapper>,
}

struct NodeWrapper {
    i: usize,
    node: Node<iroh::bytes::store::flat::Store>,
    client: iroh::client::mem::Iroh,
}

impl Cli {
    pub async fn run(self, rt: runtime::Handle) -> Result<()> {
        match self.command {
            Commands::Start { nodes, root } => {
                println!("Starting {nodes} nodes...");

                tokio::fs::create_dir_all(&root).await?;

                let mut managed_nodes = Nodes::default();
                let iroh_port_start: u16 = 7777;
                for i in 0..nodes {
                    let iroh_port = iroh_port_start + u16::try_from(i).unwrap() * 2;
                    let root_path = root.join(format!("iroh-{}", i));
                    let node = start_daemon_node(
                        &rt,
                        StartOptions {
                            addr: format!("0.0.0.0:{}", iroh_port).parse().unwrap(),
                            keylog: false,
                            derp_map: Some(default_derp_map()),
                            root_path,
                        },
                    )
                    .await?;
                    let node_id = node.peer_id();
                    let client = node.client();
                    managed_nodes
                        .nodes
                        .insert(node_id, NodeWrapper { i, node, client });
                }

                // TODO: wait for ctrl messages via rpc
                loop {
                    tokio::select! {
                        biased;
                        _ = tokio::signal::ctrl_c() => {
                            println!("Shutting down nodes...");
                            for (id, node) in managed_nodes.nodes.into_iter() {
                                println!("{} shutting down...", id);
                                node.client.node.shutdown(false).await?;
                            }

                            return Ok(());
                        }
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
struct StartOptions {
    addr: SocketAddr,
    keylog: bool,
    derp_map: Option<DerpMap>,
    root_path: PathBuf,
}

async fn start_daemon_node(
    rt: &runtime::Handle,
    opts: StartOptions,
) -> Result<Node<iroh::bytes::store::flat::Store>> {
    let blob_dir = IrohPaths::BaoFlatStoreComplete.with_root(&opts.root_path);
    let partial_blob_dir = IrohPaths::BaoFlatStorePartial.with_root(&opts.root_path);
    let meta_dir = IrohPaths::BaoFlatStoreMeta.with_root(&opts.root_path);
    let peer_data_path = IrohPaths::PeerData.with_root(&opts.root_path);
    tokio::fs::create_dir_all(&blob_dir).await?;
    tokio::fs::create_dir_all(&partial_blob_dir).await?;
    let bao_store =
        iroh::bytes::store::flat::Store::load(&blob_dir, &partial_blob_dir, &meta_dir, rt)
            .await
            .with_context(|| format!("Failed to load iroh database from {}", blob_dir.display()))?;
    let key = Some(IrohPaths::SecretKey.with_root(&opts.root_path));
    let doc_store =
        iroh::sync::store::fs::Store::new(IrohPaths::DocsDatabase.with_root(&opts.root_path))?;
    spawn_daemon_node(rt, bao_store, doc_store, key, peer_data_path, opts).await
}

async fn spawn_daemon_node<B: iroh::bytes::store::Store, D: iroh::sync::store::Store>(
    rt: &runtime::Handle,
    bao_store: B,
    doc_store: D,
    key: Option<PathBuf>,
    peers_data_path: PathBuf,
    opts: StartOptions,
) -> Result<Node<B>> {
    let secret_key = get_secret_key(key).await?;

    let mut builder = Node::builder(bao_store, doc_store)
        .custom_auth_handler(Arc::new(StaticTokenAuthHandler::new(None)))
        .peers_data_path(peers_data_path)
        .keylog(opts.keylog);
    if let Some(dm) = opts.derp_map {
        builder = builder.derp_mode(DerpMode::Custom(dm));
    }
    let builder = builder.bind_addr(opts.addr).runtime(rt);
    let provider = builder.secret_key(secret_key).spawn().await?;
    let eps = provider.local_endpoints().await?;
    println!("Listening addresses:");
    for ep in eps {
        println!("  {}", ep.addr);
    }
    let region = provider.my_derp();
    println!(
        "DERP Region: {}",
        region.map_or("None".to_string(), |r| r.to_string())
    );
    println!("PeerID: {}", provider.peer_id());
    println!();
    Ok(provider)
}

async fn get_secret_key(key: Option<PathBuf>) -> Result<SecretKey> {
    match key {
        Some(key_path) => load_secret_key(key_path).await,
        None => {
            // No path provided, just generate one
            Ok(SecretKey::generate())
        }
    }
}

// Makes a an RPC endpoint that uses a QUIC transport
fn make_rpc_endpoint(
    secret_key: &SecretKey,
    rpc_port: u16,
) -> Result<impl ServiceEndpoint<ProviderService>> {
    let rpc_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, rpc_port));
    let rpc_quinn_endpoint = quinn::Endpoint::server(
        iroh::node::make_server_config(
            secret_key,
            MAX_RPC_STREAMS,
            MAX_RPC_CONNECTIONS,
            vec![RPC_ALPN.to_vec()],
        )?,
        rpc_addr,
    )?;
    let rpc_endpoint =
        QuinnServerEndpoint::<ProviderRequest, ProviderResponse>::new(rpc_quinn_endpoint)?;
    Ok(rpc_endpoint)
}
