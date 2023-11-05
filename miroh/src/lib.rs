use std::{
    collections::BTreeMap,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result};
use clap::Parser;
use derive_more::{Display, From, FromStr};
use iroh::{
    bytes::util::{runtime, RpcError},
    client::quic::create_quinn_client,
    net::{
        defaults::default_derp_map,
        derp::{DerpMap, DerpMode},
        key::{PublicKey, SecretKey},
    },
    node::{Node, StaticTokenAuthHandler},
    rpc_protocol::{NodeStatusRequest, ProviderRequest, ProviderResponse},
    util::{fs::load_secret_key, path::IrohPaths},
};
use quic_rpc::message::RpcMsg;
use quic_rpc::server::RpcChannel;
use quic_rpc::transport::quinn::{QuinnConnection, QuinnServerEndpoint};
use quic_rpc::{RpcServer, Service, ServiceEndpoint};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::warn;

const DEFAULT_RPC_PORT: u16 = 0x1338;
const MAX_RPC_CONNECTIONS: u32 = 16;
const MAX_RPC_STREAMS: u64 = 1024;

#[derive(Parser, Debug, Clone)]
#[clap(version, verbatim_doc_comment)]
pub struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Parser, Debug, Clone)]
enum Commands {
    Start {
        /// Number of nodes to start.
        #[clap(long)]
        nodes: usize,
        /// Root path of where the data will be stored.
        #[clap(long)]
        root: PathBuf,
    },
    Run {
        /// The node to execute on. If not set executes on all.
        #[clap(long)]
        node: Option<PublicKey>,
        /// The command to execute
        command: RpcCommand,
    },
}

#[derive(Debug, Clone, Display, FromStr)]
enum RpcCommand {
    Status,
}

#[derive(Default, Clone)]
struct Nodes {
    nodes: Arc<RwLock<BTreeMap<PublicKey, NodeWrapper>>>,
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

                let managed_nodes = Nodes::default();
                let mut nodes_lock = managed_nodes.nodes.write().await;
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
                    nodes_lock.insert(node_id, NodeWrapper { i, node, client });
                }
                drop(nodes_lock);

                let secret_key = get_secret_key(Some(root.join("keys"))).await?;
                let rpc_endpoint = make_rpc_endpoint(&secret_key, DEFAULT_RPC_PORT)?;
                let rpc_server = RpcServer::new(rpc_endpoint);

                // TODO: wait for ctrl messages via rpc
                loop {
                    tokio::select! {
                        biased;
                        _ = tokio::signal::ctrl_c() => {
                            println!("Shutting down nodes...");
                            let mut nodes_lock = managed_nodes.nodes.write().await;
                            let nodes = std::mem::take(&mut *nodes_lock);
                            for (id, node) in nodes.into_iter() {
                                println!("{} shutting down...", id);
                                node.client.node.shutdown(false).await?;
                            }

                            return Ok(());
                        }
                        request = rpc_server.accept() => {
                            match request {
                                Ok((msg, chan)) => {
                                    handle_rpc_request(msg, chan, &managed_nodes, &rt);
                                }
                                Err(e) => {
                                    warn!("rpc request error: {:?}", e);
                                }
                            }
                        }
                    }
                }
            }
            Commands::Run { node, command } => {
                let client = connect_raw(DEFAULT_RPC_PORT)
                    .await
                    .context("failed to connect RPC")?;
                match command {
                    RpcCommand::Status => {
                        let res = client
                            .rpc(MirohRequest {
                                node,
                                request: NodeStatusRequest.into(),
                            })
                            .await?;
                        let MirohResponse { responses } = res;
                        for (id, response) in responses {
                            println!("{}: \n{:#?}", id, response);
                        }
                    }
                }
                return Ok(());
            }
        }
    }
}

const RPC_ALPN: [u8; 14] = *b"n0/miroh-rpc/1";
type RpcClient = quic_rpc::RpcClient<MirohService, QuinnConnection<MirohResponse, MirohRequest>>;

async fn connect_raw(rpc_port: u16) -> anyhow::Result<RpcClient> {
    let bind_addr = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0).into();
    let endpoint = create_quinn_client(bind_addr, vec![RPC_ALPN.to_vec()], false)?;
    let addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), rpc_port);
    let server_name = "localhost".to_string();
    let connection = QuinnConnection::new(endpoint, addr, server_name);
    let client = RpcClient::new(connection);
    // Do a status request to check if the server is running.
    let _version = tokio::time::timeout(
        Duration::from_secs(1),
        client.rpc(MirohRequest {
            node: None,
            request: NodeStatusRequest.into(),
        }),
    )
    .await
    .context("mIroh node is not running")??;
    Ok(client)
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
) -> Result<impl ServiceEndpoint<MirohService>> {
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
    let rpc_endpoint = QuinnServerEndpoint::<MirohRequest, MirohResponse>::new(rpc_quinn_endpoint)?;
    Ok(rpc_endpoint)
}

#[derive(Debug, Serialize, Deserialize)]
struct MirohRequest {
    /// If None, request is for all nodes.
    node: Option<PublicKey>,
    request: ProviderRequest,
}

impl RpcMsg<MirohService> for MirohRequest {
    type Response = MirohResponse;
}

#[derive(Debug, Serialize, Deserialize)]
struct MirohResponse {
    responses: BTreeMap<PublicKey, Result<ProviderResponse, RpcError>>,
}

#[derive(Debug, Clone)]
struct MirohService;

impl Service for MirohService {
    type Req = MirohRequest;
    type Res = MirohResponse;
}

fn handle_rpc_request<E: ServiceEndpoint<MirohService>>(
    msg: MirohRequest,
    chan: RpcChannel<MirohService, E>,
    nodes: &Nodes,
    rt: &runtime::Handle,
) {
    let nodes = nodes.clone();
    rt.main().spawn(async move {
        match msg.node {
            None => {
                // TODO: streaming
                chan.rpc(msg, nodes, |nodes, req| async move {
                    let mut responses = BTreeMap::new();
                    let nodes_lock = nodes.nodes.read().await;
                    for (id, node) in &*nodes_lock {
                        let res = node
                            .send_to_client(req.request.clone())
                            .await
                            .map_err(Into::into);
                        responses.insert(*id, res);
                    }
                    MirohResponse { responses }
                })
                .await
            }
            Some(id) => {
                chan.rpc(msg, nodes, |nodes, req| async move {
                    let mut responses = BTreeMap::new();
                    let nodes_lock = nodes.nodes.read().await;
                    let node = nodes_lock.get(&id).unwrap(); // TODO: handle error
                    let res = node.send_to_client(req.request).await.map_err(Into::into);
                    responses.insert(id, res);
                    MirohResponse { responses }
                })
                .await
            }
        }
    });
}

impl NodeWrapper {
    async fn send_to_client(&self, msg: ProviderRequest) -> anyhow::Result<ProviderResponse> {
        let client = &self.client.node.rpc;
        match msg {
            ProviderRequest::NodeStatus(val) => {
                let res = client.rpc(val).await?;
                Ok(res.into())
            }
            ProviderRequest::NodeStats(val) => {
                let res = client.rpc(val).await?;
                Ok(res.into())
            }
            ProviderRequest::NodeShutdown(val) => {
                let res = client.rpc(val).await?;
                Ok(res.into())
            }
            ProviderRequest::NodeConnections(val) => {
                todo!()
                // let res = client.rpc(val).await?;
                // Ok(res.into())
            }
            ProviderRequest::NodeConnectionInfo(val) => {
                let res = client.rpc(val).await?;
                Ok(res.into())
            }
            ProviderRequest::NodeWatch(val) => {
                todo!()
                // let res = client.rpc(val).await.map(Into::into)?;
                // Ok(res.into())
            }
            ProviderRequest::BlobRead(val) => {
                todo!()
                // let res = client.rpc(val).await?;
                // Ok(res.into())
            }
            ProviderRequest::BlobAddStream(val) => {
                todo!()
                // let res = client.rpc(val).await?;
                // Ok(res.into())
            }
            ProviderRequest::BlobAddStreamUpdate(val) => {
                todo!()
                // let res = client.rpc(val).await?;
                // Ok(res.into())
            }
            ProviderRequest::BlobAddPath(val) => {
                todo!()
                // let res = client.rpc(val).await?;
                // Ok(res.into())
            }
            ProviderRequest::BlobDownload(val) => {
                todo!()
                // let res = client.rpc(val).await?;
                // Ok(res.into())
            }
            ProviderRequest::BlobList(val) => {
                todo!()
                // let res = client.rpc(val).await?;
                // Ok(res.into())
            }
            ProviderRequest::BlobListIncomplete(val) => {
                todo!()
                // let res = client.rpc(val).await?;
                // Ok(res.into())
            }
            ProviderRequest::BlobListCollections(val) => {
                todo!()
                // let res = client.rpc(val).await?;
                // Ok(res.into())
            }
            ProviderRequest::BlobDeleteBlob(val) => {
                let res = client.rpc(val).await?;
                Ok(res.into())
            }
            ProviderRequest::BlobValidate(val) => {
                todo!()
                // let res = client.rpc(val).await?;
                // Ok(res.into())
            }
            ProviderRequest::DeleteTag(val) => {
                let res = client.rpc(val).await?;
                Ok(res.into())
            }
            ProviderRequest::ListTags(val) => {
                todo!()
                // let res = client.rpc(val).await?;
                // Ok(res.into())
            }
            ProviderRequest::DocOpen(val) => {
                let res = client.rpc(val).await?;
                Ok(res.into())
            }
            ProviderRequest::DocClose(val) => {
                let res = client.rpc(val).await?;
                Ok(res.into())
            }
            ProviderRequest::DocStatus(val) => {
                let res = client.rpc(val).await?;
                Ok(res.into())
            }
            ProviderRequest::DocList(val) => {
                todo!()
                // let res = client.rpc(val).await?;
                // Ok(res.into())
            }
            ProviderRequest::DocCreate(val) => {
                let res = client.rpc(val).await?;
                Ok(res.into())
            }
            ProviderRequest::DocDrop(val) => {
                let res = client.rpc(val).await?;
                Ok(res.into())
            }
            ProviderRequest::DocImport(val) => {
                let res = client.rpc(val).await?;
                Ok(res.into())
            }
            ProviderRequest::DocSet(val) => {
                let res = client.rpc(val).await?;
                Ok(res.into())
            }
            ProviderRequest::DocSetHash(val) => {
                let res = client.rpc(val).await?;
                Ok(res.into())
            }
            ProviderRequest::DocGet(val) => {
                todo!()
                // let res = client.rpc(val).await?;
                // Ok(res.into())
            }
            ProviderRequest::DocGetOne(val) => {
                let res = client.rpc(val).await?;
                Ok(res.into())
            }
            ProviderRequest::DocDel(val) => {
                let res = client.rpc(val).await?;
                Ok(res.into())
            }
            ProviderRequest::DocStartSync(val) => {
                let res = client.rpc(val).await?;
                Ok(res.into())
            }
            ProviderRequest::DocLeave(val) => {
                let res = client.rpc(val).await?;
                Ok(res.into())
            }
            ProviderRequest::DocShare(val) => {
                let res = client.rpc(val).await?;
                Ok(res.into())
            }
            ProviderRequest::DocSubscribe(val) => {
                todo!()
                // let res = client.rpc(val).await?;
                // Ok(res.into())
            }
            ProviderRequest::AuthorList(val) => {
                todo!()
                // let res = client.rpc(val).await?;
                // Ok(res.into())
            }
            ProviderRequest::AuthorCreate(val) => {
                let res = client.rpc(val).await?;
                Ok(res.into())
            }
            ProviderRequest::AuthorImport(val) => {
                let res = client.rpc(val).await?;
                Ok(res.into())
            }
        }
    }
}
