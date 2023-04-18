use std::net::{Ipv4Addr, SocketAddrV4};
use std::{net::SocketAddr, path::PathBuf, str::FromStr};

use crate::protocol::AuthToken;
use crate::provider::{CustomHandler, Database, Provider};
use crate::rpc_protocol::{ProviderRequest, ProviderResponse, ProviderService};
use crate::{provider, Keypair};
use anyhow::Result;

use quic_rpc::transport::quinn::QuinnServerEndpoint;
use quic_rpc::ServiceEndpoint;

mod util;
pub use crate::porcelain::util::{iroh_data_root, pathbuf_from_name};
pub use util::Blake3Cid;

const MAX_RPC_CONNECTIONS: u32 = 16;
const MAX_RPC_STREAMS: u64 = 1024;
const RPC_ALPN: [u8; 17] = *b"n0/provider-rpc/1";

/// Generates an [`Keypair`], either by reading an existing key file, or by generating a new one.
///
/// Returns a `Result` containing the generated `Keypair`, or an error if one occured while trying
/// to open the file at the given [`PathBuf`].
pub async fn get_keypair(key: Option<PathBuf>) -> Result<Keypair> {
    match key {
        Some(key_path) => {
            if key_path.exists() {
                let keystr = tokio::fs::read(key_path).await?;
                let keypair = Keypair::try_from_openssh(keystr)?;
                Ok(keypair)
            } else {
                let keypair = Keypair::generate();
                let ser_key = keypair.to_openssh()?;
                if let Some(parent) = key_path.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }
                tokio::fs::write(key_path, ser_key).await?;
                Ok(keypair)
            }
        }
        None => {
            // No path provided, just generate one
            Ok(Keypair::generate())
        }
    }
}

/// Construct a Provider
pub async fn provide<C: CustomHandler>(
    db: Database,
    addr: Option<SocketAddr>,
    auth_token: Option<String>,
    key: Option<PathBuf>,
    keylog: bool,
    rpc_port: Option<u16>,
    custom_handler: C,
) -> Result<Provider> {
    let keypair = get_keypair(key).await?;

    let mut builder = provider::Provider::builder(db)
        .keylog(keylog)
        .custom_handler(custom_handler);

    if let Some(addr) = addr {
        builder = builder.bind_addr(addr);
    }
    if let Some(ref encoded) = auth_token {
        let auth_token = AuthToken::from_str(encoded)?;
        builder = builder.auth_token(auth_token);
    }
    let provider = if let Some(rpc_port) = rpc_port {
        let rpc_endpoint = make_rpc_endpoint(&keypair, rpc_port)?;
        builder
            .rpc_endpoint(rpc_endpoint)
            .keypair(keypair)
            .spawn()?
    } else {
        builder.keypair(keypair).spawn()?
    };

    Ok(provider)
}

/// Print the listening addresses, the [`PeerId`], and the autho token for this provider
///
/// This should be called after the [`Provider`] has already connected, otherwise the listening address
/// will not be accurate.
///
/// Can error if there are issues getting the listening address.
pub fn display_provider_info(provider: &Provider) -> anyhow::Result<()> {
    let addrs = provider.listen_addresses()?;
    println!("Listening addresses:");
    for addr in addrs {
        println!("\t{addr}");
    }
    println!("PeerID: {}", provider.peer_id());
    println!("Auth token: {}", provider.auth_token());
    println!();
    Ok(())
}

fn make_rpc_endpoint(
    keypair: &Keypair,
    rpc_port: u16,
) -> Result<impl ServiceEndpoint<ProviderService>> {
    let rpc_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, rpc_port));
    let rpc_quinn_endpoint = quinn::Endpoint::server(
        provider::make_server_config(
            keypair,
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
