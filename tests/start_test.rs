#![allow(clippy::new_ret_no_self)]
extern crate atomic_immut;
extern crate bytecodec;
extern crate cannyls;
extern crate cannyls_rpc;
extern crate fibers;
extern crate fibers_http_server;
extern crate fibers_rpc;
extern crate fibers_tasque;
extern crate frugalos;
extern crate frugalos_config;
extern crate frugalos_core;
extern crate frugalos_mds;
extern crate frugalos_raft;
extern crate frugalos_segment;
extern crate futures;
extern crate httpcodec;
extern crate jemalloc_ctl;
extern crate libfrugalos;
extern crate num_cpus;
extern crate prometrics;
extern crate raftlog;
extern crate rustracing;
extern crate rustracing_jaeger;
extern crate serde;
extern crate slog;
extern crate sloggers;
extern crate tempdir;
#[macro_use]
extern crate trackable;

mod common;

use fibers::{Executor, Spawn, ThreadPoolExecutor};
use fibers_rpc::client::{ClientServiceBuilder, ClientServiceHandle};
use frugalos::daemon::FrugalosDaemon;
use frugalos::{Error, FrugalosConfig};
use futures::Future;
use libfrugalos::entity::server::{Server, ServerId};
use slog::Logger;
use sloggers::Build;
use std::fs;
use tempdir::TempDir;

use common::{FrugalosClient, FrugalosClientRegistry};

fn make_loggger() -> Logger {
    let mut builder = sloggers::terminal::TerminalLoggerBuilder::new();
    builder.level(sloggers::types::Severity::Info);
    builder.channel_size(1000);
    track_try_unwrap!(sloggers::LoggerBuilder::Terminal(builder).build())
}

fn spawn_rpc_service<S>(executor: S) -> ClientServiceHandle
where
    S: Spawn + Send + 'static + Clone,
{
    let service = ClientServiceBuilder::new().finish(executor.clone());
    let service_handle = service.handle();
    executor.spawn(service.map_err(|e| panic!("{}", e)));
    service_handle
}

fn spawn_daemon(
    logger: Logger,
    executor: &ThreadPoolExecutor,
    configs: Vec<FrugalosConfig>,
    client_registry: &mut FrugalosClientRegistry,
) {
    let mut contact_server = None;
    for (i, mut config) in configs.into_iter().enumerate() {
        let server_id = i.to_string();
        config.data_dir = track_try_unwrap!(TempDir::new(server_id.as_str()).map_err(Error::from))
            .path()
            .to_str()
            .unwrap()
            .to_owned();

        track_try_unwrap!(fs::create_dir_all(&config.data_dir).map_err(Error::from));

        let server = Server::new(server_id.clone(), config.http_server.bind_addr);
        client_registry.register(&server);

        if i == 0 {
            contact_server = Some(server.addr());
            track_try_unwrap!(frugalos_config::cluster::create(
                &logger,
                server,
                &config.data_dir
            ));
        } else {
            assert!(contact_server.is_some());
            track_try_unwrap!(frugalos_config::cluster::join(
                &logger,
                &server,
                &config.data_dir,
                contact_server.unwrap().clone(),
            ));
        }

        let daemon =
            track_try_unwrap!(FrugalosDaemon::new(&logger, config.clone()).map_err(Error::from));

        executor.spawn_fn(move || {
            daemon
                .run(config.daemon)
                .map_err(move |e| panic!("Error: {}", e))
        });
    }
}

#[test]
fn it_works() {
    let logger = make_loggger();
    let executor = track_try_unwrap!(ThreadPoolExecutor::new().map_err(Error::from));
    let rpc_service = spawn_rpc_service(executor.handle());
    let mut client_registry = FrugalosClientRegistry::new(logger.clone(), rpc_service);
    let mut configs = vec![FrugalosConfig::default(); 3];
    for i in 0..configs.len() {
        configs[i].http_server.bind_addr = format!("0.0.0.0:{}", 3500 + i).parse().unwrap();
    }
    spawn_daemon(logger.clone(), &executor, configs, &mut client_registry)
}
