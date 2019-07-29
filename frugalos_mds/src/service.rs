use atomic_immut::AtomicImmut;
use fibers::sync::mpsc;
use fibers_rpc::server::ServerBuilder as RpcServerBuilder;
use frugalos_core::tracer::ThreadLocalTracer;
use frugalos_raft::{LocalNodeId, NodeId};
use futures::{Async, Future, Poll, Stream};
use slog::Logger;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use node::NodeHandle;
use server::Server;
use {Error, Result};

type Nodes = Arc<AtomicImmut<HashMap<LocalNodeId, NodeHandle>>>;

/// MDS用のサービスを表す`Future`実装.
///
/// MDSノードの管理やRPC要求の処理等を担当する.
///
/// 一つのサーバ(HTTPサーバ)につき、一つのサービスインスタンスが起動していることを想定.
#[derive(Debug)]
pub struct Service {
    logger: Logger,
    nodes: Nodes,
    stopped_nodes: Arc<AtomicImmut<HashSet<LocalNodeId>>>,
    command_tx: mpsc::Sender<Command>,
    command_rx: mpsc::Receiver<Command>,
    do_stop: Arc<AtomicBool>,
}
impl Service {
    /// 新しい`Service`インスタンスを生成する.
    pub fn new(
        logger: Logger,
        rpc: &mut RpcServerBuilder,
        tracer: ThreadLocalTracer,
    ) -> Result<Self> {
        let nodes = Arc::new(AtomicImmut::new(HashMap::new()));
        let stopped_nodes = Arc::new(AtomicImmut::new(HashSet::new()));
        let (command_tx, command_rx) = mpsc::channel();
        let this = Service {
            logger,
            nodes,
            stopped_nodes,
            command_tx,
            command_rx,
            do_stop: Arc::new(AtomicBool::new(false)),
        };
        Server::register(this.handle(), rpc, tracer);
        Ok(this)
    }

    /// `Service`を操作するためのハンドルを返す.
    pub fn handle(&self) -> ServiceHandle {
        ServiceHandle {
            nodes: self.nodes.clone(),
            command_tx: self.command_tx.clone(),
            do_stop: self.do_stop.clone(),
        }
    }

    /// サービスを停止する.
    ///
    /// サービス停止前には、全てのローカルノードでスナップショットが取得される.
    pub fn stop(&mut self) {
        self.do_stop.store(true, Ordering::SeqCst);
        for (id, node) in self.nodes.load().iter() {
            info!(self.logger, "Sends stop request: {:?}", id);
            node.stop();
        }
    }

    /// スナップショットを取得する.
    pub fn take_snapshot(&mut self) {
        self.do_stop.store(true, Ordering::SeqCst);
        for (id, node) in self.nodes.load().iter() {
            info!(self.logger, "Sends taking snapshot request: {:?}", id);
            node.take_snapshot();
        }
    }

    fn handle_command(&mut self, command: Command) {
        match command {
            Command::AddNode(id, node) => {
                if self.do_stop.load(Ordering::SeqCst) {
                    warn!(self.logger, "Ignored: id={:?}, node={:?}", id, node);
                    return;
                }
                info!(self.logger, "Adds node: id={:?}, node={:?}", id, node);

                let mut nodes = (&*self.nodes.load()).clone();
                nodes.insert(id, node);
                self.nodes.store(nodes);
            }
            Command::StopNode(id) => {
                let mut nodes = (&*self.stopped_nodes.load()).clone();
                let removed = nodes.insert(id);
                let len = nodes.len();
                self.stopped_nodes.store(nodes);

                info!(
                    self.logger,
                    "Stops node: id={:?}, node={:?} (len={})", id, removed, len
                );

                if self.stopped_nodes.load().len() == self.nodes.load().len() {
                    info!(
                        self.logger,
                        "Exit node: id={:?}, node={:?} (len={})", id, removed, len
                    );
                    for (_, node) in self.nodes.load().iter() {
                        node.exit();
                    }
                }
            }
            Command::RemoveNode(id) => {
                let mut nodes = (&*self.nodes.load()).clone();
                let removed = nodes.remove(&id);
                let len = nodes.len();
                self.nodes.store(nodes);

                info!(
                    self.logger,
                    "Removes node: id={:?}, node={:?} (len={})", id, removed, len
                );
            }
        }
    }
}
impl Future for Service {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            let polled = self.command_rx.poll().expect("Never fails");
            if let Async::Ready(command) = polled {
                let command = command.expect("Unreachable");
                self.handle_command(command);
                if self.do_stop.load(Ordering::SeqCst) && self.nodes.load().is_empty() {
                    return Ok(Async::Ready(()));
                }
            } else {
                return Ok(Async::NotReady);
            }
        }
    }
}

#[derive(Debug)]
enum Command {
    AddNode(LocalNodeId, NodeHandle),
    RemoveNode(LocalNodeId),
    StopNode(LocalNodeId),
}

/// `Service`を操作するためのハンドル.
///
/// `Service`に対する操作はクレート内で閉じているため、
/// 利用者に公開されているメソッドは存在しない.
#[derive(Debug, Clone)]
pub struct ServiceHandle {
    nodes: Nodes,
    command_tx: mpsc::Sender<Command>,
    do_stop: Arc<AtomicBool>,
}
impl ServiceHandle {
    pub(crate) fn add_node(&self, id: NodeId, node: NodeHandle) -> Result<()> {
        let command = Command::AddNode(id.local_id, node);
        track!(
            self.command_tx.send(command).map_err(Error::from),
            "id={:?}",
            id
        )?;
        Ok(())
    }
    pub(crate) fn stop_node(&self, id: NodeId) -> Result<()> {
        let command = Command::StopNode(id.local_id);
        track!(
            self.command_tx.send(command).map_err(Error::from),
            "id={:?}",
            id
        )?;
        Ok(())
    }
    pub(crate) fn remove_node(&self, id: NodeId) -> Result<()> {
        let command = Command::RemoveNode(id.local_id);
        track!(
            self.command_tx.send(command).map_err(Error::from),
            "id={:?}",
            id
        )?;
        Ok(())
    }
    pub(crate) fn get_node(&self, local_id: LocalNodeId) -> Option<NodeHandle> {
        self.nodes().get(&local_id).cloned()
    }
    pub(crate) fn nodes(&self) -> Arc<HashMap<LocalNodeId, NodeHandle>> {
        self.nodes.load()
    }
    pub(crate) fn is_stopping(&self) -> bool {
        self.do_stop.load(Ordering::SeqCst)
    }
}
