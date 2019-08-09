use atomic_immut::AtomicImmut;
use fibers::sync::{mpsc, oneshot};
use fibers_rpc::server::ServerBuilder as RpcServerBuilder;
use frugalos_core::tracer::ThreadLocalTracer;
use frugalos_raft::{LocalNodeId, NodeId};
use futures::{Async, Future, Poll, Stream};
use slog::Logger;
use std::collections::HashMap;
use std::mem;
use std::sync::Arc;

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
    command_tx: mpsc::Sender<Command>,
    command_rx: mpsc::Receiver<Command>,
    do_stop: bool,
    stopping: Option<futures::SelectAll<oneshot::Monitor<(), Error>>>,
}
impl Service {
    /// 新しい`Service`インスタンスを生成する.
    pub fn new(
        logger: Logger,
        rpc: &mut RpcServerBuilder,
        tracer: ThreadLocalTracer,
    ) -> Result<Self> {
        let nodes = Arc::new(AtomicImmut::new(HashMap::new()));
        let (command_tx, command_rx) = mpsc::channel();
        let this = Service {
            logger,
            nodes,
            command_tx,
            command_rx,
            do_stop: false,
            stopping: None,
        };
        Server::register(this.handle(), rpc, tracer);
        Ok(this)
    }

    /// `Service`を操作するためのハンドルを返す.
    pub fn handle(&self) -> ServiceHandle {
        ServiceHandle {
            nodes: self.nodes.clone(),
            command_tx: self.command_tx.clone(),
        }
    }

    /// サービスを停止する.
    ///
    /// サービス停止前には、全てのローカルノードでスナップショットが取得される.
    ///
    /// # サービスの停止時に考慮すべきこと
    ///
    /// (a) すべてのノードが停止するまでに発生するノードの取得エラー(存在しない
    /// ノードへの参照)を減らすこと.
    ///
    /// (b) 停止時のノードのスナップショット取得以降に `LogSuffix` を伸ばしすぎ
    /// ないこと. 次回起動に時間がかかるようになってしまうため.
    ///
    /// # ノードを順次停止することの問題点
    ///
    /// 旧実装のようにスナップショットを取得し終えたノードから順次停止して
    /// いくと存在しないノードに対するリクエストが発生しやすくなる.なぜなら、
    /// MDS に RPC 呼び出しをするクライアント側は RPC サーバが停止するまでは
    /// 停止済みのノードに対するリクエストを送り続けてくるからである.
    ///
    /// 特にここで問題となるのは、ノードがすべて停止するまでの間に発生する、
    /// 停止済みのノードの取得失敗によるエラーであり、この状況ではクライアント
    /// 側のリトライで状況が改善しないため実質的にリトライが意味をなさない.
    ///
    /// # 停止時のエラーを極力抑える新実装
    ///
    /// 上述の問題を避けるためにサービスの停止処理を以下の2段階に分ける.
    ///
    /// 1. スナップショットの取得
    /// 2. 1 がすべてのノードで完了するまで待ち合わせてからノードを停止
    ///
    /// 1 で `Node` の状態が `Stopping` に変更され、スナップショットの取得
    /// もされる.スナップショットの取得が完了した際にそれを `Service` に
    /// `Monitored` 経由で通知する.
    ///
    /// すべてのノードがスナップショットを取得したら(あるいは、スキップ)、
    /// `Request::Exit` を `Node` に送り `Node` の状態を `Stopped` に変更
    /// する.
    ///
    /// この実装と Leader ノード以外もリクエストに応答できるようにする変更
    /// を組み合わせることで停止時のエラーを減らすことが可能になっている.
    ///
    /// # 新実装のデメリット
    ///
    /// (b) について、スナップショットの取得に時間がかかる環境では `LogSuffix`
    /// が伸びて、スナップショット取得の効果が薄れてしまうこと.許容する.
    pub fn stop(&mut self) {
        self.do_stop = true;
        let mut stopping = Vec::new();
        for (id, node) in self.nodes.load().iter() {
            info!(self.logger, "Sends stop request: {:?}", id);
            let (monitored, monitor) = oneshot::monitor();
            stopping.push(monitor);
            node.stop(monitored);
        }
        self.stopping = Some(futures::select_all(stopping));
    }

    /// スナップショットを取得する.
    pub fn take_snapshot(&mut self) {
        self.do_stop = true;
        for (id, node) in self.nodes.load().iter() {
            info!(self.logger, "Sends taking snapshot request: {:?}", id);
            node.take_snapshot();
        }
    }

    fn exit(&mut self) {
        for (id, node) in self.nodes.load().iter() {
            info!(self.logger, "Sends exit request: {:?}", id);
            node.exit();
        }
    }

    fn handle_command(&mut self, command: Command) {
        match command {
            Command::AddNode(id, node) => {
                if self.do_stop {
                    warn!(self.logger, "Ignored: id={:?}, node={:?}", id, node);
                    return;
                }
                info!(self.logger, "Adds node: id={:?}, node={:?}", id, node);

                let mut nodes = (&*self.nodes.load()).clone();
                nodes.insert(id, node);
                self.nodes.store(nodes);
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
            loop {
                match mem::replace(&mut self.stopping, None) {
                    None => break,
                    Some(mut future) => {
                        let remainings = match future.poll() {
                            Err((e, _, remainings)) => {
                                warn!(self.logger, "{:?}", e);
                                remainings
                            }
                            Ok(Async::Ready(((), _, remainings))) => {
                                info!(self.logger, "remaing: {}", remainings.len());
                                remainings
                            }
                            Ok(Async::NotReady) => {
                                self.stopping = Some(future);
                                break;
                            }
                        };
                        if remainings.is_empty() {
                            self.exit();
                            break;
                        }
                        self.stopping = Some(futures::select_all(remainings));
                    }
                }
            }
            let polled = self.command_rx.poll().expect("Never fails");
            if let Async::Ready(command) = polled {
                let command = command.expect("Unreachable");
                self.handle_command(command);
                if self.do_stop && self.nodes.load().is_empty() {
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
}

/// `Service`を操作するためのハンドル.
///
/// `Service`に対する操作はクレート内で閉じているため、
/// 利用者に公開されているメソッドは存在しない.
#[derive(Debug, Clone)]
pub struct ServiceHandle {
    nodes: Nodes,
    command_tx: mpsc::Sender<Command>,
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
}
