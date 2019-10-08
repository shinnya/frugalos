use cannyls::device::DeviceHandle;
use fibers::time::timer::{self, Timeout};
use frugalos_mds::Event;
use frugalos_raft::NodeId;
use futures::{Async, Future, Poll, Stream};
use libfrugalos::entity::object::ObjectVersion;
use libfrugalos::repair::RepairIdleness;
use prometrics::metrics::{Counter, MetricBuilder};
use slog::Logger;
use std::cmp::{self, Reverse};
use std::collections::{BTreeSet, BinaryHeap, VecDeque};
use std::time::{Duration, Instant, SystemTime};

use client::storage::StorageClient;
use delete::DeleteContent;
use repair::{RepairContent, RepairMetrics, RepairPrepContent};
use segment_gc::{SegmentGc, SegmentGcMetrics};
use service::{RepairLock, ServiceHandle};
use std::convert::Infallible;
use Error;

const MAX_TIMEOUT_SECONDS: u64 = 60;
const DELETE_CONCURRENCY: usize = 16;

// TODO: 起動直後の確認は`device.list()`の結果を使った方が効率的
pub struct Synchronizer {
    logger: Logger,
    node_id: NodeId,
    device: DeviceHandle,
    client: StorageClient,
    segment_gc_metrics: SegmentGcMetrics,
    segment_gc: Option<SegmentGc>,
    segment_gc_step: u64,

    // general-purpose queue.
    general_queue: GeneralQueueExecutor,
    // repair-only queue.
    repair_queue: RepairQueueExecutor,
}
impl Synchronizer {
    pub fn new(
        logger: Logger,
        node_id: NodeId,
        device: DeviceHandle,
        service_handle: ServiceHandle,
        client: StorageClient,
        segment_gc_step: u64,
    ) -> Self {
        let metric_builder = MetricBuilder::new()
            .namespace("frugalos")
            .subsystem("synchronizer")
            .label("node", &node_id.to_string())
            .clone();
        // Metrics related to queue length
        let enqueued_repair = metric_builder
            .counter("enqueued_items")
            .label("type", "repair")
            .finish()
            .expect("metric should be well-formed");
        let enqueued_delete = metric_builder
            .counter("enqueued_items")
            .label("type", "delete")
            .finish()
            .expect("metric should be well-formed");
        let dequeued_repair = metric_builder
            .counter("dequeued_items")
            .label("type", "repair")
            .finish()
            .expect("metric should be well-formed");
        let dequeued_delete = metric_builder
            .counter("dequeued_items")
            .label("type", "delete")
            .finish()
            .expect("metric should be well-formed");

        let general_queue = GeneralQueueExecutor::new(
            &logger,
            node_id,
            &device,
            &enqueued_repair,
            &enqueued_delete,
            &dequeued_repair,
            &dequeued_delete,
        );
        let repair_queue = RepairQueueExecutor::new(
            &logger,
            node_id,
            &device,
            &client,
            &service_handle,
            &metric_builder,
        );
        Synchronizer {
            logger,
            node_id,
            device,
            client,
            segment_gc_metrics: SegmentGcMetrics::new(&metric_builder),
            segment_gc: None,
            segment_gc_step,

            general_queue,
            repair_queue,
        }
    }
    pub fn handle_event(&mut self, event: &Event) {
        debug!(
            self.logger,
            "New event: {:?} (metadata={})",
            event,
            self.client.is_metadata(),
        );
        if !self.client.is_metadata() {
            match *event {
                Event::Putted { .. } => {
                    self.general_queue.push(event);
                }
                Event::Deleted { .. } => {
                    self.general_queue.push(event);
                }
                // Because pushing FullSync into the task queue causes difficulty in implementation,
                // we decided not to push this task to the task priority queue and handle it manually.
                Event::FullSync {
                    ref machine,
                    next_commit,
                } => {
                    // If FullSync is not being processed now, this event lets the synchronizer to handle one.
                    if self.segment_gc.is_none() {
                        self.segment_gc = Some(SegmentGc::new(
                            &self.logger,
                            self.node_id,
                            &self.device,
                            machine.clone(),
                            ObjectVersion(next_commit.as_u64()),
                            self.segment_gc_metrics.clone(),
                            self.segment_gc_step,
                        ));
                    }
                }
            }
        }
    }
    pub(crate) fn set_repair_idleness_threshold(
        &mut self,
        repair_idleness_threshold: RepairIdleness,
    ) {
        self.repair_queue
            .set_repair_idleness_threshold(repair_idleness_threshold);
    }
}
impl Future for Synchronizer {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        while let Async::Ready(Some(())) = self.segment_gc.poll().unwrap_or_else(|e| {
            warn!(self.logger, "Task failure: {}", e);
            Async::Ready(Some(()))
        }) {
            // Full sync is done. Clearing the segment_gc field.
            self.segment_gc = None;
            self.segment_gc_metrics.reset();
        }

        if let Async::Ready(Some(version)) = self.general_queue.poll().unwrap_or_else(|e| {
            warn!(self.logger, "Task failure in general_queue: {}", e);
            Async::Ready(None)
        }) {
            self.repair_queue.push(version);
        }

        // Never stops, never fails.
        self.repair_queue.poll().unwrap_or_else(Into::into);
        Ok(Async::NotReady)
    }
}

#[derive(Debug, PartialOrd, Ord, PartialEq, Eq)]
enum TodoItem {
    RepairContent {
        start_time: SystemTime,
        version: ObjectVersion,
    },
    DeleteContent {
        versions: Vec<ObjectVersion>,
    },
}
impl TodoItem {
    pub fn new(event: &Event) -> Self {
        match *event {
            Event::Deleted { version } => TodoItem::DeleteContent {
                versions: vec![version],
            },
            Event::Putted {
                version,
                put_content_timeout,
            } => {
                // Wait for put_content_timeout.0 seconds, to avoid race condition with storage.put.
                let start_time = SystemTime::now() + Duration::from_secs(put_content_timeout.0);
                TodoItem::RepairContent {
                    start_time,
                    version,
                }
            }
            Event::FullSync { .. } => unreachable!(),
        }
    }
    pub fn wait_time(&self) -> Option<Duration> {
        match *self {
            TodoItem::DeleteContent { .. } => None,
            TodoItem::RepairContent { start_time, .. } => {
                start_time.duration_since(SystemTime::now()).ok()
            }
        }
    }
}

#[allow(clippy::large_enum_variant)]
enum Task {
    Idle,
    Wait(Timeout),
    Delete(DeleteContent),
    Repair(RepairContent, RepairLock),
    RepairPrep(RepairPrepContent),
}
impl Task {
    fn is_sleeping(&self) -> bool {
        match self {
            Task::Idle => true,
            Task::Wait(_) => true,
            _ => false,
        }
    }
}
impl Future for Task {
    type Item = Option<ObjectVersion>;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match *self {
            Task::Idle => Ok(Async::Ready(None)),
            Task::Wait(ref mut f) => track!(f
                .poll()
                .map_err(Error::from)
                .map(|async| async.map(|()| None))),
            Task::Delete(ref mut f) => track!(f
                .poll()
                .map_err(Error::from)
                .map(|async| async.map(|()| None))),
            Task::Repair(ref mut f, _) => track!(f
                .poll()
                .map_err(Error::from)
                .map(|async| async.map(|()| None))),
            Task::RepairPrep(ref mut f) => track!(f.poll()),
        }
    }
}

/// RepairPrep, Delete タスクの管理と、その処理を行う。
struct GeneralQueueExecutor {
    logger: Logger,
    node_id: NodeId,
    device: DeviceHandle,
    repair_prep_queue: RepairPrepQueue,
    delete_queue: DeleteQueue,
    task: Task,
    repair_candidates: BTreeSet<ObjectVersion>,
}

impl GeneralQueueExecutor {
    fn new(
        logger: &Logger,
        node_id: NodeId,
        device: &DeviceHandle,
        enqueued_repair: &Counter,
        enqueued_delete: &Counter,
        dequeued_repair: &Counter,
        dequeued_delete: &Counter,
    ) -> Self {
        Self {
            logger: logger.clone(),
            node_id,
            device: device.clone(),
            repair_prep_queue: RepairPrepQueue::new(enqueued_repair, dequeued_repair),
            delete_queue: DeleteQueue::new(enqueued_delete, dequeued_delete),
            task: Task::Idle,
            repair_candidates: BTreeSet::new(),
        }
    }
    fn push(&mut self, event: &Event) {
        match *event {
            Event::Putted { version, .. } => {
                self.repair_prep_queue.push(TodoItem::new(event));
                self.repair_candidates.insert(version);
            }
            Event::Deleted { version } => {
                self.repair_candidates.remove(&version);
                self.delete_queue.push(version);
            }
            Event::FullSync { .. } => {
                unreachable!();
            }
        }
    }
    fn pop(&mut self) -> Option<TodoItem> {
        // assert!(self.task == Task::Idle);
        if let Task::Idle = self.task {
        } else {
            unreachable!("self.task != Task::Idle");
        }
        let item = loop {
            // Repair has priority higher than deletion. repair_prep_queue should be examined first.
            let maybe_item = if let Some(item) = self.repair_prep_queue.pop() {
                Some(item)
            } else {
                self.delete_queue.pop()
            };
            if let Some(item) = maybe_item {
                if let TodoItem::RepairContent { version, .. } = item {
                    if !self.repair_candidates.contains(&version) {
                        // 既に削除済み
                        continue;
                    }
                }
                break item;
            } else {
                return None;
            }
        };
        if let Some(duration) = item.wait_time() {
            // NOTE: `assert_eq!(self.task, Task::Idel)`

            let duration = cmp::min(duration, Duration::from_secs(MAX_TIMEOUT_SECONDS));
            self.task = Task::Wait(timer::timeout(duration));
            self.repair_prep_queue.push(item);

            // NOTE:
            // 同期処理が少し遅れても全体としては大きな影響はないので、
            // 一度Wait状態に入った後に、開始時間がより近いアイテムが入って来たとしても、
            // 古いTimeoutをキャンセルしたりはしない.
            //
            // 仮に`put_content_timeout`が極端に長いイベントが発生したとしても、
            // `MAX_TIMEOUT_SECONDS`以上に後続のTODOの処理が(Waitによって)遅延することはない.
            None
        } else {
            Some(item)
        }
    }
}

impl Stream for GeneralQueueExecutor {
    type Item = ObjectVersion;
    type Error = Infallible;
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        while let Async::Ready(result) = self.task.poll().unwrap_or_else(|e| {
            // 同期処理のエラーは致命的ではないので、ログを出すだけに留める
            warn!(self.logger, "Task failure: {}", e);
            Async::Ready(None)
        }) {
            self.task = Task::Idle;
            if let Some(version) = result {
                return Ok(Async::Ready(Some(version)));
            }
            if let Some(item) = self.pop() {
                match item {
                    TodoItem::DeleteContent { versions } => {
                        self.task = Task::Delete(DeleteContent::new(
                            &self.logger,
                            &self.device,
                            self.node_id,
                            versions,
                        ));
                    }
                    TodoItem::RepairContent { version, .. } => {
                        self.task = Task::RepairPrep(RepairPrepContent::new(
                            &self.logger,
                            &self.device,
                            self.node_id,
                            version,
                        ));
                    }
                }
            } else if let Task::Idle = self.task {
                break;
            }
        }
        Ok(Async::NotReady)
    }
}

/// 若い番号のオブジェクトから順番にリペアするためのキュー。
struct RepairQueueExecutor {
    logger: Logger,
    node_id: NodeId,
    device: DeviceHandle,
    client: StorageClient,
    service_handle: ServiceHandle,
    task: Task,
    queue: BinaryHeap<Reverse<ObjectVersion>>,
    // The idleness threshold for repair functionality.
    repair_idleness_threshold: RepairIdleness,
    last_not_idle: Instant,
    repair_metrics: RepairMetrics,
}
impl RepairQueueExecutor {
    fn new(
        logger: &Logger,
        node_id: NodeId,
        device: &DeviceHandle,
        client: &StorageClient,
        service_handle: &ServiceHandle,
        metric_builder: &MetricBuilder,
    ) -> Self {
        RepairQueueExecutor {
            logger: logger.clone(),
            node_id,
            device: device.clone(),
            client: client.clone(),
            service_handle: service_handle.clone(),
            task: Task::Idle,
            queue: BinaryHeap::new(),
            repair_idleness_threshold: RepairIdleness::Disabled,
            last_not_idle: Instant::now(),
            repair_metrics: RepairMetrics::new(metric_builder),
        }
    }
    fn push(&mut self, version: ObjectVersion) {
        self.queue.push(Reverse(version));
    }
    fn pop(&mut self) -> Option<ObjectVersion> {
        let result = self.queue.pop();
        // Shrink if necessary
        if self.queue.capacity() > 32 && self.queue.len() < self.queue.capacity() / 2 {
            self.queue.shrink_to_fit();
        }
        result.map(|version| version.0)
    }
    fn set_repair_idleness_threshold(&mut self, repair_idleness_threshold: RepairIdleness) {
        info!(
            self.logger,
            "repair_idleness_threshold set to {:?}", repair_idleness_threshold,
        );
        self.repair_idleness_threshold = repair_idleness_threshold;
    }
}
impl Future for RepairQueueExecutor {
    type Item = Infallible; // This executor will never finish normally.
    type Error = Infallible;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if !self.task.is_sleeping() {
            self.last_not_idle = Instant::now();
            debug!(self.logger, "last_not_idle = {:?}", self.last_not_idle);
        }

        while let Async::Ready(_result) = self.task.poll().unwrap_or_else(|e| {
            // 同期処理のエラーは致命的ではないので、ログを出すだけに留める
            warn!(self.logger, "Task failure in RepairQueueExecutor: {}", e);
            Async::Ready(None)
        }) {
            self.task = Task::Idle;
            self.last_not_idle = Instant::now();
            if let RepairIdleness::Threshold(repair_idleness_threshold_duration) =
                self.repair_idleness_threshold
            {
                if let Some(version) = self.pop() {
                    let elapsed = self.last_not_idle.elapsed();
                    if elapsed < repair_idleness_threshold_duration {
                        self.push(version);
                        break;
                    } else {
                        let repair_lock = self.service_handle.acquire_repair_lock();
                        if let Some(repair_lock) = repair_lock {
                            self.task = Task::Repair(
                                RepairContent::new(
                                    &self.logger,
                                    &self.device,
                                    self.node_id,
                                    &self.client,
                                    &self.repair_metrics,
                                    version,
                                ),
                                repair_lock,
                            );
                            self.last_not_idle = Instant::now();
                        } else {
                            self.push(version);
                            break;
                        }
                    }
                }
            }
        }
        Ok(Async::NotReady)
    }
}

/// Trait for queue.
trait Queue<Pushed, Popped> {
    fn push(&mut self, element: Pushed);
    fn pop(&mut self) -> Option<Popped>;
}

struct RepairPrepQueue {
    queue: BinaryHeap<Reverse<TodoItem>>,
    enqueued: Counter,
    dequeued: Counter,
}
impl RepairPrepQueue {
    fn new(enqueued_repair: &Counter, dequeued_repair: &Counter) -> Self {
        Self {
            queue: BinaryHeap::new(),
            enqueued: enqueued_repair.clone(),
            dequeued: dequeued_repair.clone(),
        }
    }
}
impl Queue<TodoItem, TodoItem> for RepairPrepQueue {
    fn push(&mut self, element: TodoItem) {
        self.queue.push(Reverse(element));
        self.enqueued.increment();
    }
    fn pop(&mut self) -> Option<TodoItem> {
        let result = self.queue.pop();
        if let Some(_) = result {
            self.dequeued.increment();
        }
        // Shrink if necessary
        if self.queue.capacity() > 32 && self.queue.len() < self.queue.capacity() / 2 {
            self.queue.shrink_to_fit();
        }
        result.map(|element| element.0)
    }
}

struct DeleteQueue {
    deque: VecDeque<ObjectVersion>,
    enqueued: Counter,
    dequeued: Counter,
}
impl DeleteQueue {
    fn new(enqueued_delete: &Counter, dequeued_delete: &Counter) -> Self {
        Self {
            deque: VecDeque::new(),
            enqueued: enqueued_delete.clone(),
            dequeued: dequeued_delete.clone(),
        }
    }
}
impl Queue<ObjectVersion, TodoItem> for DeleteQueue {
    fn push(&mut self, element: ObjectVersion) {
        self.deque.push_back(element);
        self.enqueued.increment();
    }
    fn pop(&mut self) -> Option<TodoItem> {
        let result = self.deque.pop_front();
        if let Some(_) = result {
            self.dequeued.increment();
        }
        if self.deque.capacity() > 32 && self.deque.len() < self.deque.capacity() / 2 {
            self.deque.shrink_to_fit();
        }
        result.map(|version| TodoItem::DeleteContent {
            versions: vec![version],
        })
    }
}
