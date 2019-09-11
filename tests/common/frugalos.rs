use fibers_rpc::client::ClientServiceHandle;
use libfrugalos;
use libfrugalos::entity::bucket::Bucket;
use libfrugalos::entity::object::{ObjectId, ObjectSummary, ObjectVersion};
use libfrugalos::entity::server::{Server, ServerId};
use slog::Logger;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

use super::AsyncResult;

pub struct FrugalosClientRegistry {
    logger: Logger,
    service: ClientServiceHandle,
    clients: HashMap<ServerId, FrugalosClient>,
}
impl FrugalosClientRegistry {
    pub fn new(logger: Logger, service: ClientServiceHandle) -> Self {
        Self {
            logger,
            service,
            clients: HashMap::new(),
        }
    }
    pub fn get(&self, id: &ServerId) -> Option<&FrugalosClient> {
        self.clients.get(id)
    }
    pub fn register(&mut self, server: &Server) {
        let client = FrugalosClient::new(self.logger.clone(), server.addr(), self.service.clone());
        self.clients.insert(server.id.clone(), client);
    }
}

#[derive(Debug, Clone)]
pub struct FrugalosClient {
    logger: Logger,
    frugalos_addr: SocketAddr,
    service: ClientServiceHandle,
}
impl FrugalosClient {
    pub fn new(logger: Logger, frugalos_addr: SocketAddr, service: ClientServiceHandle) -> Self {
        FrugalosClient {
            logger,
            frugalos_addr,
            service,
        }
    }

    pub fn get_bucket(&self, bucket_id: &str) -> AsyncResult<Option<Bucket>> {
        let client =
            libfrugalos::client::config::Client::new(self.frugalos_addr, self.service.clone());
        async!(client.get_bucket(bucket_id.to_owned()))
    }

    pub fn get_objects(&self, bucket_id: &str, segment: u16) -> AsyncResult<Vec<ObjectSummary>> {
        let client =
            libfrugalos::client::frugalos::Client::new(self.frugalos_addr, self.service.clone());
        async!(client.list_objects(bucket_id.to_owned(), segment))
    }

    pub fn get_latest_version(
        &self,
        bucket_id: &str,
        segment: u16,
    ) -> AsyncResult<Option<ObjectSummary>> {
        let client =
            libfrugalos::client::frugalos::Client::new(self.frugalos_addr, self.service.clone());
        async!(client.latest_version(bucket_id.to_owned(), segment))
    }

    pub fn get_object(&self, bucket_id: &str, object_id: ObjectId) -> AsyncResult<Option<Vec<u8>>> {
        let client =
            libfrugalos::client::frugalos::Client::new(self.frugalos_addr, self.service.clone());
        async!(client
            .get_object(
                bucket_id.to_owned(),
                object_id,
                Duration::from_secs(30),
                Default::default()
            )
            .map(|o| o.map(|(_, data)| data)))
    }

    pub fn delete_object(
        &self,
        bucket_id: &str,
        object_id: ObjectId,
    ) -> AsyncResult<Option<ObjectVersion>> {
        let client =
            libfrugalos::client::frugalos::Client::new(self.frugalos_addr, self.service.clone());
        async!(client.delete_object(
            bucket_id.to_owned(),
            object_id,
            Duration::from_secs(30),
            Default::default()
        ))
    }
}
