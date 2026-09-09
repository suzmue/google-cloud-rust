// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use rand::RngExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::task::JoinHandle;

#[allow(
    dead_code,
    unused_variables,
    missing_docs,
    clippy::all,
    clippy::enum_variant_names,
    clippy::let_unit_value,
    clippy::wildcard_imports
)]
pub mod google {
    pub mod api {
        include!("../../../grpc-mock/src/generated/protos/google.api.rs");
    }
    pub mod pubsub {
        pub mod v1 {
            include!("../../../grpc-mock/src/generated/protos/google.pubsub.v1.rs");
        }
    }
}

use google::pubsub::v1::publisher_server::{Publisher, PublisherServer};
use google::pubsub::v1::{
    DeleteTopicRequest, DetachSubscriptionRequest, DetachSubscriptionResponse, GetTopicRequest,
    ListTopicSnapshotsRequest, ListTopicSnapshotsResponse, ListTopicSubscriptionsRequest,
    ListTopicSubscriptionsResponse, ListTopicsRequest, ListTopicsResponse, PublishRequest,
    PublishResponse, Topic, UpdateTopicRequest,
};

#[derive(Debug, Clone)]
pub struct MockServerConfig {
    pub fast_latency: Duration,
    pub fast_ratio: f64,
    pub degraded_latency: Duration,
    pub degraded_ratio: f64,
    pub stall_latency: Duration,
    pub stall_ratio: f64,
}

#[derive(Debug, Default, Clone)]
pub struct MockServerStats {
    pub total_publish_requests: Arc<AtomicU64>,
    pub hedged_publish_requests: Arc<AtomicU64>,
}

#[derive(Clone)]
struct MockPublisherService {
    config: MockServerConfig,
    stats: MockServerStats,
    id_counter: Arc<AtomicU64>,
}

#[tonic::async_trait]
impl Publisher for MockPublisherService {
    async fn create_topic(
        &self,
        _request: tonic::Request<Topic>,
    ) -> Result<tonic::Response<Topic>, tonic::Status> {
        Err(tonic::Status::unimplemented("unimplemented in mock server"))
    }

    async fn update_topic(
        &self,
        _request: tonic::Request<UpdateTopicRequest>,
    ) -> Result<tonic::Response<Topic>, tonic::Status> {
        Err(tonic::Status::unimplemented("unimplemented in mock server"))
    }

    async fn publish(
        &self,
        request: tonic::Request<PublishRequest>,
    ) -> Result<tonic::Response<PublishResponse>, tonic::Status> {
        self.stats
            .total_publish_requests
            .fetch_add(1, Ordering::Relaxed);

        if request
            .metadata()
            .get("x-goog-pubsub-client-telemetry")
            .is_some()
        {
            self.stats
                .hedged_publish_requests
                .fetch_add(1, Ordering::Relaxed);
        }

        let roll: f64 = rand::rng().random();
        let sleep_duration = if roll < self.config.fast_ratio {
            self.config.fast_latency
        } else if roll < self.config.fast_ratio + self.config.degraded_ratio {
            self.config.degraded_latency
        } else if roll
            < self.config.fast_ratio + self.config.degraded_ratio + self.config.stall_ratio
        {
            self.config.stall_latency
        } else {
            self.config.fast_latency
        };

        if !sleep_duration.is_zero() {
            tokio::time::sleep(sleep_duration).await;
        }

        let inner = request.into_inner();
        let count = inner.messages.len();
        let mut message_ids = Vec::with_capacity(count);
        for _ in 0..count {
            let id = self.id_counter.fetch_add(1, Ordering::Relaxed);
            message_ids.push(format!("msg-{id}"));
        }

        Ok(tonic::Response::new(PublishResponse { message_ids }))
    }

    async fn get_topic(
        &self,
        _request: tonic::Request<GetTopicRequest>,
    ) -> Result<tonic::Response<Topic>, tonic::Status> {
        Err(tonic::Status::unimplemented("unimplemented in mock server"))
    }

    async fn list_topics(
        &self,
        _request: tonic::Request<ListTopicsRequest>,
    ) -> Result<tonic::Response<ListTopicsResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unimplemented in mock server"))
    }

    async fn list_topic_subscriptions(
        &self,
        _request: tonic::Request<ListTopicSubscriptionsRequest>,
    ) -> Result<tonic::Response<ListTopicSubscriptionsResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unimplemented in mock server"))
    }

    async fn list_topic_snapshots(
        &self,
        _request: tonic::Request<ListTopicSnapshotsRequest>,
    ) -> Result<tonic::Response<ListTopicSnapshotsResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unimplemented in mock server"))
    }

    async fn delete_topic(
        &self,
        _request: tonic::Request<DeleteTopicRequest>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        Err(tonic::Status::unimplemented("unimplemented in mock server"))
    }

    async fn detach_subscription(
        &self,
        _request: tonic::Request<DetachSubscriptionRequest>,
    ) -> Result<tonic::Response<DetachSubscriptionResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("unimplemented in mock server"))
    }
}

pub struct MockServerHandle {
    pub stats: MockServerStats,
    pub server_task: JoinHandle<()>,
}

pub async fn start_mock_server(
    config: MockServerConfig,
    port: Option<u16>,
) -> anyhow::Result<(String, MockServerHandle)> {
    let port = port.unwrap_or(0);
    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}")).await?;
    let addr = listener.local_addr()?;

    let stats = MockServerStats::default();
    let service = MockPublisherService {
        config,
        stats: stats.clone(),
        id_counter: Arc::new(AtomicU64::new(1)),
    };

    let server_task = tokio::spawn(async move {
        let stream = tokio_stream::wrappers::TcpListenerStream::new(listener);
        let _ = tonic::transport::Server::builder()
            .add_service(PublisherServer::new(service))
            .serve_with_incoming(stream)
            .await;
    });

    let uri = format!("http://127.0.0.1:{}", addr.port());
    Ok((uri, MockServerHandle { stats, server_task }))
}
