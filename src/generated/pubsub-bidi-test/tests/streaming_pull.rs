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

#![cfg(google_cloud_unstable_gapic_streaming)]

use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
use google_cloud_gax::options::RequestOptions;
use google_cloud_pubsub_bidi_test::client::Subscriber;
use google_cloud_pubsub_bidi_test::model::StreamingPullRequest;
use pubsub_grpc_mock::{start, MockSubscriber};

#[tokio::test]
async fn test_pubsub_bidi_streaming_pull() -> anyhow::Result<()> {
    let (resp_tx, resp_rx) = tokio::sync::mpsc::channel(10);

    let mut mock = MockSubscriber::new();
    mock.expect_streaming_pull().return_once(move |_req| {
        let response = pubsub_grpc_mock::google::pubsub::v1::StreamingPullResponse {
            received_messages: vec![pubsub_grpc_mock::google::pubsub::v1::ReceivedMessage {
                ack_id: "test-ack-id".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let _ = resp_tx.try_send(Ok(response));
        Ok(tonic::Response::new(resp_rx))
    });

    let (endpoint, _server) = start("127.0.0.1:0", mock).await?;

    let client = Subscriber::builder()
        .with_endpoint(endpoint)
        .with_credentials(Anonymous::new().build())
        .build()
        .await?;

    let (sender, mut receiver) = client.streaming_pull(RequestOptions::default()).await;

    let req = StreamingPullRequest::new().set_subscription("projects/test/subscriptions/test-sub");
    sender.send(req).await?;

    let response = receiver.recv().await.expect("expected response")?;
    assert_eq!(response.received_messages.len(), 1);
    assert_eq!(response.received_messages[0].ack_id, "test-ack-id");

    Ok(())
}
