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

use google_cloud_gax::options::RequestOptions;
use google_cloud_pubsub_bidi_test::client::{Publisher, Subscriber};
use google_cloud_pubsub_bidi_test::model::{
    PubsubMessage, StreamingPullRequest,
};

#[tokio::test]
async fn test_real_pubsub_bidi_streaming_pull() -> anyhow::Result<()> {
    let project_id = std::env::var("GOOGLE_CLOUD_PROJECT")
        .or_else(|_| std::env::var("PROJECT_ID"))
        .unwrap_or_else(|_| "suzmue-testing".to_string());

    let nonce = uuid::Uuid::new_v4().to_string();
    let nonce = &nonce[..8];
    let topic_name = format!("projects/{project_id}/topics/bidi-test-{nonce}");
    let sub_name = format!("projects/{project_id}/subscriptions/bidi-test-{nonce}");

    println!("Testing against GCP Pub/Sub in project: {project_id}");
    println!("Creating topic: {topic_name}");
    println!("Creating subscription: {sub_name}");

    let publisher = Publisher::builder().build().await?;
    let subscriber = Subscriber::builder().build().await?;

    // 1. Create Topic
    let _topic = publisher
        .create_topic()
        .set_name(&topic_name)
        .send()
        .await?;

    // 2. Create Subscription
    let _sub = subscriber
        .create_subscription()
        .set_name(&sub_name)
        .set_topic(&topic_name)
        .set_ack_deadline_seconds(10)
        .send()
        .await?;

    // Cleanup guard to ensure deletion even on failure
    struct Cleanup {
        publisher: Publisher,
        subscriber: Subscriber,
        topic_name: String,
        sub_name: String,
    }
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let p = self.publisher.clone();
            let s = self.subscriber.clone();
            let t = self.topic_name.clone();
            let sub = self.sub_name.clone();
            tokio::spawn(async move {
                let _ = s.delete_subscription().set_subscription(&sub).send().await;
                let _ = p.delete_topic().set_topic(&t).send().await;
            });
        }
    }

    let _cleanup = Cleanup {
        publisher: publisher.clone(),
        subscriber: subscriber.clone(),
        topic_name: topic_name.clone(),
        sub_name: sub_name.clone(),
    };

    // 3. Publish a message to topic
    let pub_res = publisher
        .publish()
        .set_topic(&topic_name)
        .set_messages([PubsubMessage::new().set_data(bytes::Bytes::from("Hello Real PubSub Bidi!"))])
        .send()
        .await?;
    println!("Published message IDs: {:?}", pub_res.message_ids);

    // 4. Open bidi stream with Subscriber client
    let (sender, mut receiver) = subscriber.streaming_pull(RequestOptions::default()).await;

    // 5. Send initial StreamingPullRequest
    let req = StreamingPullRequest::new()
        .set_subscription(&sub_name)
        .set_stream_ack_deadline_seconds(10);
    sender.send(req).await?;

    // 6. Receive streaming response
    let response = receiver
        .recv()
        .await
        .expect("expected streaming response")?;

    println!("Received streaming response with {} messages", response.received_messages.len());
    assert!(!response.received_messages.is_empty(), "expected at least one message in response");

    let received_msg = response.received_messages[0]
        .message
        .as_ref()
        .expect("expected message payload");
    println!("Received payload: {:?}", String::from_utf8_lossy(&received_msg.data));
    assert_eq!(received_msg.data.as_ref(), b"Hello Real PubSub Bidi!");

    // Acknowledge the message via bidi stream
    let ack_req = StreamingPullRequest::new()
        .set_subscription(&sub_name)
        .set_ack_ids([response.received_messages[0].ack_id.clone()]);
    sender.send(ack_req).await?;

    println!("Successfully tested bidi streaming against GCP Pub/Sub service!");
    Ok(())
}
