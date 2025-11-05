// Copyright 2025 Google LLC
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

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use tokio::sync::oneshot;

/// A handle that represents an in-flight publish operation.
///
/// This struct is a `Future`. You can `.await` it to get the final
/// result of the publish call: either a server-assigned message ID `String`
/// or an `Error` if the publish failed.
pub struct PublishHandle {
    pub(crate) rx: oneshot::Receiver<crate::Result<String>>,
}

impl Future for PublishHandle {
    type Output = crate::Result<String>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result = ready!(Pin::new(&mut self.rx).poll(cx));
        // An error will only occur if the sender of the self.rx was dropped,
        // which would be a bug.
        Poll::Ready(result.expect("the client library should not release the sender"))
    }
}

/// A message that is published by publishers and consumed by subscribers. The
/// message must contain either a non-empty data field or at least one attribute.
///
/// See [quotas and limits]
/// (<https://cloud.google.com/pubsub/quotas>) for more information about message
/// limits.
#[derive(Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct Message {
    /// Optional. The message data field. If this field is empty, the message must
    /// contain at least one attribute.
    pub data: ::bytes::Bytes,

    /// Optional. Attributes for this message. If this field is empty, the message
    /// must contain non-empty data. This can be used to filter messages on the
    /// subscription.
    pub attributes: std::collections::HashMap<std::string::String, std::string::String>,

    /// ID of this message, assigned by the server when the message is published.
    /// Guaranteed to be unique within the topic.
    pub message_id: std::string::String,

    /// The time at which the message was published.
    pub publish_time: std::option::Option<wkt::Timestamp>,

    /// If non-empty, identifies related messages for which publish order
    /// should be respected. If a `Subscription` has `enable_message_ordering` set
    /// to `true`, messages published with the same non-empty `ordering_key` value
    /// will be delivered to subscribers in the order in which they are received by
    /// the Pub/Sub system.
    ///
    /// For more information, see [ordering
    /// messages](https://cloud.google.com/pubsub/docs/ordering).
    pub ordering_key: std::string::String,

    pub(crate) _unknown_fields: serde_json::Map<std::string::String, serde_json::Value>,
}

impl Message {
    pub fn new() -> Self {
        std::default::Default::default()
    }

    /// Sets the value of [data][crate::model_ext::Message::data].
    ///
    /// # Example
    /// ```
    /// # use google_cloud_pubsub::model_ext::Message;
    /// let x = Message::new().set_data(bytes::Bytes::from_static(b"example"));
    /// ```
    pub fn set_data<T: std::convert::Into<::bytes::Bytes>>(mut self, v: T) -> Self {
        self.data = v.into();
        self
    }

    /// Sets the value of [attributes][crate::model::PubsubMessage::attributes].
    ///
    /// # Example
    /// ```
    /// # use google_cloud_pubsub::model_ext::Message;
    /// let x = Message::new().set_attributes([
    ///     ("key0", "abc"),
    ///     ("key1", "xyz"),
    /// ]);
    /// ```
    pub fn set_attributes<T, K, V>(mut self, v: T) -> Self
    where
        T: std::iter::IntoIterator<Item = (K, V)>,
        K: std::convert::Into<std::string::String>,
        V: std::convert::Into<std::string::String>,
    {
        use std::iter::Iterator;
        self.attributes = v.into_iter().map(|(k, v)| (k.into(), v.into())).collect();
        self
    }

    /// Sets the value of [ordering_key][crate::model::PubsubMessage::ordering_key].
    ///
    /// # Example
    /// ```
    /// # use google_cloud_pubsub::model::PubsubMessage;
    /// let x = PubsubMessage::new().set_ordering_key("example");
    /// ```
    pub fn set_ordering_key<T: std::convert::Into<std::string::String>>(mut self, v: T) -> Self {
        self.ordering_key = v.into();
        self
    }
}

impl From<crate::model::PubsubMessage> for Message {
    fn from(msg: crate::model::PubsubMessage) -> Self {
        Self {
            data: msg.data,
            attributes: msg.attributes,
            message_id: msg.message_id,
            publish_time: msg.publish_time,
            ordering_key: msg.ordering_key,
            _unknown_fields: msg._unknown_fields,
        }
    }
}

impl From<Message> for crate::model::PubsubMessage {
    fn from(msg: Message) -> Self {
        crate::model::PubsubMessage {
            data: msg.data,
            attributes: msg.attributes,
            message_id: msg.message_id,
            publish_time: msg.publish_time,
            ordering_key: msg.ordering_key,
            _unknown_fields: msg._unknown_fields,
        }
    }
}
