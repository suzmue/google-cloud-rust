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

use std::pin::Pin;
use std::task::{Context, Poll};

/// A wrapper for request streams.
pub struct RequestStream<T> {
    inner: Pin<Box<dyn futures::stream::Stream<Item = T> + Send>>,
}

impl<T> RequestStream<T> {
    /// Creates a `RequestStream` from a `tokio::sync::mpsc::Receiver`.
    pub fn from_receiver(mut rx: tokio::sync::mpsc::Receiver<T>) -> Self
    where
        T: Send + 'static,
    {
        let stream = futures::stream::poll_fn(move |cx| rx.poll_recv(cx));
        Self {
            inner: Box::pin(stream),
        }
    }

    /// Creates a `RequestStream` from an iterator.
    #[allow(clippy::should_implement_trait)]
    pub fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = T> + Send + 'static,
        I::IntoIter: Send,
        T: Send + 'static,
    {
        let stream = futures::stream::iter(iter);
        Self {
            inner: Box::pin(stream),
        }
    }

    /// Creates a `RequestStream` from a stream.
    #[cfg(feature = "unstable-stream")]
    #[cfg_attr(docsrs, doc(cfg(feature = "unstable-stream")))]
    pub fn from_stream<S>(stream: S) -> Self
    where
        S: futures::stream::Stream<Item = T> + Send + 'static,
    {
        Self {
            inner: Box::pin(stream),
        }
    }
}

impl<T> futures::stream::Stream for RequestStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

impl<T> std::fmt::Debug for RequestStream<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RequestStream").finish()
    }
}

/// A wrapper for response streams.
pub struct ResponseStream<T> {
    inner: Pin<Box<dyn futures::stream::Stream<Item = crate::Result<T>> + Send>>,
}

impl<T> ResponseStream<T> {
    /// Creates a `ResponseStream` from a stream.
    #[cfg(feature = "unstable-stream")]
    #[cfg_attr(docsrs, doc(cfg(feature = "unstable-stream")))]
    pub fn from_stream<S>(stream: S) -> Self
    where
        S: futures::stream::Stream<Item = crate::Result<T>> + Send + 'static,
    {
        Self {
            inner: Box::pin(stream),
        }
    }

    /// Returns the next item in the stream.
    pub async fn next(&mut self) -> Option<crate::Result<T>> {
        futures::future::poll_fn(|cx| self.inner.as_mut().poll_next(cx)).await
    }
}

impl<T> std::fmt::Debug for ResponseStream<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResponseStream").finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[tokio::test]
    async fn test_request_stream_from_receiver() {
        let (tx, rx) = tokio::sync::mpsc::channel(10);
        tx.send(1).await.unwrap();
        tx.send(2).await.unwrap();
        drop(tx);

        let mut stream = RequestStream::from_receiver(rx);
        assert_eq!(stream.next().await, Some(1));
        assert_eq!(stream.next().await, Some(2));
        assert_eq!(stream.next().await, None);
    }

    #[tokio::test]
    async fn test_request_stream_from_iter() {
        let mut stream = RequestStream::from_iter(vec![1, 2]);
        assert_eq!(stream.next().await, Some(1));
        assert_eq!(stream.next().await, Some(2));
        assert_eq!(stream.next().await, None);
    }

    #[cfg(feature = "unstable-stream")]
    #[tokio::test]
    async fn test_request_stream_from_stream() {
        let inner_stream = futures::stream::iter(vec![1, 2]);
        let mut stream = RequestStream::from_stream(inner_stream);
        assert_eq!(stream.next().await, Some(1));
        assert_eq!(stream.next().await, Some(2));
        assert_eq!(stream.next().await, None);
    }

    #[cfg(feature = "unstable-stream")]
    #[tokio::test]
    async fn test_response_stream() {
        let inner_stream = futures::stream::iter(vec![Ok(1), Err(crate::error::Error::io("test"))]);
        let mut stream = ResponseStream::from_stream(inner_stream);
        assert!(matches!(stream.next().await, Some(Ok(1))));
        assert!(matches!(stream.next().await, Some(Err(_))));
        assert!(stream.next().await.is_none());
    }
}
