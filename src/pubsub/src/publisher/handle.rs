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
use std::task::{Context, Poll};
use tokio::sync::oneshot;

/// A handle that represents an in-flight publish operation.
///
/// This struct is a `Future`. You can `.await` it to get the final
/// result of the publish call: either a server-assigned message ID `String`
/// or an `Error` if the publish failed.
#[derive(Debug)]
pub struct PublishHandle {
    pub(crate) rx: oneshot::Receiver<Result<String, crate::Error>>,
}

impl Future for PublishHandle {
    type Output = Result<String, crate::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.rx).poll(cx) {
            Poll::Ready(Ok(result)) => Poll::Ready(result),
            Poll::Ready(Err(_)) => {
                Poll::Ready(Err(crate::Error::service(Default::default())))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
