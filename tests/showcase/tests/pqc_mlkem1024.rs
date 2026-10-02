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

// The process-default `rustls::crypto::CryptoProvider` is global state. This
// file is a separate test binary (and process), so installing a provider here
// does not affect the other showcase tests.
#[cfg(all(test, feature = "run-showcase-tests"))]
mod pqc_mlkem1024 {
    use google_cloud_test_utils::errors::anydump;

    #[tokio::test]
    async fn run() -> anyhow::Result<()> {
        // This is what applications do to opt in to `MLKEM1024`: install a
        // process-default provider, before building any clients, that includes
        // the `MLKEM1024` key exchange group. Appending the group keeps the
        // default preference order unchanged.
        let mut provider = rustls::crypto::aws_lc_rs::default_provider();
        provider
            .kx_groups
            .push(rustls::crypto::aws_lc_rs::kx_group::MLKEM1024);
        provider
            .install_default()
            .map_err(|_| anyhow::anyhow!("a rustls CryptoProvider was already installed"))?;

        integration_tests_showcase::pqc::run_mlkem1024()
            .await
            .inspect_err(anydump)
    }
}
