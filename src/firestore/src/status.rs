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

//! Status conversions for Firestore.

#[cfg(test)]
mod tests {
    use crate::generated::gapic::prost::google;
    use gaxi::prost::{FromProto, ToProto};
    use google_cloud_rpc::model::Status;

    #[test]
    fn from_proto() -> anyhow::Result<()> {
        let input = google::rpc::Status {
            code: 12,
            message: "test-message".into(),
            ..Default::default()
        };
        let got = input.cnv()?;
        let want = Status::new().set_code(12).set_message("test-message");
        assert_eq!(got, want);
        Ok(())
    }

    #[test]
    fn to_proto() -> anyhow::Result<()> {
        let input = Status::new().set_code(12).set_message("test-message");
        let got: google::rpc::Status = input.to_proto()?;
        let want = google::rpc::Status {
            code: 12,
            message: "test-message".into(),
            ..Default::default()
        };
        assert_eq!(got, want);
        Ok(())
    }
}
