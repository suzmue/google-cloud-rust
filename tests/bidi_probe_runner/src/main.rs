use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio_stream::StreamExt as _;

struct DelayedStream<T> {
    delay: Pin<Box<tokio::time::Sleep>>,
    item: Option<T>,
}

impl<T> DelayedStream<T> {
    fn new(dur: Duration, item: T) -> Self {
        Self {
            delay: Box::pin(tokio::time::sleep(dur)),
            item: Some(item),
        }
    }
}

impl<T: Unpin> tokio_stream::Stream for DelayedStream<T> {
    type Item = T;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.item.is_none() {
            return Poll::Ready(None);
        }
        match self.delay.as_mut().poll(cx) {
            Poll::Ready(()) => Poll::Ready(self.item.take()),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("============================================================");
    println!("   AUTOMATED BIDI STREAMING INITIAL REQUEST PROBE RUNNER");
    println!("============================================================");

    let delay_dur = Duration::from_secs(10);

    // 1. GAPIC Showcase (Echo.Chat)
    probe_showcase_echo_chat(delay_dur).await;

    // 2. GAPIC Showcase (Messaging.Connect)
    probe_showcase_messaging_connect(delay_dur).await;

    // 3. Speech V2 (StreamingRecognize)
    probe_speech_v2(delay_dur).await;

    // 4. Text-to-Speech V1 (StreamingSynthesize)
    probe_tts_v1(delay_dur).await;

    // 5. AI Platform V1 (PredictionService.StreamingPredict)
    probe_aiplatform_streaming_predict(delay_dur).await;

    // 6. AI Platform V1 (PredictionService.StreamingRawPredict)
    probe_aiplatform_streaming_raw_predict(delay_dur).await;

    // 7. AI Platform V1 (PredictionService.StreamDirectPredict)
    probe_aiplatform_stream_direct_predict(delay_dur).await;

    // 8. AI Platform V1 (PredictionService.StreamDirectRawPredict)
    probe_aiplatform_stream_direct_raw_predict(delay_dur).await;

    // 9. AI Platform V1 (FeatureOnlineStoreService.FeatureViewDirectWrite)
    probe_aiplatform_feature_view_direct_write(delay_dur).await;

    // 10. Apigee Connect V1 (Tether.Egress)
    probe_apigeeconnect_v1(delay_dur).await;

    // 11. Discovery Engine V1 (stream_generate_grounded_content)
    probe_discoveryengine_v1(delay_dur).await;

    // 12. Model Armor V1 (stream_sanitize_user_prompt)
    probe_modelarmor_user_prompt(delay_dur).await;

    // 13. Model Armor V1 (stream_sanitize_model_response)
    probe_modelarmor_model_response(delay_dur).await;

    // 14. Device Streaming V1 (adb_connect)
    probe_devicestreaming_v1(delay_dur).await;

    // 15. Dialogflow CX V3 (Sessions.streaming_detect_intent)
    probe_dialogflow_cx_v3(delay_dur).await;

    // 16. Dialogflow V2 (Sessions.streaming_detect_intent)
    probe_dialogflow_v2_sessions(delay_dur).await;

    // 17. Dialogflow V2 (Participants.streaming_analyze_content)
    probe_dialogflow_v2_participants(delay_dur).await;

    // 18. Vision AI V1 (StreamingService.send_packets)
    probe_visionai_v1_send_packets(delay_dur).await;

    // 19. Vision AI V1 (StreamingService.receive_packets)
    probe_visionai_v1_receive_packets(delay_dur).await;

    // 20. Vision AI V1 (StreamingService.receive_events)
    probe_visionai_v1_receive_events(delay_dur).await;

    // 21. Vision AI V1 (Warehouse.ingest_asset)
    probe_visionai_v1_ingest_asset(delay_dur).await;

    println!("\n============================================================");
    println!("               ALL BIDI PROBES COMPLETED");
    println!("============================================================");

    Ok(())
}

async fn probe_showcase_echo_chat(delay_dur: Duration) {
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use google_cloud_showcase_v1beta1::client::Echo;
    use google_cloud_showcase_v1beta1::model::EchoRequest;

    println!("\n------------------------------------------------------------");
    println!("PROBING 1: Showcase Echo.Chat (Endpoint: http://localhost:7469)");

    let client_res = Echo::builder()
        .with_endpoint("http://localhost:7469")
        .with_credentials(Anonymous::new().build())
        .build()
        .await;

    let Ok(client) = client_res else {
        println!("[SKIP/WARN] Showcase endpoint localhost:7469 not active.");
        return;
    };

    let req = EchoRequest::default().set_content("Hello Showcase Probe");
    let stream = DelayedStream::new(delay_dur, req);

    let t0 = Instant::now();
    let res = client.chat(stream, google_cloud_gax::options::RequestOptions::default()).await;
    let t_headers = Instant::now();
    let elapsed_headers = t_headers.duration_since(t0);

    match res {
        Ok(mut resp_stream) => {
            println!("[Headers Received] t_headers = {:.2?}", elapsed_headers);
            let first_item = resp_stream.next().await;
            let elapsed_item = Instant::now().duration_since(t0);
            println!("[First Item Received] t_first_item = {:.2?}: {:?}", elapsed_item, first_item);

            if elapsed_headers < delay_dur {
                println!(">>> CLASSIFICATION: CATEGORY 1 (STREAM CREATED PRE-MESSAGE) <<<");
            } else {
                println!(">>> CLASSIFICATION: CATEGORY 2 (STREAM CREATED POST-MESSAGE) <<<");
            }
        }
        Err(e) => {
            println!("[Stream Error/Header Fail] t_headers = {:.2?}: {:?}", elapsed_headers, e);
            if elapsed_headers < delay_dur {
                println!(">>> CLASSIFICATION: CATEGORY 1 (FAILED PRE-MESSAGE: {:?}) <<<", e);
            } else {
                println!(">>> CLASSIFICATION: CATEGORY 2 (FAILED POST-MESSAGE: {:?}) <<<", e);
            }
        }
    }
}

async fn probe_showcase_messaging_connect(delay_dur: Duration) {
    use google_cloud_auth::credentials::anonymous::Builder as Anonymous;
    use google_cloud_showcase_v1beta1::client::Messaging;
    use google_cloud_showcase_v1beta1::model::ConnectRequest;

    println!("\n------------------------------------------------------------");
    println!("PROBING 2: Showcase Messaging.Connect (Endpoint: http://localhost:7469)");

    let client_res = Messaging::builder()
        .with_endpoint("http://localhost:7469")
        .with_credentials(Anonymous::new().build())
        .build()
        .await;

    let Ok(client) = client_res else {
        println!("[SKIP/WARN] Showcase endpoint localhost:7469 not active.");
        return;
    };

    let req = ConnectRequest::default();
    let stream = DelayedStream::new(delay_dur, req);

    let t0 = Instant::now();
    let res = client.connect(stream, google_cloud_gax::options::RequestOptions::default()).await;
    let t_headers = Instant::now();
    let elapsed_headers = t_headers.duration_since(t0);

    match res {
        Ok(mut resp_stream) => {
            println!("[Headers Received] t_headers = {:.2?}", elapsed_headers);
            let first_item = resp_stream.next().await;
            let elapsed_item = Instant::now().duration_since(t0);
            println!("[First Item Received] t_first_item = {:.2?}: {:?}", elapsed_item, first_item);

            if elapsed_headers < delay_dur {
                println!(">>> CLASSIFICATION: CATEGORY 1 (STREAM CREATED PRE-MESSAGE) <<<");
            } else {
                println!(">>> CLASSIFICATION: CATEGORY 2 (STREAM CREATED POST-MESSAGE) <<<");
            }
        }
        Err(e) => {
            println!("[Stream Error/Header Fail] t_headers = {:.2?}: {:?}", elapsed_headers, e);
            if elapsed_headers < delay_dur {
                println!(">>> CLASSIFICATION: CATEGORY 1 (FAILED PRE-MESSAGE: {:?}) <<<", e);
            } else {
                println!(">>> CLASSIFICATION: CATEGORY 2 (FAILED POST-MESSAGE: {:?}) <<<", e);
            }
        }
    }
}

async fn probe_speech_v2(delay_dur: Duration) {
    use google_cloud_speech_v2::client::Speech;
    use google_cloud_speech_v2::model::StreamingRecognizeRequest;

    println!("\n------------------------------------------------------------");
    println!("PROBING 3: Speech V2 StreamingRecognize");

    let Ok(client) = Speech::builder().build().await else {
        println!("[ERROR] Failed to build Speech client with ADC credentials.");
        return;
    };

    let req = StreamingRecognizeRequest::default().set_recognizer("projects/rust-sdk-testing/locations/global/recognizers/_");
    let stream = DelayedStream::new(delay_dur, req);

    let t0 = Instant::now();
    let res = client.streaming_recognize(stream, google_cloud_gax::options::RequestOptions::default()).await;
    let t_headers = Instant::now();
    let elapsed_headers = t_headers.duration_since(t0);

    match res {
        Ok(mut resp_stream) => {
            println!("[Headers Received] t_headers = {:.2?}", elapsed_headers);
            let first_item = resp_stream.next().await;
            let elapsed_item = Instant::now().duration_since(t0);
            println!("[First Item Received] t_first_item = {:.2?}: {:?}", elapsed_item, first_item);

            if elapsed_headers < delay_dur {
                println!(">>> CLASSIFICATION: CATEGORY 1 (STREAM CREATED PRE-MESSAGE) <<<");
            } else {
                println!(">>> CLASSIFICATION: CATEGORY 2 (STREAM CREATED POST-MESSAGE) <<<");
            }
        }
        Err(e) => {
            println!("[Stream Call Error] t_headers = {:.2?}: {:?}", elapsed_headers, e);
            if elapsed_headers < delay_dur {
                println!(">>> CLASSIFICATION: CATEGORY 1 (FAIL PRE-MESSAGE: {}) <<<", e);
            } else {
                println!(">>> CLASSIFICATION: CATEGORY 2 (FAIL POST-MESSAGE: {}) <<<", e);
            }
        }
    }
}

async fn probe_tts_v1(delay_dur: Duration) {
    use google_cloud_texttospeech_v1::client::TextToSpeech;
    use google_cloud_texttospeech_v1::model::StreamingSynthesizeRequest;

    println!("\n------------------------------------------------------------");
    println!("PROBING 4: Text-to-Speech V1 StreamingSynthesize");

    let Ok(client) = TextToSpeech::builder().build().await else {
        println!("[ERROR] Failed to build TextToSpeech client.");
        return;
    };

    let req = StreamingSynthesizeRequest::default();
    let stream = DelayedStream::new(delay_dur, req);

    let t0 = Instant::now();
    let res = client.streaming_synthesize(stream, google_cloud_gax::options::RequestOptions::default()).await;
    let t_headers = Instant::now();
    let elapsed_headers = t_headers.duration_since(t0);

    match res {
        Ok(mut resp_stream) => {
            println!("[Headers Received] t_headers = {:.2?}", elapsed_headers);
            let first_item = resp_stream.next().await;
            let elapsed_item = Instant::now().duration_since(t0);
            println!("[First Item Received] t_first_item = {:.2?}: {:?}", elapsed_item, first_item);

            if elapsed_headers < delay_dur {
                println!(">>> CLASSIFICATION: CATEGORY 1 (STREAM CREATED PRE-MESSAGE) <<<");
            } else {
                println!(">>> CLASSIFICATION: CATEGORY 2 (STREAM CREATED POST-MESSAGE) <<<");
            }
        }
        Err(e) => {
            println!("[Stream Call Error] t_headers = {:.2?}: {:?}", elapsed_headers, e);
            if elapsed_headers < delay_dur {
                println!(">>> CLASSIFICATION: CATEGORY 1 (FAIL PRE-MESSAGE: {}) <<<", e);
            } else {
                println!(">>> CLASSIFICATION: CATEGORY 2 (FAIL POST-MESSAGE: {}) <<<", e);
            }
        }
    }
}

async fn probe_aiplatform_streaming_predict(delay_dur: Duration) {
    use google_cloud_aiplatform_v1::client::PredictionService;
    use google_cloud_aiplatform_v1::model::StreamingPredictRequest;

    println!("\n------------------------------------------------------------");
    println!("PROBING 5: AI Platform V1 StreamingPredict");

    let Ok(client) = PredictionService::builder().build().await else {
        println!("[ERROR] Failed to build AI Platform PredictionService client.");
        return;
    };

    let req = StreamingPredictRequest::default().set_endpoint("projects/rust-sdk-testing/locations/us-central1/endpoints/probe");
    let stream = DelayedStream::new(delay_dur, req);

    let t0 = Instant::now();
    let res = client.streaming_predict(stream, google_cloud_gax::options::RequestOptions::default()).await;
    let t_headers = Instant::now();
    let elapsed_headers = t_headers.duration_since(t0);

    match res {
        Ok(mut resp_stream) => {
            println!("[Headers Received] t_headers = {:.2?}", elapsed_headers);
            let first_item = resp_stream.next().await;
            let elapsed_item = Instant::now().duration_since(t0);
            println!("[First Item Received] t_first_item = {:.2?}: {:?}", elapsed_item, first_item);

            if elapsed_headers < delay_dur {
                println!(">>> CLASSIFICATION: CATEGORY 1 (STREAM CREATED PRE-MESSAGE) <<<");
            } else {
                println!(">>> CLASSIFICATION: CATEGORY 2 (STREAM CREATED POST-MESSAGE) <<<");
            }
        }
        Err(e) => {
            println!("[Stream Call Error] t_headers = {:.2?}: {:?}", elapsed_headers, e);
            if elapsed_headers < delay_dur {
                println!(">>> CLASSIFICATION: CATEGORY 1 (FAIL PRE-MESSAGE: {}) <<<", e);
            } else {
                println!(">>> CLASSIFICATION: CATEGORY 2 (FAIL POST-MESSAGE: {}) <<<", e);
            }
        }
    }
}

async fn probe_aiplatform_streaming_raw_predict(delay_dur: Duration) {
    use google_cloud_aiplatform_v1::client::PredictionService;
    use google_cloud_aiplatform_v1::model::StreamingRawPredictRequest;

    println!("\n------------------------------------------------------------");
    println!("PROBING 6: AI Platform V1 streaming_raw_predict");

    let Ok(client) = PredictionService::builder().build().await else {
        println!("[ERROR] Failed to build AI Platform PredictionService client.");
        return;
    };

    let req = StreamingRawPredictRequest::default().set_endpoint("projects/rust-sdk-testing/locations/us-central1/endpoints/probe");
    let stream = DelayedStream::new(delay_dur, req);

    let t0 = Instant::now();
    let res = client.streaming_raw_predict(stream, google_cloud_gax::options::RequestOptions::default()).await;
    let t_headers = Instant::now();
    let elapsed_headers = t_headers.duration_since(t0);

    match res {
        Ok(mut resp_stream) => {
            println!("[Headers Received] t_headers = {:.2?}", elapsed_headers);
            let first_item = resp_stream.next().await;
            let elapsed_item = Instant::now().duration_since(t0);
            println!("[First Item Received] t_first_item = {:.2?}: {:?}", elapsed_item, first_item);

            if elapsed_headers < delay_dur {
                println!(">>> CLASSIFICATION: CATEGORY 1 (STREAM CREATED PRE-MESSAGE) <<<");
            } else {
                println!(">>> CLASSIFICATION: CATEGORY 2 (STREAM CREATED POST-MESSAGE) <<<");
            }
        }
        Err(e) => {
            println!("[Stream Call Error] t_headers = {:.2?}: {:?}", elapsed_headers, e);
            if elapsed_headers < delay_dur {
                println!(">>> CLASSIFICATION: CATEGORY 1 (FAIL PRE-MESSAGE: {}) <<<", e);
            } else {
                println!(">>> CLASSIFICATION: CATEGORY 2 (FAIL POST-MESSAGE: {}) <<<", e);
            }
        }
    }
}

async fn probe_aiplatform_stream_direct_predict(delay_dur: Duration) {
    use google_cloud_aiplatform_v1::client::PredictionService;
    use google_cloud_aiplatform_v1::model::StreamDirectPredictRequest;

    println!("\n------------------------------------------------------------");
    println!("PROBING 7: AI Platform V1 stream_direct_predict");

    let Ok(client) = PredictionService::builder().build().await else {
        println!("[ERROR] Failed to build AI Platform PredictionService client.");
        return;
    };

    let req = StreamDirectPredictRequest::default().set_endpoint("projects/rust-sdk-testing/locations/us-central1/endpoints/probe");
    let stream = DelayedStream::new(delay_dur, req);

    let t0 = Instant::now();
    let res = client.stream_direct_predict(stream, google_cloud_gax::options::RequestOptions::default()).await;
    let t_headers = Instant::now();
    let elapsed_headers = t_headers.duration_since(t0);

    match res {
        Ok(mut resp_stream) => {
            println!("[Headers Received] t_headers = {:.2?}", elapsed_headers);
            let first_item = resp_stream.next().await;
            let elapsed_item = Instant::now().duration_since(t0);
            println!("[First Item Received] t_first_item = {:.2?}: {:?}", elapsed_item, first_item);

            if elapsed_headers < delay_dur {
                println!(">>> CLASSIFICATION: CATEGORY 1 (STREAM CREATED PRE-MESSAGE) <<<");
            } else {
                println!(">>> CLASSIFICATION: CATEGORY 2 (STREAM CREATED POST-MESSAGE) <<<");
            }
        }
        Err(e) => {
            println!("[Stream Call Error] t_headers = {:.2?}: {:?}", elapsed_headers, e);
            if elapsed_headers < delay_dur {
                println!(">>> CLASSIFICATION: CATEGORY 1 (FAIL PRE-MESSAGE: {}) <<<", e);
            } else {
                println!(">>> CLASSIFICATION: CATEGORY 2 (FAIL POST-MESSAGE: {}) <<<", e);
            }
        }
    }
}

async fn probe_aiplatform_stream_direct_raw_predict(delay_dur: Duration) {
    use google_cloud_aiplatform_v1::client::PredictionService;
    use google_cloud_aiplatform_v1::model::StreamDirectRawPredictRequest;

    println!("\n------------------------------------------------------------");
    println!("PROBING 8: AI Platform V1 stream_direct_raw_predict");

    let Ok(client) = PredictionService::builder().build().await else {
        println!("[ERROR] Failed to build AI Platform PredictionService client.");
        return;
    };

    let req = StreamDirectRawPredictRequest::default().set_endpoint("projects/rust-sdk-testing/locations/us-central1/endpoints/probe");
    let stream = DelayedStream::new(delay_dur, req);

    let t0 = Instant::now();
    let res = client.stream_direct_raw_predict(stream, google_cloud_gax::options::RequestOptions::default()).await;
    let t_headers = Instant::now();
    let elapsed_headers = t_headers.duration_since(t0);

    match res {
        Ok(mut resp_stream) => {
            println!("[Headers Received] t_headers = {:.2?}", elapsed_headers);
            let first_item = resp_stream.next().await;
            let elapsed_item = Instant::now().duration_since(t0);
            println!("[First Item Received] t_first_item = {:.2?}: {:?}", elapsed_item, first_item);

            if elapsed_headers < delay_dur {
                println!(">>> CLASSIFICATION: CATEGORY 1 (STREAM CREATED PRE-MESSAGE) <<<");
            } else {
                println!(">>> CLASSIFICATION: CATEGORY 2 (STREAM CREATED POST-MESSAGE) <<<");
            }
        }
        Err(e) => {
            println!("[Stream Call Error] t_headers = {:.2?}: {:?}", elapsed_headers, e);
            if elapsed_headers < delay_dur {
                println!(">>> CLASSIFICATION: CATEGORY 1 (FAIL PRE-MESSAGE: {}) <<<", e);
            } else {
                println!(">>> CLASSIFICATION: CATEGORY 2 (FAIL POST-MESSAGE: {}) <<<", e);
            }
        }
    }
}

async fn probe_aiplatform_feature_view_direct_write(delay_dur: Duration) {
    use google_cloud_aiplatform_v1::client::FeatureOnlineStoreService;
    use google_cloud_aiplatform_v1::model::FeatureViewDirectWriteRequest;

    println!("\n------------------------------------------------------------");
    println!("PROBING 9: AI Platform V1 FeatureOnlineStoreService.feature_view_direct_write");

    let Ok(client) = FeatureOnlineStoreService::builder().build().await else {
        println!("[ERROR] Failed to build FeatureOnlineStoreService client.");
        return;
    };

    let req = FeatureViewDirectWriteRequest::default().set_feature_view("projects/rust-sdk-testing/locations/us-central1/featureOnlineStores/fos/featureViews/fv");
    let stream = DelayedStream::new(delay_dur, req);

    let t0 = Instant::now();
    let res = client.feature_view_direct_write(stream, google_cloud_gax::options::RequestOptions::default()).await;
    let t_headers = Instant::now();
    let elapsed_headers = t_headers.duration_since(t0);

    match res {
        Ok(mut resp_stream) => {
            println!("[Headers Received] t_headers = {:.2?}", elapsed_headers);
            let first_item = resp_stream.next().await;
            let elapsed_item = Instant::now().duration_since(t0);
            println!("[First Item Received] t_first_item = {:.2?}: {:?}", elapsed_item, first_item);

            if elapsed_headers < delay_dur {
                println!(">>> CLASSIFICATION: CATEGORY 1 (STREAM CREATED PRE-MESSAGE) <<<");
            } else {
                println!(">>> CLASSIFICATION: CATEGORY 2 (STREAM CREATED POST-MESSAGE) <<<");
            }
        }
        Err(e) => {
            println!("[Stream Call Error] t_headers = {:.2?}: {:?}", elapsed_headers, e);
            if elapsed_headers < delay_dur {
                println!(">>> CLASSIFICATION: CATEGORY 1 (FAIL PRE-MESSAGE: {}) <<<", e);
            } else {
                println!(">>> CLASSIFICATION: CATEGORY 2 (FAIL POST-MESSAGE: {}) <<<", e);
            }
        }
    }
}

async fn probe_apigeeconnect_v1(delay_dur: Duration) {
    use google_cloud_apigeeconnect_v1::client::Tether;
    use google_cloud_apigeeconnect_v1::model::EgressResponse;

    println!("\n------------------------------------------------------------");
    println!("PROBING 10: Apigee Connect V1 Egress");

    let Ok(client) = Tether::builder().build().await else {
        println!("[ERROR] Failed to build Apigee Connect client.");
        return;
    };

    let req = EgressResponse::default().set_id("probe-id");
    let stream = DelayedStream::new(delay_dur, req);

    let t0 = Instant::now();
    let res = client.egress(stream, google_cloud_gax::options::RequestOptions::default()).await;
    let t_headers = Instant::now();
    let elapsed_headers = t_headers.duration_since(t0);

    match res {
        Ok(mut resp_stream) => {
            println!("[Headers Received] t_headers = {:.2?}", elapsed_headers);
            let first_item = resp_stream.next().await;
            let elapsed_item = Instant::now().duration_since(t0);
            println!("[First Item Received] t_first_item = {:.2?}: {:?}", elapsed_item, first_item);

            if elapsed_headers < delay_dur {
                println!(">>> CLASSIFICATION: CATEGORY 1 (STREAM CREATED PRE-MESSAGE) <<<");
            } else {
                println!(">>> CLASSIFICATION: CATEGORY 2 (STREAM CREATED POST-MESSAGE) <<<");
            }
        }
        Err(e) => {
            println!("[Stream Call Error] t_headers = {:.2?}: {:?}", elapsed_headers, e);
            if elapsed_headers < delay_dur {
                println!(">>> CLASSIFICATION: CATEGORY 1 (FAIL PRE-MESSAGE: {}) <<<", e);
            } else {
                println!(">>> CLASSIFICATION: CATEGORY 2 (FAIL POST-MESSAGE: {}) <<<", e);
            }
        }
    }
}

async fn probe_discoveryengine_v1(delay_dur: Duration) {
    use google_cloud_discoveryengine_v1::client::GroundedGenerationService;
    use google_cloud_discoveryengine_v1::model::GenerateGroundedContentRequest;

    println!("\n------------------------------------------------------------");
    println!("PROBING 11: Discovery Engine V1 stream_generate_grounded_content");

    let Ok(client) = GroundedGenerationService::builder().build().await else {
        println!("[ERROR] Failed to build Discovery Engine client.");
        return;
    };

    let req = GenerateGroundedContentRequest::default().set_location("projects/rust-sdk-testing/locations/global");
    let stream = DelayedStream::new(delay_dur, req);

    let t0 = Instant::now();
    let res = client.stream_generate_grounded_content(stream, google_cloud_gax::options::RequestOptions::default()).await;
    let t_headers = Instant::now();
    let elapsed_headers = t_headers.duration_since(t0);

    match res {
        Ok(mut resp_stream) => {
            println!("[Headers Received] t_headers = {:.2?}", elapsed_headers);
            let first_item = resp_stream.next().await;
            let elapsed_item = Instant::now().duration_since(t0);
            println!("[First Item Received] t_first_item = {:.2?}: {:?}", elapsed_item, first_item);

            if elapsed_headers < delay_dur {
                println!(">>> CLASSIFICATION: CATEGORY 1 (STREAM CREATED PRE-MESSAGE) <<<");
            } else {
                println!(">>> CLASSIFICATION: CATEGORY 2 (STREAM CREATED POST-MESSAGE) <<<");
            }
        }
        Err(e) => {
            println!("[Stream Call Error] t_headers = {:.2?}: {:?}", elapsed_headers, e);
            if elapsed_headers < delay_dur {
                println!(">>> CLASSIFICATION: CATEGORY 1 (FAIL PRE-MESSAGE: {}) <<<", e);
            } else {
                println!(">>> CLASSIFICATION: CATEGORY 2 (FAIL POST-MESSAGE: {}) <<<", e);
            }
        }
    }
}

async fn probe_modelarmor_user_prompt(delay_dur: Duration) {
    use google_cloud_modelarmor_v1::client::ModelArmor;
    use google_cloud_modelarmor_v1::model::SanitizeUserPromptRequest;

    println!("\n------------------------------------------------------------");
    println!("PROBING 12: Model Armor V1 stream_sanitize_user_prompt");

    let Ok(client) = ModelArmor::builder().build().await else {
        println!("[ERROR] Failed to build Model Armor client.");
        return;
    };

    let req = SanitizeUserPromptRequest::default().set_name("projects/rust-sdk-testing/locations/global/templates/probe");
    let stream = DelayedStream::new(delay_dur, req);

    let t0 = Instant::now();
    let res = client.stream_sanitize_user_prompt(stream, google_cloud_gax::options::RequestOptions::default()).await;
    let t_headers = Instant::now();
    let elapsed_headers = t_headers.duration_since(t0);

    match res {
        Ok(mut resp_stream) => {
            println!("[Headers Received] t_headers = {:.2?}", elapsed_headers);
            let first_item = resp_stream.next().await;
            let elapsed_item = Instant::now().duration_since(t0);
            println!("[First Item Received] t_first_item = {:.2?}: {:?}", elapsed_item, first_item);

            if elapsed_headers < delay_dur {
                println!(">>> CLASSIFICATION: CATEGORY 1 (STREAM CREATED PRE-MESSAGE) <<<");
            } else {
                println!(">>> CLASSIFICATION: CATEGORY 2 (STREAM CREATED POST-MESSAGE) <<<");
            }
        }
        Err(e) => {
            println!("[Stream Call Error] t_headers = {:.2?}: {:?}", elapsed_headers, e);
            if elapsed_headers < delay_dur {
                println!(">>> CLASSIFICATION: CATEGORY 1 (FAIL PRE-MESSAGE: {}) <<<", e);
            } else {
                println!(">>> CLASSIFICATION: CATEGORY 2 (FAIL POST-MESSAGE: {}) <<<", e);
            }
        }
    }
}

async fn probe_modelarmor_model_response(delay_dur: Duration) {
    use google_cloud_modelarmor_v1::client::ModelArmor;
    use google_cloud_modelarmor_v1::model::SanitizeModelResponseRequest;

    println!("\n------------------------------------------------------------");
    println!("PROBING 13: Model Armor V1 stream_sanitize_model_response");

    let Ok(client) = ModelArmor::builder().build().await else {
        println!("[ERROR] Failed to build Model Armor client.");
        return;
    };

    let req = SanitizeModelResponseRequest::default().set_name("projects/rust-sdk-testing/locations/global/templates/probe");
    let stream = DelayedStream::new(delay_dur, req);

    let t0 = Instant::now();
    let res = client.stream_sanitize_model_response(stream, google_cloud_gax::options::RequestOptions::default()).await;
    let t_headers = Instant::now();
    let elapsed_headers = t_headers.duration_since(t0);

    match res {
        Ok(mut resp_stream) => {
            println!("[Headers Received] t_headers = {:.2?}", elapsed_headers);
            let first_item = resp_stream.next().await;
            let elapsed_item = Instant::now().duration_since(t0);
            println!("[First Item Received] t_first_item = {:.2?}: {:?}", elapsed_item, first_item);

            if elapsed_headers < delay_dur {
                println!(">>> CLASSIFICATION: CATEGORY 1 (STREAM CREATED PRE-MESSAGE) <<<");
            } else {
                println!(">>> CLASSIFICATION: CATEGORY 2 (STREAM CREATED POST-MESSAGE) <<<");
            }
        }
        Err(e) => {
            println!("[Stream Call Error] t_headers = {:.2?}: {:?}", elapsed_headers, e);
            if elapsed_headers < delay_dur {
                println!(">>> CLASSIFICATION: CATEGORY 1 (FAIL PRE-MESSAGE: {}) <<<", e);
            } else {
                println!(">>> CLASSIFICATION: CATEGORY 2 (FAIL POST-MESSAGE: {}) <<<", e);
            }
        }
    }
}

async fn probe_devicestreaming_v1(delay_dur: Duration) {
    use google_cloud_devicestreaming_v1::client::DirectAccessService;
    use google_cloud_devicestreaming_v1::model::AdbMessage;

    println!("\n------------------------------------------------------------");
    println!("PROBING 14: Device Streaming V1 adb_connect");

    let Ok(client) = DirectAccessService::builder().build().await else {
        println!("[ERROR] Failed to build Device Streaming client.");
        return;
    };

    let req = AdbMessage::default();
    let stream = DelayedStream::new(delay_dur, req);

    let t0 = Instant::now();
    let res = client.adb_connect(stream, google_cloud_gax::options::RequestOptions::default()).await;
    let t_headers = Instant::now();
    let elapsed_headers = t_headers.duration_since(t0);

    match res {
        Ok(mut resp_stream) => {
            println!("[Headers Received] t_headers = {:.2?}", elapsed_headers);
            let first_item = resp_stream.next().await;
            let elapsed_item = Instant::now().duration_since(t0);
            println!("[First Item Received] t_first_item = {:.2?}: {:?}", elapsed_item, first_item);

            if elapsed_headers < delay_dur {
                println!(">>> CLASSIFICATION: CATEGORY 1 (STREAM CREATED PRE-MESSAGE) <<<");
            } else {
                println!(">>> CLASSIFICATION: CATEGORY 2 (STREAM CREATED POST-MESSAGE) <<<");
            }
        }
        Err(e) => {
            println!("[Stream Call Error] t_headers = {:.2?}: {:?}", elapsed_headers, e);
            if elapsed_headers < delay_dur {
                println!(">>> CLASSIFICATION: CATEGORY 1 (FAIL PRE-MESSAGE: {}) <<<", e);
            } else {
                println!(">>> CLASSIFICATION: CATEGORY 2 (FAIL POST-MESSAGE: {}) <<<", e);
            }
        }
    }
}

async fn probe_dialogflow_cx_v3(delay_dur: Duration) {
    use google_cloud_dialogflow_cx_v3::client::Sessions;
    use google_cloud_dialogflow_cx_v3::model::StreamingDetectIntentRequest;

    println!("\n------------------------------------------------------------");
    println!("PROBING 15: Dialogflow CX V3 streaming_detect_intent");

    let Ok(client) = Sessions::builder().build().await else {
        println!("[ERROR] Failed to build Dialogflow CX Sessions client.");
        return;
    };

    let req = StreamingDetectIntentRequest::default().set_session("projects/rust-sdk-testing/locations/global/agents/probe/sessions/probe");
    let stream = DelayedStream::new(delay_dur, req);

    let t0 = Instant::now();
    let res = client.streaming_detect_intent(stream, google_cloud_gax::options::RequestOptions::default()).await;
    let t_headers = Instant::now();
    let elapsed_headers = t_headers.duration_since(t0);

    match res {
        Ok(mut resp_stream) => {
            println!("[Headers Received] t_headers = {:.2?}", elapsed_headers);
            let first_item = resp_stream.next().await;
            let elapsed_item = Instant::now().duration_since(t0);
            println!("[First Item Received] t_first_item = {:.2?}: {:?}", elapsed_item, first_item);

            if elapsed_headers < delay_dur {
                println!(">>> CLASSIFICATION: CATEGORY 1 (STREAM CREATED PRE-MESSAGE) <<<");
            } else {
                println!(">>> CLASSIFICATION: CATEGORY 2 (STREAM CREATED POST-MESSAGE) <<<");
            }
        }
        Err(e) => {
            println!("[Stream Call Error] t_headers = {:.2?}: {:?}", elapsed_headers, e);
            if elapsed_headers < delay_dur {
                println!(">>> CLASSIFICATION: CATEGORY 1 (FAIL PRE-MESSAGE: {}) <<<", e);
            } else {
                println!(">>> CLASSIFICATION: CATEGORY 2 (FAIL POST-MESSAGE: {}) <<<", e);
            }
        }
    }
}

async fn probe_dialogflow_v2_sessions(delay_dur: Duration) {
    use google_cloud_dialogflow_v2::client::Sessions;
    use google_cloud_dialogflow_v2::model::StreamingDetectIntentRequest;

    println!("\n------------------------------------------------------------");
    println!("PROBING 16: Dialogflow V2 Sessions.streaming_detect_intent");

    let Ok(client) = Sessions::builder().build().await else {
        println!("[ERROR] Failed to build Dialogflow V2 Sessions client.");
        return;
    };

    let req = StreamingDetectIntentRequest::default().set_session("projects/rust-sdk-testing/agent/sessions/probe");
    let stream = DelayedStream::new(delay_dur, req);

    let t0 = Instant::now();
    let res = client.streaming_detect_intent(stream, google_cloud_gax::options::RequestOptions::default()).await;
    let t_headers = Instant::now();
    let elapsed_headers = t_headers.duration_since(t0);

    match res {
        Ok(mut resp_stream) => {
            println!("[Headers Received] t_headers = {:.2?}", elapsed_headers);
            let first_item = resp_stream.next().await;
            let elapsed_item = Instant::now().duration_since(t0);
            println!("[First Item Received] t_first_item = {:.2?}: {:?}", elapsed_item, first_item);

            if elapsed_headers < delay_dur {
                println!(">>> CLASSIFICATION: CATEGORY 1 (STREAM CREATED PRE-MESSAGE) <<<");
            } else {
                println!(">>> CLASSIFICATION: CATEGORY 2 (STREAM CREATED POST-MESSAGE) <<<");
            }
        }
        Err(e) => {
            println!("[Stream Call Error] t_headers = {:.2?}: {:?}", elapsed_headers, e);
            if elapsed_headers < delay_dur {
                println!(">>> CLASSIFICATION: CATEGORY 1 (FAIL PRE-MESSAGE: {}) <<<", e);
            } else {
                println!(">>> CLASSIFICATION: CATEGORY 2 (FAIL POST-MESSAGE: {}) <<<", e);
            }
        }
    }
}

async fn probe_dialogflow_v2_participants(delay_dur: Duration) {
    use google_cloud_dialogflow_v2::client::Participants;
    use google_cloud_dialogflow_v2::model::StreamingAnalyzeContentRequest;

    println!("\n------------------------------------------------------------");
    println!("PROBING 17: Dialogflow V2 Participants.streaming_analyze_content");

    let Ok(client) = Participants::builder().build().await else {
        println!("[ERROR] Failed to build Dialogflow V2 Participants client.");
        return;
    };

    let req = StreamingAnalyzeContentRequest::default().set_participant("projects/rust-sdk-testing/locations/global/conversations/probe/participants/probe");
    let stream = DelayedStream::new(delay_dur, req);

    let t0 = Instant::now();
    let res = client.streaming_analyze_content(stream, google_cloud_gax::options::RequestOptions::default()).await;
    let t_headers = Instant::now();
    let elapsed_headers = t_headers.duration_since(t0);

    match res {
        Ok(mut resp_stream) => {
            println!("[Headers Received] t_headers = {:.2?}", elapsed_headers);
            let first_item = resp_stream.next().await;
            let elapsed_item = Instant::now().duration_since(t0);
            println!("[First Item Received] t_first_item = {:.2?}: {:?}", elapsed_item, first_item);

            if elapsed_headers < delay_dur {
                println!(">>> CLASSIFICATION: CATEGORY 1 (STREAM CREATED PRE-MESSAGE) <<<");
            } else {
                println!(">>> CLASSIFICATION: CATEGORY 2 (STREAM CREATED POST-MESSAGE) <<<");
            }
        }
        Err(e) => {
            println!("[Stream Call Error] t_headers = {:.2?}: {:?}", elapsed_headers, e);
            if elapsed_headers < delay_dur {
                println!(">>> CLASSIFICATION: CATEGORY 1 (FAIL PRE-MESSAGE: {}) <<<", e);
            } else {
                println!(">>> CLASSIFICATION: CATEGORY 2 (FAILED POST-MESSAGE: {:?}) <<<", e);
            }
        }
    }
}

async fn probe_visionai_v1_send_packets(delay_dur: Duration) {
    use google_cloud_visionai_v1::client::StreamingService;
    use google_cloud_visionai_v1::model::SendPacketsRequest;

    println!("\n------------------------------------------------------------");
    println!("PROBING 18: Vision AI V1 StreamingService.send_packets");

    let Ok(client) = StreamingService::builder().build().await else {
        println!("[ERROR] Failed to build Vision AI StreamingService client.");
        return;
    };

    let req = SendPacketsRequest::default();
    let stream = DelayedStream::new(delay_dur, req);

    let t0 = Instant::now();
    let res = client.send_packets(stream, google_cloud_gax::options::RequestOptions::default()).await;
    let t_headers = Instant::now();
    let elapsed_headers = t_headers.duration_since(t0);

    match res {
        Ok(mut resp_stream) => {
            println!("[Headers Received] t_headers = {:.2?}", elapsed_headers);
            let first_item = resp_stream.next().await;
            let elapsed_item = Instant::now().duration_since(t0);
            println!("[First Item Received] t_first_item = {:.2?}: {:?}", elapsed_item, first_item);

            if elapsed_headers < delay_dur {
                println!(">>> CLASSIFICATION: CATEGORY 1 (STREAM CREATED PRE-MESSAGE) <<<");
            } else {
                println!(">>> CLASSIFICATION: CATEGORY 2 (STREAM CREATED POST-MESSAGE) <<<");
            }
        }
        Err(e) => {
            println!("[Stream Call Error] t_headers = {:.2?}: {:?}", elapsed_headers, e);
            if elapsed_headers < delay_dur {
                println!(">>> CLASSIFICATION: CATEGORY 1 (FAIL PRE-MESSAGE: {}) <<<", e);
            } else {
                println!(">>> CLASSIFICATION: CATEGORY 2 (FAIL POST-MESSAGE: {}) <<<", e);
            }
        }
    }
}

async fn probe_visionai_v1_receive_packets(delay_dur: Duration) {
    use google_cloud_visionai_v1::client::StreamingService;
    use google_cloud_visionai_v1::model::ReceivePacketsRequest;

    println!("\n------------------------------------------------------------");
    println!("PROBING 19: Vision AI V1 StreamingService.receive_packets");

    let Ok(client) = StreamingService::builder().build().await else {
        println!("[ERROR] Failed to build Vision AI StreamingService client.");
        return;
    };

    let req = ReceivePacketsRequest::default();
    let stream = DelayedStream::new(delay_dur, req);

    let t0 = Instant::now();
    let res = client.receive_packets(stream, google_cloud_gax::options::RequestOptions::default()).await;
    let t_headers = Instant::now();
    let elapsed_headers = t_headers.duration_since(t0);

    match res {
        Ok(mut resp_stream) => {
            println!("[Headers Received] t_headers = {:.2?}", elapsed_headers);
            let first_item = resp_stream.next().await;
            let elapsed_item = Instant::now().duration_since(t0);
            println!("[First Item Received] t_first_item = {:.2?}: {:?}", elapsed_item, first_item);

            if elapsed_headers < delay_dur {
                println!(">>> CLASSIFICATION: CATEGORY 1 (STREAM CREATED PRE-MESSAGE) <<<");
            } else {
                println!(">>> CLASSIFICATION: CATEGORY 2 (STREAM CREATED POST-MESSAGE) <<<");
            }
        }
        Err(e) => {
            println!("[Stream Call Error] t_headers = {:.2?}: {:?}", elapsed_headers, e);
            if elapsed_headers < delay_dur {
                println!(">>> CLASSIFICATION: CATEGORY 1 (FAIL PRE-MESSAGE: {}) <<<", e);
            } else {
                println!(">>> CLASSIFICATION: CATEGORY 2 (FAIL POST-MESSAGE: {}) <<<", e);
            }
        }
    }
}

async fn probe_visionai_v1_receive_events(delay_dur: Duration) {
    use google_cloud_visionai_v1::client::StreamingService;
    use google_cloud_visionai_v1::model::ReceiveEventsRequest;

    println!("\n------------------------------------------------------------");
    println!("PROBING 20: Vision AI V1 StreamingService.receive_events");

    let Ok(client) = StreamingService::builder().build().await else {
        println!("[ERROR] Failed to build Vision AI StreamingService client.");
        return;
    };

    let req = ReceiveEventsRequest::default();
    let stream = DelayedStream::new(delay_dur, req);

    let t0 = Instant::now();
    let res = client.receive_events(stream, google_cloud_gax::options::RequestOptions::default()).await;
    let t_headers = Instant::now();
    let elapsed_headers = t_headers.duration_since(t0);

    match res {
        Ok(mut resp_stream) => {
            println!("[Headers Received] t_headers = {:.2?}", elapsed_headers);
            let first_item = resp_stream.next().await;
            let elapsed_item = Instant::now().duration_since(t0);
            println!("[First Item Received] t_first_item = {:.2?}: {:?}", elapsed_item, first_item);

            if elapsed_headers < delay_dur {
                println!(">>> CLASSIFICATION: CATEGORY 1 (STREAM CREATED PRE-MESSAGE) <<<");
            } else {
                println!(">>> CLASSIFICATION: CATEGORY 2 (STREAM CREATED POST-MESSAGE) <<<");
            }
        }
        Err(e) => {
            println!("[Stream Call Error] t_headers = {:.2?}: {:?}", elapsed_headers, e);
            if elapsed_headers < delay_dur {
                println!(">>> CLASSIFICATION: CATEGORY 1 (FAIL PRE-MESSAGE: {}) <<<", e);
            } else {
                println!(">>> CLASSIFICATION: CATEGORY 2 (FAIL POST-MESSAGE: {}) <<<", e);
            }
        }
    }
}

async fn probe_visionai_v1_ingest_asset(delay_dur: Duration) {
    use google_cloud_visionai_v1::client::Warehouse;
    use google_cloud_visionai_v1::model::IngestAssetRequest;

    println!("\n------------------------------------------------------------");
    println!("PROBING 21: Vision AI V1 Warehouse.ingest_asset");

    let Ok(client) = Warehouse::builder().build().await else {
        println!("[ERROR] Failed to build Vision AI Warehouse client.");
        return;
    };

    let req = IngestAssetRequest::default();
    let stream = DelayedStream::new(delay_dur, req);

    let t0 = Instant::now();
    let res = client.ingest_asset(stream, google_cloud_gax::options::RequestOptions::default()).await;
    let t_headers = Instant::now();
    let elapsed_headers = t_headers.duration_since(t0);

    match res {
        Ok(mut resp_stream) => {
            println!("[Headers Received] t_headers = {:.2?}", elapsed_headers);
            let first_item = resp_stream.next().await;
            let elapsed_item = Instant::now().duration_since(t0);
            println!("[First Item Received] t_first_item = {:.2?}: {:?}", elapsed_item, first_item);

            if elapsed_headers < delay_dur {
                println!(">>> CLASSIFICATION: CATEGORY 1 (STREAM CREATED PRE-MESSAGE) <<<");
            } else {
                println!(">>> CLASSIFICATION: CATEGORY 2 (STREAM CREATED POST-MESSAGE) <<<");
            }
        }
        Err(e) => {
            println!("[Stream Call Error] t_headers = {:.2?}: {:?}", elapsed_headers, e);
            if elapsed_headers < delay_dur {
                println!(">>> CLASSIFICATION: CATEGORY 1 (FAIL PRE-MESSAGE: {}) <<<", e);
            } else {
                println!(">>> CLASSIFICATION: CATEGORY 2 (FAIL POST-MESSAGE: {}) <<<", e);
            }
        }
    }
}
