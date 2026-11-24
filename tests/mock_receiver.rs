//! End-to-end tests against an in-process mock Cast receiver over TLS.
//!
//! The mock speaks the real wire format (TLS + length-prefixed protobuf
//! CastMessage with JSON payloads) so these tests exercise the client's
//! connection, routing and request-correlation logic.

mod common;

use common::*;
use serde_json::json;
use std::time::Duration;
use tokio::sync::mpsc;

#[tokio::test]
async fn join_session_fetches_status_so_controls_work() {
    let (port, mut seen) = start_mock_receiver().await;
    let client = connect(port).await;

    let status = client.receiver_status().await.expect("receiver status");
    let app1 = status.applications.iter().find(|a| a.transport_id == "app-1").expect("app-1");

    let media = client.join_session(app1).await.expect("join").expect("media status");
    assert_eq!(media.media_session_id, 11);
    assert_eq!(
        media.media.as_ref().and_then(|m| m.content_url.as_deref()),
        Some("http://example.com/app-1.mp4")
    );

    let connect_msg = expect_msg(&mut seen, |m| m.destination == "app-1").await;
    assert_eq!(connect_msg.namespace, NS_CONNECTION);
    assert_eq!(connect_msg.payload["type"], "CONNECT");

    // No explicit media_status() call: pause must use the joined session.
    let paused = client.pause().await.expect("pause after join");
    assert_eq!(paused.media_session_id, 11);
    let pause = expect_msg(&mut seen, |m| m.payload["type"] == "PAUSE").await;
    assert_eq!(pause.destination, "app-1");
    assert_eq!(pause.payload["mediaSessionId"], 11);

    client.disconnect().await.expect("disconnect");
}

#[tokio::test]
async fn join_another_session_closes_previous_and_drops_stale_media_session() {
    let (port, mut seen) = start_mock_receiver().await;
    let client = connect(port).await;

    let status = client.receiver_status().await.expect("receiver status");
    let app1 = status.applications.iter().find(|a| a.transport_id == "app-1").expect("app-1");
    let app2 = status.applications.iter().find(|a| a.transport_id == "app-2").expect("app-2");

    client.join_session(app1).await.expect("join app-1");
    let media = client.join_session(app2).await.expect("join app-2").expect("media status");
    assert_eq!(media.media_session_id, 22);

    let close = expect_msg(&mut seen, |m| m.payload["type"] == "CLOSE").await;
    assert_eq!(close.destination, "app-1");
    assert_eq!(close.namespace, NS_CONNECTION);

    client.pause().await.expect("pause after second join");
    let pause = expect_msg(&mut seen, |m| m.payload["type"] == "PAUSE").await;
    assert_eq!(pause.destination, "app-2");
    assert_eq!(pause.payload["mediaSessionId"], 22, "must not reuse app-1's session id");

    client.disconnect().await.expect("disconnect");
}

/// Collects everything the client sent until the mock connection closes.
async fn drain(rx: &mut mpsc::UnboundedReceiver<Received>) -> Vec<Received> {
    let mut out = Vec::new();
    while let Ok(Some(msg)) = tokio::time::timeout(Duration::from_secs(1), rx.recv()).await {
        out.push(msg);
    }
    out
}

#[tokio::test]
async fn close_leaves_playback_running() {
    let (port, mut seen) = start_mock_receiver().await;
    let client = connect(port).await;

    let status = client.receiver_status().await.expect("receiver status");
    let app1 = status.applications.iter().find(|a| a.transport_id == "app-1").expect("app-1");
    client.join_session(app1).await.expect("join");

    client.close().await.expect("close");
    assert!(!client.is_connected());

    let sent = drain(&mut seen).await;
    assert!(
        !sent.iter().any(|m| m.payload["type"] == "STOP"),
        "close() must not stop media: {sent:?}"
    );
    assert!(sent.iter().any(|m| m.payload["type"] == "CLOSE" && m.destination == "app-1"));
    assert!(sent.iter().any(|m| m.payload["type"] == "CLOSE" && m.destination == "receiver-0"));
}

#[tokio::test]
async fn disconnect_still_stops_media() {
    let (port, mut seen) = start_mock_receiver().await;
    let client = connect(port).await;

    let status = client.receiver_status().await.expect("receiver status");
    let app1 = status.applications.iter().find(|a| a.transport_id == "app-1").expect("app-1");
    client.join_session(app1).await.expect("join");

    client.disconnect().await.expect("disconnect");

    let sent = drain(&mut seen).await;
    let stop =
        sent.iter().position(|m| m.payload["type"] == "STOP").expect("disconnect sends STOP");
    assert_eq!(sent[stop].destination, "app-1");
    assert_eq!(sent[stop].payload["mediaSessionId"], 11);
    let close = sent
        .iter()
        .position(|m| m.payload["type"] == "CLOSE" && m.destination == "receiver-0")
        .expect("disconnect sends CLOSE to receiver-0");
    assert!(stop < close, "STOP must be sent before CLOSE");
    // Behaviour downstream users rely on: disconnect() does not CLOSE the app transport.
    assert!(!sent.iter().any(|m| m.payload["type"] == "CLOSE" && m.destination == "app-1"));
}

#[tokio::test]
async fn join_session_succeeds_when_status_fetch_times_out() {
    let (port, mut seen) = start_mock_receiver().await;
    let client = oxicast::CastClient::builder("127.0.0.1", port)
        .request_timeout(Duration::from_millis(300))
        .connect()
        .await
        .expect("connect");

    let status = client.receiver_status().await.expect("receiver status");
    let silent = status.applications.iter().find(|a| a.transport_id == "silent-app").expect("app");

    let media = client.join_session(silent).await.expect("join must not fail on status timeout");
    assert!(media.is_none());
    let connect = expect_msg(&mut seen, |m| m.destination == "silent-app").await;
    assert_eq!(connect.payload["type"], "CONNECT");
    client.disconnect().await.expect("disconnect");
}

#[tokio::test]
async fn connects_to_ipv6_literals_with_and_without_brackets() {
    for host in ["::1", "[::1]"] {
        let Some((port, _seen)) = start_mock_receiver_on("[::1]:0").await else {
            eprintln!("IPv6 loopback unavailable, skipping");
            return;
        };
        let client = oxicast::CastClient::builder(host, port)
            .request_timeout(Duration::from_secs(5))
            .connect()
            .await
            .unwrap_or_else(|e| panic!("connect to {host}: {e}"));
        client.receiver_status().await.expect("receiver status over IPv6");
        client.close().await.expect("close");
    }
}

#[tokio::test]
async fn connection_loss_is_reported_exactly_once() {
    let (port, _seen) = start_mock_receiver().await;
    let client = oxicast::CastClient::builder("127.0.0.1", port)
        .auto_reconnect(false)
        .request_timeout(Duration::from_millis(500))
        .connect()
        .await
        .expect("connect");

    client
        .send_raw_no_reply(NS_TEST, "receiver-0", json!({"type": "DROP_CONNECTION"}))
        .await
        .expect("send drop trigger");
    // Keep writing so both the reader and the writer hit the dead socket.
    for _ in 0..20 {
        let _ = client
            .send_raw_no_reply(NS_RECEIVER, "receiver-0", json!({"type": "GET_STATUS"}))
            .await;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let mut disconnects = 0;
    while let Ok(Some(event)) =
        tokio::time::timeout(Duration::from_millis(500), client.next_event()).await
    {
        if matches!(event, oxicast::CastEvent::Disconnected(_)) {
            disconnects += 1;
        }
    }
    assert_eq!(disconnects, 1, "a lost connection must be reported exactly once");
    assert!(!client.is_connected());
}

#[tokio::test]
async fn stop_and_close_matches_disconnect() {
    let (port, mut seen) = start_mock_receiver().await;
    let client = connect(port).await;
    let status = client.receiver_status().await.expect("receiver status");
    let app1 = status.applications.iter().find(|a| a.transport_id == "app-1").expect("app-1");
    client.join_session(app1).await.expect("join");

    client.stop_and_close().await.expect("stop_and_close");

    let sent = drain(&mut seen).await;
    let stop = sent.iter().position(|m| m.payload["type"] == "STOP").expect("sends STOP");
    let close = sent
        .iter()
        .position(|m| m.payload["type"] == "CLOSE" && m.destination == "receiver-0")
        .expect("sends CLOSE to receiver-0");
    assert!(stop < close);
    assert!(!sent.iter().any(|m| m.payload["type"] == "CLOSE" && m.destination == "app-1"));
}

#[tokio::test]
async fn disconnect_reports_a_single_event_when_the_device_vanishes_meanwhile() {
    let (port, _seen) = start_mock_receiver().await;
    let client = oxicast::CastClient::builder("127.0.0.1", port)
        .auto_reconnect(false)
        .request_timeout(Duration::from_millis(500))
        .connect()
        .await
        .expect("connect");
    let status = client.receiver_status().await.expect("receiver status");
    let app = status.applications.iter().find(|a| a.transport_id == "drop-app").expect("drop-app");
    client.join_session(app).await.expect("join");

    // The mock resets the connection when it receives STOP.
    client.disconnect().await.expect("disconnect");

    let mut disconnects = Vec::new();
    while let Ok(Some(event)) =
        tokio::time::timeout(Duration::from_millis(500), client.next_event()).await
    {
        if let oxicast::CastEvent::Disconnected(reason) = event {
            disconnects.push(reason);
        }
    }
    assert_eq!(disconnects, vec![None], "only the terminal Disconnected(None) of disconnect()");
}

#[tokio::test]
async fn reports_the_connection_addresses() {
    let (port, _seen) = start_mock_receiver().await;
    let client = connect(port).await;

    assert_eq!(client.peer_addr(), std::net::SocketAddr::from(([127, 0, 0, 1], port)));
    let local = client.local_addr();
    assert_eq!(local.ip(), std::net::IpAddr::from([127, 0, 0, 1]));
    assert_ne!(local.port(), port);

    client.disconnect().await.expect("disconnect");
    assert_eq!(client.local_addr(), local, "keeps the last connection's address after close");
}

/// Joins app-1 (media session 11), then has the mock announce media session 99
/// on app-1, as when another sender loads media, and pauses.
async fn pause_after_another_sender_loads(client: &oxicast::CastClient) {
    let status = client.receiver_status().await.expect("receiver status");
    let app1 = status.applications.iter().find(|a| a.transport_id == "app-1").expect("app-1");
    client.join_session(app1).await.expect("join").expect("media status");

    let mut media = client.watch_media_status();
    client
        .send_raw_no_reply(
            NS_TEST,
            "receiver-0",
            json!({"type": "BROADCAST_MEDIA_STATUS", "from": "app-1", "mediaSessionId": 99}),
        )
        .await
        .expect("send broadcast trigger");
    tokio::time::timeout(
        Duration::from_secs(5),
        media.wait_for(|s| s.as_ref().is_some_and(|s| s.media_session_id == 99)),
    )
    .await
    .expect("broadcast reported")
    .expect("watch open");

    client.pause().await.expect("pause");
}

#[tokio::test]
async fn commands_follow_broadcast_media_sessions_by_default() {
    let (port, mut seen) = start_mock_receiver().await;
    let client = connect(port).await;

    pause_after_another_sender_loads(&client).await;
    let pause = expect_msg(&mut seen, |m| m.payload["type"] == "PAUSE").await;
    assert_eq!(pause.payload["mediaSessionId"], 99);

    client.disconnect().await.expect("disconnect");
}

#[tokio::test]
async fn commands_keep_the_joined_media_session_when_not_adopting_broadcasts() {
    let (port, mut seen) = start_mock_receiver().await;
    let client = oxicast::CastClient::builder("127.0.0.1", port)
        .request_timeout(Duration::from_secs(5))
        .adopt_broadcast_media_sessions(false)
        .connect()
        .await
        .expect("connect");

    pause_after_another_sender_loads(&client).await;
    let pause = expect_msg(&mut seen, |m| m.payload["type"] == "PAUSE").await;
    assert_eq!(pause.payload["mediaSessionId"], 11, "must not follow another sender's session");

    client.close().await.expect("close");
}

#[tokio::test]
async fn a_rejected_load_does_not_replace_the_session_commands_target() {
    let (port, mut seen) = start_mock_receiver().await;
    let client = oxicast::CastClient::builder("127.0.0.1", port)
        .request_timeout(Duration::from_secs(5))
        .adopt_broadcast_media_sessions(false)
        .connect()
        .await
        .expect("connect");

    let status = client.receiver_status().await.expect("receiver status");
    let app = status.applications.iter().find(|a| a.transport_id == "reject-app").expect("app");
    client.join_session(app).await.expect("join").expect("media status");

    let media = oxicast::MediaInfo::new("http://example.com/x.mp4", "video/mp4");
    assert!(client.load_media(&media, true, 0.0, None).await.is_err(), "the device rejected it");

    client.pause().await.expect("pause");
    let pause = expect_msg(&mut seen, |m| m.payload["type"] == "PAUSE").await;
    assert_eq!(
        pause.payload["mediaSessionId"], 55,
        "must keep the session from before the rejected load"
    );

    client.close().await.expect("close");
}

#[tokio::test]
async fn binary_messages_round_trip_as_raw_binary_events() {
    let (port, mut seen) = start_mock_receiver().await;
    let client = connect(port).await;

    client.send_raw_binary(NS_TEST, "receiver-0", vec![1, 2, 3]).await.expect("send binary");
    let sent = expect_msg(&mut seen, |m| m.namespace == NS_TEST).await;
    assert_eq!(sent.binary.as_deref(), Some(&[1, 2, 3][..]));
    assert_eq!(sent.destination, "receiver-0");

    let reply = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match client.next_event().await.expect("event stream open") {
                oxicast::CastEvent::RawBinaryMessage {
                    connection,
                    namespace,
                    source,
                    destination,
                    payload,
                } => {
                    assert_eq!(connection, 0);
                    return (namespace, source, destination, payload);
                }
                _ => continue,
            }
        }
    })
    .await
    .expect("binary reply");
    assert_eq!(reply, (NS_TEST.into(), "receiver-0".into(), "sender-0".into(), vec![3, 2, 1]));

    client.disconnect().await.expect("disconnect");
}

/// The next message the client sent to a reconnectable receiver, with its connection number.
async fn next(rx: &mut mpsc::UnboundedReceiver<(usize, Received)>) -> (usize, Received) {
    tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out")
        .expect("receiver running")
}

#[tokio::test]
async fn first_message_goes_out_before_anything_else_on_every_connection() {
    let (port, mut seen) = start_reconnectable_receiver().await;
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicU8::new(0));
    let counter = calls.clone();
    let client = oxicast::CastClient::builder("127.0.0.1", port)
        .auto_reconnect(false)
        .request_timeout(Duration::from_secs(5))
        .first_message(NS_TEST, "receiver-0", move |_connection| {
            vec![counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed)]
        })
        .connect()
        .await
        .expect("connect");

    let (connection, first) = next(&mut seen).await;
    assert_eq!((connection, first.namespace.as_str()), (0, NS_TEST));
    assert_eq!(first.binary, Some(vec![0]), "a payload made for this connection");
    let (_, second) = next(&mut seen).await;
    assert_eq!(
        (second.namespace.as_str(), &second.payload["type"]),
        (NS_CONNECTION, &json!("CONNECT"))
    );

    client
        .send_raw_no_reply(NS_TEST, "receiver-0", json!({"type": "DROP_CONNECTION"}))
        .await
        .expect("send drop trigger");
    tokio::time::timeout(Duration::from_secs(5), async {
        while client.is_connected() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("connection dropped");
    client.reconnect().await.expect("reconnect");

    let (connection, first) = loop {
        let (connection, msg) = next(&mut seen).await;
        if connection == 1 {
            break (connection, msg);
        }
    };
    assert_eq!((connection, first.namespace.as_str()), (1, NS_TEST));
    assert_eq!(first.binary, Some(vec![1]), "a fresh payload for the new connection");
    let (_, second) = next(&mut seen).await;
    assert_eq!(second.payload["type"], "CONNECT");

    client.close().await.expect("close");
}

#[tokio::test]
async fn reports_the_device_tls_certificate() {
    use rustls::pki_types::CertificateDer;
    use rustls::pki_types::pem::PemObject;

    let (port, _seen) = start_mock_receiver().await;
    let client = connect(port).await;

    let expected = CertificateDer::from_pem_slice(include_bytes!("fixtures/mock_device.crt"))
        .expect("fixture certificate");
    assert_eq!(client.peer_certificate().as_deref(), Some(expected.as_ref()));

    client.disconnect().await.expect("disconnect");
    assert_eq!(
        client.peer_certificate().as_deref(),
        Some(expected.as_ref()),
        "keeps the last connection's certificate after close"
    );
}

#[tokio::test]
async fn the_device_certificate_follows_a_reconnect() {
    use rustls::pki_types::CertificateDer;
    use rustls::pki_types::pem::PemObject;
    let der = |pem: &[u8]| CertificateDer::from_pem_slice(pem).expect("fixture").to_vec();
    let first = der(include_bytes!("fixtures/mock_device.crt"));
    let second = der(include_bytes!("fixtures/mock_device_2.crt"));

    let port = start_receiver_changing_certificate().await;
    let client = oxicast::CastClient::builder("127.0.0.1", port)
        .auto_reconnect(false)
        .connect()
        .await
        .expect("connect");
    assert_eq!(client.peer_certificate(), Some(first));

    client
        .send_raw_no_reply(NS_TEST, "receiver-0", json!({"type": "DROP_CONNECTION"}))
        .await
        .expect("send drop trigger");
    tokio::time::timeout(Duration::from_secs(5), async {
        while client.is_connected() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("connection dropped");
    client.reconnect().await.expect("reconnect");
    assert_eq!(client.peer_certificate(), Some(second), "the new connection's certificate");
    client.close().await.expect("close");
}

#[tokio::test]
async fn oversized_binary_payloads_are_refused_before_they_are_queued() {
    let (port, _seen) = start_mock_receiver().await;
    let client = connect(port).await;
    let result = client.send_raw_binary(NS_TEST, "receiver-0", vec![0; 70_000]).await;
    assert!(matches!(result, Err(oxicast::Error::Framing(_))), "{result:?}");
    assert!(client.is_connected(), "the connection survives");
    client.receiver_status().await.expect("still usable");
    client.disconnect().await.expect("disconnect");
}

#[tokio::test]
async fn binary_replies_queued_before_a_reconnect_keep_their_connection_number() {
    let (port, mut seen) = start_reconnectable_receiver().await;
    let client = oxicast::CastClient::builder("127.0.0.1", port)
        .auto_reconnect(false)
        .request_timeout(Duration::from_secs(5))
        .first_message(NS_TEST, "receiver-0", |connection| vec![connection as u8, 0xAA])
        .connect()
        .await
        .expect("connect");
    assert_eq!(client.connection_number(), 0);
    let _ = next(&mut seen).await; // the first message, echoed on connection 0

    // Nobody reads events while the connection drops and comes back.
    client
        .send_raw_no_reply(NS_TEST, "receiver-0", json!({"type": "DROP_CONNECTION"}))
        .await
        .expect("send drop trigger");
    tokio::time::timeout(Duration::from_secs(5), async {
        while client.is_connected() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("connection dropped");
    client.reconnect().await.expect("reconnect");
    assert_eq!(client.connection_number(), 1);

    let mut replies = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        while replies.len() < 2 {
            if let Some(oxicast::CastEvent::RawBinaryMessage { connection, payload, .. }) =
                client.next_event().await
            {
                replies.push((connection, payload));
            }
        }
    })
    .await
    .expect("both replies");
    assert_eq!(replies, vec![(0, vec![0xAA, 0]), (1, vec![0xAA, 1])]);
    client.close().await.expect("close");
}

#[tokio::test]
async fn an_oversized_first_message_fails_the_connection_attempt() {
    let (port, _seen) = start_reconnectable_receiver().await;
    let result = oxicast::CastClient::builder("127.0.0.1", port)
        .first_message(NS_TEST, "receiver-0", |_| vec![0; 70_000])
        .connect()
        .await;
    assert!(matches!(result, Err(oxicast::Error::Framing(_))));
}

#[tokio::test]
async fn text_tracks_load_on_and_turn_off_without_a_reload() {
    use oxicast::{MediaInfo, MediaTrack};
    let (port, mut seen) = start_mock_receiver().await;
    let client = connect(port).await;
    let status = client.receiver_status().await.expect("receiver status");
    let app1 = status.applications.iter().find(|a| a.transport_id == "app-1").expect("app-1");
    client.join_session(app1).await.expect("join");

    let media = MediaInfo::new("http://example.com/v.mp4", "video/mp4")
        .tracks(vec![MediaTrack::subtitles(1, "http://example.com/subs.vtt", "text/vtt", "en")]);
    let loaded =
        client.load_media_with_active_tracks(&media, true, 0.0, &[1], None).await.expect("load");
    assert_eq!(loaded.media_session_id, 11);
    let load = expect_msg(&mut seen, |m| m.payload["type"] == "LOAD").await;
    assert_eq!(load.destination, "app-1");
    assert_eq!(load.payload["media"]["tracks"][0]["trackContentId"], "http://example.com/subs.vtt");
    assert_eq!(load.payload["media"]["tracks"][0]["language"], "en");
    assert_eq!(load.payload["activeTrackIds"], json!([1]));

    client.edit_tracks_info(&[]).await.expect("off");
    let edit = expect_msg(&mut seen, |m| m.payload["type"] == "EDIT_TRACKS_INFO").await;
    assert_eq!(edit.destination, "app-1");
    assert_eq!(edit.payload["mediaSessionId"], 11);
    assert_eq!(edit.payload["activeTrackIds"], json!([]));

    // The mock rejects an edit naming track 99 as a receiver whose media ended would.
    let refused = client.edit_tracks_info(&[99]).await;
    assert!(matches!(refused, Err(oxicast::Error::InvalidPlayerState { .. })), "{refused:?}");
    client.disconnect().await.expect("disconnect");
}
