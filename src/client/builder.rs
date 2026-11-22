//! Builder for CastClient with advanced configuration.

use crate::CastClient;
use crate::channel::ns;
use crate::client::framing;
use crate::error::Result;
use crate::proto::CastMessage;
use std::sync::Arc;
use std::time::Duration;

/// Builder for creating a [`CastClient`] with custom configuration.
#[must_use]
pub struct CastClientBuilder {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) heartbeat_interval: Duration,
    pub(crate) heartbeat_timeout: Duration,
    pub(crate) request_timeout: Duration,
    pub(crate) connect_timeout: Duration,
    pub(crate) auto_reconnect: bool,
    pub(crate) max_reconnect_attempts: u32,
    pub(crate) reconnect_delay: Duration,
    pub(crate) event_buffer_size: usize,
    pub(crate) verify_tls: bool,
    pub(crate) adopt_broadcast_media_sessions: bool,
    pub(crate) first_message: Option<FirstMessage>,
}

/// A binary message sent before any other on every connection
/// (see [`CastClientBuilder::first_message`]).
#[derive(Clone)]
pub(crate) struct FirstMessage {
    namespace: String,
    destination: String,
    payload: Arc<dyn Fn() -> Vec<u8> + Send + Sync>,
}

impl FirstMessage {
    /// Build the message for a new connection, with a fresh payload.
    pub(crate) fn build(&self) -> CastMessage {
        framing::build_binary_message(
            &self.namespace,
            ns::SENDER_ID,
            &self.destination,
            (self.payload)(),
        )
    }
}

impl CastClientBuilder {
    /// Create a new builder with default configuration for the given host and port.
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
            heartbeat_interval: Duration::from_secs(5),
            heartbeat_timeout: Duration::from_secs(15),
            request_timeout: Duration::from_secs(10),
            connect_timeout: Duration::from_secs(10),
            auto_reconnect: true,
            max_reconnect_attempts: 5,
            reconnect_delay: Duration::from_secs(2),
            event_buffer_size: 64,
            verify_tls: false,
            adopt_broadcast_media_sessions: true,
            first_message: None,
        }
    }

    /// Set the interval between heartbeat PING messages.
    pub fn heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = interval;
        self
    }

    /// Set the timeout for heartbeat liveness detection.
    ///
    /// If no message is received from the device within this duration,
    /// the connection is considered dead and auto-reconnect triggers.
    pub fn heartbeat_timeout(mut self, timeout: Duration) -> Self {
        self.heartbeat_timeout = timeout;
        self
    }

    /// Set the timeout for request-response operations.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Set the timeout for establishing a connection (TCP connect and TLS handshake).
    ///
    /// Applies to the initial connection and to every reconnection attempt.
    /// Defaults to 10 seconds; must be non-zero or every connection attempt fails.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Enable or disable automatic reconnection on connection loss.
    pub fn auto_reconnect(mut self, enabled: bool) -> Self {
        self.auto_reconnect = enabled;
        self
    }

    /// Set the maximum number of reconnection attempts.
    pub fn max_reconnect_attempts(mut self, max: u32) -> Self {
        self.max_reconnect_attempts = max;
        self
    }

    /// Set the base delay between reconnection attempts.
    pub fn reconnect_delay(mut self, delay: Duration) -> Self {
        self.reconnect_delay = delay;
        self
    }

    /// Set the buffer size for the event channel.
    pub fn event_buffer_size(mut self, size: usize) -> Self {
        self.event_buffer_size = size;
        self
    }

    /// Enable or disable TLS certificate verification.
    /// Default: `false` (Cast devices use self-signed certificates).
    pub fn verify_tls(mut self, verify: bool) -> Self {
        self.verify_tls = verify;
        self
    }

    /// Send a message with a binary payload before any other message on every
    /// connection, including reconnections.
    ///
    /// `payload` is called once per connection, right after the TLS handshake,
    /// so each connection can carry fresh content such as a nonce. The message
    /// is sent ahead of the `CONNECT` to `receiver-0`, heartbeats and any
    /// command. Cast senders send their device-authentication challenge this way:
    /// a `DeviceAuthMessage` on `urn:x-cast:com.google.cast.tp.deviceauth` to
    /// `receiver-0`. Replies arrive as [`CastEvent::RawBinaryMessage`](crate::CastEvent::RawBinaryMessage).
    ///
    /// ```no_run
    /// # async fn example() -> oxicast::Result<()> {
    /// let client = oxicast::CastClient::builder("192.168.1.100", 8009)
    ///     .first_message("urn:x-cast:com.google.cast.tp.deviceauth", "receiver-0", || {
    ///         vec![/* an encoded DeviceAuthMessage with a fresh nonce */]
    ///     })
    ///     .connect()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn first_message(
        mut self,
        namespace: impl Into<String>,
        destination: impl Into<String>,
        payload: impl Fn() -> Vec<u8> + Send + Sync + 'static,
    ) -> Self {
        self.first_message = Some(FirstMessage {
            namespace: namespace.into(),
            destination: destination.into(),
            payload: Arc::new(payload),
        });
        self
    }

    /// Whether playback commands follow media sessions announced in status broadcasts.
    ///
    /// Default: `true`. Every `MEDIA_STATUS` updates the media session that
    /// `play()`, `pause()`, `seek()` and the other media commands target, even
    /// one started by another sender.
    ///
    /// With `false`, commands only target a media session this client started
    /// with an accepted [`load_media`](CastClient::load_media) or
    /// [`queue_load`](CastClient::queue_load), or found with
    /// [`join_session`](CastClient::join_session). If another sender then loads
    /// different media, commands keep the old session id and the device rejects
    /// them, instead of controlling the other sender's media. A `join_session`
    /// whose status fetch fails leaves commands without a session. Status
    /// updates and events are reported either way.
    pub fn adopt_broadcast_media_sessions(mut self, adopt: bool) -> Self {
        self.adopt_broadcast_media_sessions = adopt;
        self
    }

    /// Connect to the Cast device with the configured settings.
    pub async fn connect(self) -> Result<CastClient> {
        CastClient::from_builder(&self).await
    }
}
