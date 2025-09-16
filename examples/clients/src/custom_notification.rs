use anyhow::Result;
use rmcp::{
    ClientHandler, ServerHandler, ServiceExt,
    model::*,
    service::{NotificationContext, RoleClient, RoleServer},
};
use serde_json::json;
use tokio::sync::mpsc;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

pub struct TestServer {
    notification_tx: mpsc::UnboundedSender<(String, serde_json::Value)>,
}

impl ServerHandler for TestServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            protocol_version: ProtocolVersion::LATEST,
            server_info: Implementation {
                name: "test-server".to_string(),
                version: "1.0.0".to_string(),
                title: None,
                icons: None,
                website_url: None,
            },
            capabilities: ServerCapabilities::default(),
            instructions: None,
        }
    }

    fn on_custom_notification(
        &self,
        method: String,
        params: serde_json::Value,
        _context: NotificationContext<RoleServer>,
    ) -> impl std::future::Future<Output = ()> + Send + '_ {
        tracing::info!("Server received custom notification: method={method}, params={params:#?}");
        self.notification_tx.send((method, params)).ok();
        std::future::ready(())
    }
}

pub struct TestClient {
    notification_rx: mpsc::UnboundedSender<(String, serde_json::Value)>,
}

impl ClientHandler for TestClient {
    fn get_info(&self) -> ClientInfo {
        ClientInfo {
            protocol_version: ProtocolVersion::LATEST,
            capabilities: ClientCapabilities::default(),
            client_info: Implementation {
                name: "test-client".to_string(),
                version: "1.0.0".to_string(),
                title: None,
                icons: None,
                website_url: None,
            },
        }
    }

    fn on_custom_notification(
        &self,
        method: String,
        params: serde_json::Value,
        _context: NotificationContext<RoleClient>,
    ) -> impl std::future::Future<Output = ()> + Send + '_ {
        tracing::info!("Client received custom notification: method={method}, params={params:#?}");
        self.notification_rx.send((method, params)).ok();
        std::future::ready(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| format!("info,{}=debug", env!("CARGO_CRATE_NAME")).into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let (server_tx, mut server_rx) = mpsc::unbounded_channel();
    let (client_tx, mut client_rx) = mpsc::unbounded_channel();

    let (server_transport, client_transport) = tokio::io::duplex(4096);

    let server = TestServer {
        notification_tx: server_tx,
    };

    let server_handle = tokio::spawn(async move {
        let service = server.serve(server_transport).await?;

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        tracing::info!("Server sending custom notification to client");
        service.peer().send_custom_notification(
            "notifications/session/update".to_string(),
            json!({
                "sessionId": "test-session-123",
                "messageId": "msg-456",
                "progress": "delta",
                "content": [{
                    "type": "text",
                    "text": "Hello from server!"
                }]
            })
        ).await?;

        if let Some((method, params)) = server_rx.recv().await {
            tracing::info!("Server verifying client notification: method={method}");
            assert_eq!(method, "client/status/update");
            assert_eq!(params["status"], "ready");
        }

        service.waiting().await?;
        anyhow::Ok(())
    });

    let client = TestClient {
        notification_rx: client_tx,
    };

    let client_service = client.serve(client_transport).await?;

    tracing::info!("Starting bidirectional custom notification test");

    tokio::select! {
        Some((method, params)) = client_rx.recv() => {
            tracing::info!("Client verifying server notification: method={method}");
            assert_eq!(method, "notifications/session/update");
            assert_eq!(params["sessionId"], "test-session-123");

            tracing::info!("Client sending custom notification to server");
            client_service.peer().send_custom_notification(
                "client/status/update".to_string(),
                json!({
                    "clientId": "client-789",
                    "status": "ready",
                    "capabilities": ["streaming", "batch-processing"]
                })
            ).await?;
        }
        _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {
            panic!("Timeout waiting for notification");
        }
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    client_service.cancel().await?;
    server_handle.await??;

    tracing::info!("Bidirectional custom notification test completed successfully");

    Ok(())
}