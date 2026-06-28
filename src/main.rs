mod app;
mod constants;
mod http_proxy;
mod matcher;
mod replay;
mod sse;
mod types;
mod util;
mod websocket_proxy;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    app::run().await
}

#[cfg(test)]
mod tests;
