mod app;
mod constants;
mod matcher;
mod proxy;
mod recording;
mod sse;
mod types;
mod util;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    app::run().await
}

#[cfg(test)]
mod tests;
