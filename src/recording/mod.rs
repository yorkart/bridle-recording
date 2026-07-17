mod filesystem;

pub use filesystem::{
    append_access_log_line, headers_to_records, recording_failure, write_bytes_file,
    write_error_response_meta, write_json_file, write_manifest, write_websocket_meta,
};

use std::{future::Future, path::PathBuf};

use tokio::sync::watch;

pub const RECORDING_QUEUE_CAPACITY: usize = 32;

#[derive(Clone)]
enum RecordingSetupState {
    Pending,
    Ready(Option<PathBuf>),
}

/// A recording-only dependency that may be awaited by background writers but never by proxy I/O.
#[derive(Clone)]
pub struct RecordingContext {
    setup: watch::Receiver<RecordingSetupState>,
    #[cfg(test)]
    stream_write_delay: std::time::Duration,
}

impl RecordingContext {
    pub fn spawn<F>(setup: F) -> Self
    where
        F: Future<Output = Option<PathBuf>> + Send + 'static,
    {
        let (sender, receiver) = watch::channel(RecordingSetupState::Pending);
        tokio::spawn(async move {
            let request_dir = setup.await;
            let _ = sender.send(RecordingSetupState::Ready(request_dir));
        });
        Self {
            setup: receiver,
            #[cfg(test)]
            stream_write_delay: std::time::Duration::ZERO,
        }
    }

    pub async fn request_dir(&self) -> Option<PathBuf> {
        let mut setup = self.setup.clone();
        loop {
            let state = setup.borrow().clone();
            match state {
                RecordingSetupState::Pending => {
                    if setup.changed().await.is_err() {
                        return None;
                    }
                }
                RecordingSetupState::Ready(request_dir) => return request_dir,
            }
        }
    }

    pub async fn before_stream_write(&self) {
        #[cfg(test)]
        if !self.stream_write_delay.is_zero() {
            tokio::time::sleep(self.stream_write_delay).await;
        }
    }

    #[cfg(test)]
    pub fn with_stream_write_delay(mut self, delay: std::time::Duration) -> Self {
        self.stream_write_delay = delay;
        self
    }
}
