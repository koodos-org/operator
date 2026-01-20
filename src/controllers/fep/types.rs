use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Failed to create Pod: {0}")]
    HTTPDPodCreationFailed(#[source] kube::Error),
    #[error("MissingObjectKey: {0}")]
    MissingObjectKey(&'static str),
}

