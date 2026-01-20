use thiserror::Error;
#[derive(Debug, Error)]
pub enum Error {
    #[error("Failed to create Svc: {0}")]
    SvcCreationFailed(#[source] kube::Error),
    #[error("MissingObjectKey: {0}")]
    MissingObjectKey(&'static str),
}



