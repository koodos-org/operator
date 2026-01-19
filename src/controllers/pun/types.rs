use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Failed to create Pod: {0}")]
    PunPodCreationFailed(#[source] kube::Error),
    #[error("MissingObjectKey: {0}")]
    MissingObjectKey(&'static str),
    #[error("Failed to create Service: {0}")]
    SvcCreationFailed(#[source] kube::Error),
    #[error("Failed to find PunClass: {0}")]
    PunClassNotFound(#[source] kube::Error),
}
