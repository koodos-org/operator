use thiserror::Error;
#[derive(Debug, Error)]
pub enum Error {
    #[error("Failed to create Svc: {0}")]
    SvcCreationFailed(#[source] kube::Error),
    #[error("MissingObjectKey: {0}")]
    MissingObjectKey(&'static str),
    #[error("Failed to query config maps: {0}")]
    ListConfigMapFailed(#[source] kube::Error),
    #[error("Failed to clean up config maps: {0}")]
    DeleteConfigMapFailed(#[source] kube::Error),
    #[error("Failed to clean up puns")]
    FinalizerFailure,
}
