use thiserror::Error;
#[derive(Debug, Error)]
pub enum Error {
    #[error("Failed to create Svc: {0}")]
    SvcCreationFailed(#[source] kube::Error),
    #[error("MissingObjectKey: {0}")]
    MissingObjectKey(&'static str),
    #[error("Failed to query config maps: {0}")]
    ListConfigMapFailed(#[source] kube::Error),
    #[error("Failed to delete: {0}")]
    DeleteFailed(#[source] kube::Error),
    #[error("Failed to clean up puns: {0}")]
    FinalizerFailure(#[source] Box<kube_runtime::finalizer::Error<crate::ood::types::Error>>),
    #[error("Failed to create api: {0}")]
    ApiCreationFailure(#[source] kube::Error),
    #[error("Generic failure: {0}")]
    GenericError(&'static str),
}
