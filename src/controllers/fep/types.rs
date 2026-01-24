use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Failed to create Pod: {0}")]
    HTTPDPodCreationFailed(#[source] kube::Error),
    #[error("MissingObjectKey: {0}")]
    MissingObjectKey(&'static str),
    #[error("Failed to patch status on fep: {0}")]
    StatusPatchError(#[source] kube::Error),
    #[error("Failed to resolve OOD on fep: {0}")]
    OODResolutionFailed(#[source] kube::Error)

}

