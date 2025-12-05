// Nightly clippy (0.1.64) considers Drop a side effect, see https://github.com/rust-lang/rust-clippy/issues/9608
#![allow(clippy::unnecessary_lazy_evaluations)]

use crate::crds::FrontEndProxy;
use anyhow::Result;
use futures::StreamExt;
use k8s_openapi::api::core::v1::*;
use kube::{
    Client, CustomResourceExt,
    api::{Api, ObjectMeta, Patch, PatchParams, Resource},
    runtime::{
        controller::{Action, Config, Controller},
        watcher,
    },
};
use std::sync::Arc;
use thiserror::Error;
use tokio::time::Duration;
use tracing::*;

#[derive(Debug, Error)]
enum Error {
    #[error("Failed to create Pod: {0}")]
    HTTPDPodCreationFailed(#[source] kube::Error),
    #[error("MissingObjectKey: {0}")]
    MissingObjectKey(&'static str),
}

/// Controller triggers this whenever our main object or our children changed
async fn reconcile(generator: Arc<FrontEndProxy>, ctx: Arc<Data>) -> Result<Action, Error> {
    let client = &ctx.client;

    let oref = generator.controller_owner_ref(&()).unwrap();

    let labels = generator.metadata.labels.clone().unwrap();

    // Pod creation
    let pod = Pod {
        metadata: ObjectMeta {
            name: Some(generator.spec.name.clone()),
            namespace: generator.metadata.namespace.clone(),
            owner_references: Some(vec![oref]),
            labels: Some(labels),
            ..ObjectMeta::default()
        },
        spec: Some(PodSpec {
            service_account_name: Some("pun-creator".to_string()),
            containers: vec![Container {
                env: Some(vec![EnvVar {
                    name: "KOODO_IMAGE".to_string(),
                    value: Some(generator.spec.image.clone()),
                    value_from: None,
                }]),
                image: Some(generator.spec.image.clone()),
                image_pull_policy: Some("Always".to_string()),
                name: generator.spec.name.clone(),
                volume_mounts: Some(vec![
                    VolumeMount {
                        name: "sssd-host-pipe".to_string(),
                        mount_path: "/var/lib/sss/pipes".to_string(),
                        ..Default::default()
                    },
                    VolumeMount {
                    mount_path: "/etc/ood/config/ood_portal.yml".to_string(),
                    name: "ood-portal".to_string(),
                    sub_path: Some("ood_portal.yml".to_string()),
                    ..Default::default()
                }]),
                ..Default::default()
            }],
            volumes: Some(vec![
                Volume {
                    name: "sssd-host-pipe".to_string(),
                    host_path: Some(HostPathVolumeSource {
                        path: "/var/run/sssd-pipes".to_string(),
                        type_: Some("Directory".to_string()),
                    }),
                    ..Default::default()
                },
                Volume {
                    name: "ood-portal".to_string(),
                    config_map: Some(ConfigMapVolumeSource {
                        name: format!(
                            "{}-ood-config-files",
                            &generator
                                .metadata
                                .labels
                                .clone()
                                .unwrap()
                                .get("ood-cluster")
                                .unwrap()
                        ),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            ]),
            ..Default::default()
        }),
        ..Default::default()
    };

    let pod_api = Api::<Pod>::namespaced(
        client.clone(),
        generator
            .metadata
            .namespace
            .as_ref()
            .ok_or_else(|| Error::MissingObjectKey(".metadata.namespace"))?,
    );

    pod_api
        .patch(
            pod.metadata
                .name
                .as_ref()
                .ok_or_else(|| Error::MissingObjectKey(".metadata.name"))?,
            &PatchParams::apply("frontendproxies.ondemand.dev"),
            &Patch::Apply(&pod),
        )
        .await
        .map_err(Error::HTTPDPodCreationFailed)?;

    Ok(Action::requeue(Duration::from_secs(300)))
}

/// The controller triggers this on reconcile errors
fn error_policy(_object: Arc<FrontEndProxy>, _error: &Error, _ctx: Arc<Data>) -> Action {
    Action::requeue(Duration::from_secs(1))
}

// Data we want access to in error/reconcile calls
struct Data {
    client: Client,
}

pub async fn controller() -> Result<()> {
    //let crd = serde_yaml::to_string(&FrontEndProxy::crd()).unwrap();
    //println!("{}",crd);

    //
    let client = Client::try_default().await?;

    // Api clients
    let feps = Api::<FrontEndProxy>::all(client.clone());
    let pods = Api::<Pod>::all(client.clone());

    // limit the controller to running a maximum of two concurrent reconciliations
    let config = Config::default().concurrency(2);

    Controller::new(feps, watcher::Config::default())
        .owns(pods, watcher::Config::default())
        .with_config(config)
        .shutdown_on_signal()
        .run(reconcile, error_policy, Arc::new(Data { client }))
        .for_each(|res| async move {
            match res {
                Ok(o) => info!("reconciled {:?}", o),
                Err(e) => warn!("reconcile failed: {:?}", e),
            }
        })
        .await;
    info!("controller terminated");
    Ok(())
}

pub fn crd() -> String {
    serde_yaml::to_string(&FrontEndProxy::crd()).unwrap()
}
