// Nightly clippy (0.1.64) considers Drop a side effect, see https://github.com/rust-lang/rust-clippy/issues/9608
#![allow(clippy::unnecessary_lazy_evaluations)]

use crate::crds::Pun;
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
use std::collections::BTreeMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::time::Duration;
use tracing::*;

#[derive(Debug, Error)]
enum Error {
    #[error("Failed to create Pod: {0}")]
    PunPodCreationFailed(#[source] kube::Error),
    #[error("MissingObjectKey: {0}")]
    MissingObjectKey(&'static str),
    #[error("Failed to create Service: {0}")]
    SvcCreationFailed(#[source] kube::Error),
}

/// Controller triggers this whenever our main object or our children changed
async fn reconcile(generator: Arc<Pun>, ctx: Arc<Data>) -> Result<Action, Error> {
    let client = &ctx.client;

    let oref = generator.controller_owner_ref(&()).unwrap();

    let mut labels = BTreeMap::new();

    labels.insert("app".to_string(), "pun".to_string());
    labels.insert("user".to_string(), generator.spec.user.clone());

    let svc = Service {
        metadata: ObjectMeta {
            name: Some(format!(
                "nginx-{}",
                generator.metadata.name.clone().unwrap()
            )),
            namespace: generator.metadata.namespace.clone(),
            owner_references: Some(vec![oref.clone()]),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            cluster_ip: Some("None".to_string()),
            ports: Some(vec![ServicePort {
                port: 443,
                ..Default::default()
            }]),
            selector: Some(labels.clone()),
            ..Default::default()
        }),
        ..Default::default()
    };

    let mut volumes = vec![];

    // let nfs_vol = Volume {
    //     name: "home-nfs".to_string(),
    //     persistent_volume_claim: Some(PersistentVolumeClaimVolumeSource {
    //         claim_name: "pvc-nfs-static".to_string(),
    //         ..Default::default()
    //     }),
    //     ..Default::default()
    // };
    // volumes.push(nfs_vol);
    // let sssd_vol = Volume {
    //     name: "sssd-host-pipe".to_string(),
    //     host_path: Some(HostPathVolumeSource {
    //         path: "/var/run/sssd-pipes".to_string(),
    //         type_: Some("Directory".to_string()),
    //     }),
    //
    //     ..Default::default()
    // };
    let config_vol = Volume {
        name: "clusters-d".to_string(),
        config_map: Some(ConfigMapVolumeSource {
            name: format!(
                "stormhead-work-ood-cluster-config-files",
                //&generator
                //    .metadata
                //    .labels.clone()
                //    .unwrap()
                //    .get("ood-cluster")
                //    .unwrap()
            ),
            ..Default::default()
        }),
        ..Default::default()
    };
    volumes.push(config_vol);

    let mut volume_mounts = vec![];
    // if generator
    //     .spec
    //     .sssd
    //     .clone()
    //     .map(|obj| obj.enabled)
    //     .unwrap_or(false)
    // {
    //     volumes.push(sssd_vol);
    //     let sssd_vol_mount = VolumeMount {
    //         name: "sssd-host-pipe".to_string(),
    //         mount_path: "/var/lib/sss/pipes".to_string(),
    //         ..Default::default()
    //     };
    //     volume_mounts.push(sssd_vol_mount);
    // }
    //
    let cluster_volume_mount = VolumeMount {
        mount_path: "/etc/ood/config/clusters.d".to_string(),
        name: "clusters-d".to_string(),
        ..Default::default()
    };

    volume_mounts.push(cluster_volume_mount);
    // let nfs_vol_mount = VolumeMount {
    //     mount_path: "/home".to_string(),
    //     name: "home-nfs".to_string(),
    //     ..Default::default()
    // };
    // volume_mounts.push(nfs_vol_mount);
    // Pod creation
    let pod = Pod {
        metadata: ObjectMeta {
            name: Some(format!(
                "nginx-{}",
                generator.metadata.name.clone().unwrap()
            )),
            namespace: generator.metadata.namespace.clone(),
            labels: Some(labels),
            owner_references: Some(vec![oref]),
            ..ObjectMeta::default()
        },
        spec: Some(PodSpec {
            containers: vec![Container {
                image: Some(generator.spec.httpd.image.clone()),
                image_pull_policy: Some("Always".to_string()),
                security_context: Some(SecurityContext {
                    ..Default::default()
                }),
                name: generator.metadata.name.clone().unwrap(),
                volume_mounts: Some(volume_mounts),
                command: Some(vec![
                    "/opt/krood/pun_entry.sh".to_string(),
                    generator.spec.user.to_string(),
                ]),

                ..Default::default()
            }],
            volumes: Some(volumes),
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

    let svc_api = Api::<Service>::namespaced(
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
            &PatchParams::apply("pun.ondemand.dev"),
            &Patch::Apply(&pod),
        )
        .await
        .map_err(Error::PunPodCreationFailed)?;

    svc_api
        .patch(
            svc.metadata
                .name
                .as_ref()
                .ok_or_else(|| Error::MissingObjectKey(".metadata.namespace"))?,
            &PatchParams::apply("pun.ondemand.dev"),
            &Patch::Apply(&svc),
        )
        .await
        .map_err(Error::SvcCreationFailed)?;

    Ok(Action::requeue(Duration::from_secs(300)))
}

/// The controller triggers this on reconcile errors
fn error_policy(_object: Arc<Pun>, _error: &Error, _ctx: Arc<Data>) -> Action {
    Action::requeue(Duration::from_secs(1))
}

// Data we want access to in error/reconcile calls
struct Data {
    client: Client,
}

pub async fn controller() -> Result<()> {
    let client = Client::try_default().await?;

    // Api clients
    let feps = Api::<Pun>::all(client.clone());
    let pods = Api::<Pod>::all(client.clone());
    let svcs = Api::<Service>::all(client.clone());

    // limit the controller to running a maximum of two concurrent reconciliations
    let config = Config::default().concurrency(2);

    Controller::new(feps, watcher::Config::default())
        .owns(pods, watcher::Config::default())
        .owns(svcs, watcher::Config::default())
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
    error!("controller terminated");
    Ok(())
}

pub fn crd() -> String {
    serde_yaml::to_string(&Pun::crd()).unwrap()
}
