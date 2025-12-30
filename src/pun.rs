// Nightly clippy (0.1.64) considers Drop a side effect, see https://github.com/rust-lang/rust-clippy/issues/9608
#![allow(clippy::unnecessary_lazy_evaluations)]

use crate::crds::{InteractiveApp, Pun, PunClass};
use anyhow::Result;
use futures::StreamExt;
use k8s_openapi::{
    api::{
        apps::v1::{Deployment, DeploymentSpec},
        core::v1::*,
    },
    apimachinery::pkg::apis::meta::v1::LabelSelector,
};
use kube::{
    Client, CustomResourceExt,
    api::{Api, ListParams, ObjectMeta, Patch, PatchParams, Resource},
    runtime::{
        controller::{Action, Config, Controller},
        watcher,
    },
};
use std::{collections::BTreeMap, sync::Arc};
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
    #[error("Failed to find PunClass: {0}")]
    PunClassNotFound(#[source] kube::Error),
}

/// Controller triggers this whenever our main object or our children changed
async fn reconcile(generator: Arc<Pun>, ctx: Arc<Data>) -> Result<Action, Error> {
    let client = &ctx.client;

    let oref = generator.controller_owner_ref(&()).unwrap();

    let mut labels = generator
        .metadata
        .labels
        .clone()
        .unwrap_or_else(|| BTreeMap::new());

    labels.insert("ood-component".to_string(), "pun".to_string());
    labels.insert("user".to_string(), generator.spec.user.clone());

    let ood_instance_name = generator
        .spec
        .ood_instance_ref
        .name
        .as_ref()
        .ok_or_else(|| Error::MissingObjectKey(".spec.ood_instance_ref.name"))?;

    let username = generator
        .metadata
        .name
        .as_ref()
        .ok_or_else(|| Error::MissingObjectKey(".metadata.name"))?;

    let current_namespace = generator
        .metadata
        .namespace
        .as_ref()
        .ok_or_else(|| Error::MissingObjectKey(".metadata.namespace"))?;

    let svc = Service {
        metadata: ObjectMeta {
            name: Some(format!("nginx-{}", username)),
            namespace: Some(current_namespace.to_string()),
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
    let punclass_api = Api::<PunClass>::all(client.clone());

    let punclass = punclass_api
        .get(
            &generator
                .spec
                .pun_class_ref
                .name
                .clone()
                .ok_or_else(|| Error::MissingObjectKey(".spec.pun_class_ref.name"))?,
        )
        .await
        .map_err(Error::PunClassNotFound)?;

    let image = punclass.spec.httpd.image;

    let mut volumes = punclass.spec.httpd.extra_volumes.unwrap_or(vec![]);
    let mut volume_mounts = punclass.spec.httpd.extra_volume_mounts.unwrap_or(vec![]);

    let config_vol = Volume {
        name: "clusters-d".to_string(),
        config_map: Some(ConfigMapVolumeSource {
            name: format!("{}-ood-cluster-config-files", ood_instance_name),
            ..Default::default()
        }),
        ..Default::default()
    };
    volumes.push(config_vol);

    let iapps_api = Api::<InteractiveApp>::namespaced(client.clone(), current_namespace);

    let lp = ListParams::default().labels(&format!("ood-cluster={}", ood_instance_name));
    let iapps = iapps_api.list(&lp).await.unwrap();
    for iapp in iapps {
        let iapp_vol = iapp.spec.source;
        volumes.push(Volume {
            image: Some(iapp_vol),
            name: iapp.spec.name.clone(),
            ..Default::default()
        });
        volume_mounts.push(VolumeMount {
            name: iapp.spec.name.clone(),
            mount_path: format!("/var/www/ood/apps/sys/{}", iapp.spec.name),
            ..Default::default()
        })
    }

    let cluster_volume_mount = VolumeMount {
        mount_path: "/etc/ood/config/clusters.d".to_string(),
        name: "clusters-d".to_string(),
        ..Default::default()
    };

    volume_mounts.push(cluster_volume_mount);

    let pod = Pod {
        metadata: ObjectMeta {
            name: Some(format!(
                "nginx-{}",
                generator.metadata.name.clone().unwrap()
            )),
            namespace: Some(current_namespace.to_string()),
            labels: Some(labels.clone()),
            owner_references: Some(vec![oref.clone()]),
            ..ObjectMeta::default()
        },
        spec: Some(PodSpec {
            containers: vec![Container {
                image: Some(image),
                image_pull_policy: Some("Always".to_string()),
                security_context: Some(SecurityContext {
                    ..Default::default()
                }),
                name: format!("{}", username),
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

    let deployment = Deployment {
        metadata: ObjectMeta {
            name: Some(format!("{}-pun", username)),
            namespace: Some(current_namespace.to_string()),
            owner_references: Some(vec![oref.clone()]),
            ..Default::default()
        },
        spec: Some(DeploymentSpec {
            selector: LabelSelector {
                match_expressions: None,
                match_labels: Some(labels.clone()),
            },
            template: PodTemplateSpec {
                metadata: Some(pod.metadata),
                spec: pod.spec,
            },
            ..Default::default()
        }),
        ..Default::default()
    };
    let deployment_api = Api::<Deployment>::namespaced(client.clone(), current_namespace);

    let svc_api = Api::<Service>::namespaced(client.clone(), current_namespace);

    deployment_api
        .patch(
            deployment
                .metadata
                .name
                .as_ref()
                .ok_or_else(|| Error::MissingObjectKey(".metadata.name"))?,
            &PatchParams::apply("pun.ondemand.dev"),
            &Patch::Apply(&deployment),
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
    let deployments = Api::<Deployment>::all(client.clone());
    let svcs = Api::<Service>::all(client.clone());

    // limit the controller to running a maximum of two concurrent reconciliations
    let config = Config::default().concurrency(2);

    Controller::new(feps, watcher::Config::default())
        .owns(deployments, watcher::Config::default())
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
    let pun_class = serde_yaml::to_string(&PunClass::crd()).unwrap();
    let pun = serde_yaml::to_string(&Pun::crd()).unwrap();
    format!("{pun}\n---\n{pun_class}")
}
