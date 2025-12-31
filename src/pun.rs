// Nightly clippy (0.1.64) considers Drop a side effect, see https://github.com/rust-lang/rust-clippy/issues/9608
#![allow(clippy::unnecessary_lazy_evaluations)]

use crate::crds::{InteractiveApp, Pun, PunClass, PunStatus};
use anyhow::Result;
use chrono::Utc;
use futures::StreamExt;
use k8s_openapi::{
    api::{
        apps::v1::{Deployment, DeploymentSpec},
        core::v1::*,
    },
    apimachinery::pkg::apis::meta::v1::{Condition, LabelSelector, Time},
};
use kube::{
    Client, CustomResourceExt,
    api::{Api, ListParams, ObjectList, ObjectMeta, Patch, PatchParams, Resource},
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

fn get_ood_instance_name(pun: &Pun) -> Result<&String, Error> {
    return pun
        .spec
        .ood_instance_ref
        .name
        .as_ref()
        .ok_or_else(|| Error::MissingObjectKey(".spec.ood_instance_ref.name"));
}
fn build_deployment(
    pun: &Pun,
    punclass: &PunClass,
    labels: BTreeMap<String, String>,
    volumes: Vec<Volume>,
    volume_mounts: Vec<VolumeMount>,
) -> Result<Deployment, Error> {
    let ood_instance_name = get_ood_instance_name(&pun)?;
    let oref = pun.controller_owner_ref(&()).unwrap();
    let current_namespace = pun
        .metadata
        .namespace
        .as_ref()
        .ok_or_else(|| Error::MissingObjectKey(".metadata.namespace"))?;
    let image = punclass.spec.httpd.image.clone();

    let dns_username = pun
        .metadata
        .name
        .as_ref()
        .ok_or_else(|| Error::MissingObjectKey(".metadata.name"))?;

    let pod = Pod {
        metadata: ObjectMeta {
            name: Some(format!(
                "{}-nginx-{}",
                ood_instance_name,
                pun.metadata.name.clone().unwrap()
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
                name: format!("{dns_username}"),
                volume_mounts: Some(volume_mounts),
                command: Some(vec![
                    "/opt/krood/pun_entry.sh".to_string(),
                    pun.spec.user.to_string(),
                ]),

                ..Default::default()
            }],
            volumes: Some(volumes),
            ..Default::default()
        }),
        ..Default::default()
    };

    Ok(Deployment {
        metadata: ObjectMeta {
            name: Some(format!("{dns_username}-pun")),
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
    })
}

/// Controller triggers this whenever our main object or our children changed
async fn reconcile(generator: Arc<Pun>, ctx: Arc<Data>) -> Result<Action, Error> {
    // Initial setup
    let client = &ctx.client;
    let current_namespace = generator
        .metadata
        .namespace
        .as_ref()
        .ok_or_else(|| Error::MissingObjectKey(".metadata.namespace"))?;

    let deployment_api = Api::<Deployment>::namespaced(client.clone(), current_namespace);

    let punclass_api = Api::<PunClass>::all(client.clone());
    let svc_api = Api::<Service>::namespaced(client.clone(), current_namespace);
    let ood_instance_name = generator
        .spec
        .ood_instance_ref
        .name
        .as_ref()
        .ok_or_else(|| Error::MissingObjectKey(".spec.ood_instance_ref.name"))?;

    let mut labels = generator
        .metadata
        .labels
        .clone()
        .unwrap_or_else(|| BTreeMap::new());

    labels.insert("ood-component".to_string(), "pun".to_string());
    labels.insert("user".to_string(), generator.spec.user.clone());
    labels.insert("ood-instance".to_string(), ood_instance_name.clone());

    // Read other resources for additional spec info

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

    let iapps_api = Api::<InteractiveApp>::namespaced(client.clone(), current_namespace);

    let lp = ListParams::default().labels(&format!("ood-cluster={}", ood_instance_name));
    let iapps = iapps_api.list(&lp).await.unwrap();

    // TODO: PUN observed gen handling

    // Generate desired world

    let svc = generate_svc(&generator, labels.clone())?;

    let (volumes, volume_mounts) = generate_volumes_mounts(&generator, &punclass, &iapps)?;

    let deployment = build_deployment(&generator, &punclass, labels, volumes, volume_mounts)?;
    
    // Get current status
    let current_deployment = deployment_api
        .get_opt(
            &deployment
                .metadata
                .name
                .as_ref()
                .ok_or_else(|| Error::MissingObjectKey(".metadata.name"))?,
        )
        .await
        // Should be an API error, not a not found
        .map_err(Error::PunClassNotFound)?;

    let current_dep_gen = current_deployment.and_then(|dep| dep.metadata.generation);

    // Update World to match desired state

    let new_deployment = deployment_api
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

    let pun_api = Api::<Pun>::namespaced(client.clone(), current_namespace);

    let status_patch = |conditions| {
        serde_json::json!({
                "status": {
                    "conditions": vec![conditions]
        }
        })
    };
    // 6 Check for generation change

    // Set as progressing: (spec change) if old_gen != new_gen
    if current_dep_gen != new_deployment.metadata.generation {
        warn!("Setting status: gen mismatch");
        let progressing_cond = Condition {
            status: "False".to_string(),
            type_: "DeploymentProgressing".to_string(),
            last_transition_time: Time(Utc::now()),
            message: format!("Deployment has been updated, waiting"),
            observed_generation: generator.metadata.generation,
            reason: format!("DeploymentUpdated"),
        };
        pun_api
            .patch_status(
                generator
                    .metadata
                    .name
                    .as_ref()
                    .ok_or_else(|| Error::MissingObjectKey(".metadata.name"))?,
                &PatchParams::default(),
                &Patch::Merge(status_patch(progressing_cond)),
            )
            .await
            .map_err(Error::SvcCreationFailed)?;

        return Ok(Action::requeue(Duration::from_secs(300)));
    }

    // Check for number of ready pods if this matches desired state, set condition to available
    let ready_pods = new_deployment
        .status
        .and_then(|status| status.ready_replicas);

    if ready_pods == Some(1) {
        warn!("Setting status: Ready");
        let available_cond = Condition {
            status: "True".to_string(),
            type_: "Available".to_string(),
            last_transition_time: Time(Utc::now()),
            message: format!("PUN is available"),
            observed_generation: generator.metadata.generation,
            reason: format!("DeploymentReady"),
        };
        pun_api
            .patch_status(
                generator
                    .metadata
                    .name
                    .as_ref()
                    .ok_or_else(|| Error::MissingObjectKey(".metadata.name"))?,
                &PatchParams::default(),
                &Patch::Merge(status_patch(available_cond)),
            )
            .await
            .map_err(Error::SvcCreationFailed)?;

        return Ok(Action::requeue(Duration::from_secs(300)));
    }
    Ok(Action::requeue(Duration::from_secs(300)))
}

fn generate_svc(pun: &Pun, labels: BTreeMap<String, String>) -> Result<Service, Error> {
    let oref = pun.controller_owner_ref(&()).unwrap();
    let current_namespace = pun
        .metadata
        .namespace
        .as_ref()
        .ok_or_else(|| Error::MissingObjectKey(".metadata.namespace"))?;

    let dns_username = pun
        .metadata
        .name
        .as_ref()
        .ok_or_else(|| Error::MissingObjectKey(".metadata.name"))?;

    Ok(Service {
        metadata: ObjectMeta {
            name: Some(format!("nginx-{dns_username}")),
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
    })
}

fn generate_volumes_mounts(
    pun: &Pun,
    punclass: &PunClass,
    iapps: &ObjectList<InteractiveApp>,
) -> Result<(Vec<Volume>, Vec<VolumeMount>), Error> {
    let ood_instance_name = get_ood_instance_name(pun)?;
    let mut volumes = punclass.spec.httpd.extra_volumes.clone().unwrap_or(vec![]);
    let mut volume_mounts = punclass
        .spec
        .httpd
        .extra_volume_mounts
        .clone()
        .unwrap_or(vec![]);

    let config_vol = Volume {
        name: "clusters-d".to_string(),
        config_map: Some(ConfigMapVolumeSource {
            name: format!("{}-ood-cluster-config-files", ood_instance_name),
            ..Default::default()
        }),
        ..Default::default()
    };
    volumes.push(config_vol);

    for iapp in iapps {
        let iapp_vol = iapp.spec.source.clone();
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
    Ok((volumes, volume_mounts))
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
