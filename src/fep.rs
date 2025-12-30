// Nightly clippy (0.1.64) considers Drop a side effect, see https://github.com/rust-lang/rust-clippy/issues/9608
#![allow(clippy::unnecessary_lazy_evaluations)]

use crate::crds::{FrontEndProxy, InteractiveApp};
use anyhow::Result;
use futures::StreamExt;
use k8s_openapi::{
    api::{
        apps::v1::{Deployment, DeploymentSpec},
        core::v1::*,
        rbac::v1::{PolicyRule, Role, RoleBinding, RoleRef, Subject},
    },
    apimachinery::pkg::apis::meta::v1::LabelSelector,
};
use kube::{
    Client, CustomResourceExt,
    api::{Api, ObjectMeta, Patch, PatchParams, Resource},
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
    HTTPDPodCreationFailed(#[source] kube::Error),
    #[error("MissingObjectKey: {0}")]
    MissingObjectKey(&'static str),
}

/// Controller triggers this whenever our main object or our children changed
async fn reconcile(generator: Arc<FrontEndProxy>, ctx: Arc<Data>) -> Result<Action, Error> {
    let client = &ctx.client;

    let oref = generator.controller_owner_ref(&()).unwrap();

    let labels = generator.metadata.labels.clone()
        .ok_or_else(|| Error::MissingObjectKey(".metadata.labels"))?;

    let ood_instance_name = generator
        .spec
        .ood_instance_ref
        .name
        .as_ref()
        .ok_or_else(|| Error::MissingObjectKey(".spec.ood_instance_ref.name"))?;

    let current_namespace = generator
        .metadata
        .namespace
        .as_ref()
        .ok_or_else(|| Error::MissingObjectKey(".metadata.namespace"))?;

    let obj_meta = ObjectMeta {
        labels: Some(labels.clone()),
        name: Some("fep-cluster".to_string()),
        namespace: Some(current_namespace.to_string()),
        owner_references: Some(vec![oref.clone()]),
        ..Default::default()
    };

    let role = Role {
        metadata: obj_meta.clone(),
        rules: Some(vec![PolicyRule {
            api_groups: Some(
                vec!["ondemand.dev"]
                    .iter()
                    .map(|string| string.to_string())
                    .collect(),
            ),
            resources: Some(
                vec!["puns"]
                    .iter()
                    .map(|string| string.to_string())
                    .collect(),
            ),
            verbs: vec!["create", "patch", "update"]
                .iter()
                .map(|string| string.to_string())
                .collect(),
            ..Default::default()
        }]),
    };

    let role_binding = RoleBinding {
        metadata: obj_meta.clone(),
        role_ref: RoleRef {
            api_group: "rbac.authorization.k8s.io".to_string(),
            kind: "Role".to_string(),
            name: obj_meta
                .name
                .clone()
                .ok_or_else(|| Error::MissingObjectKey(".metadata.name"))?,
        },
        subjects: Some(vec![Subject {
            kind: "ServiceAccount".to_string(),
            name: obj_meta
                .name
                .clone()
                .ok_or_else(|| Error::MissingObjectKey(".metadata.name"))?,
            namespace: Some(current_namespace.to_string()),
            ..Default::default()
        }]),
    };

    let service_account = ServiceAccount {
        metadata: obj_meta,
        ..Default::default()
    };

    let role_api = Api::<Role>::namespaced(client.clone(), current_namespace);

    role_api
        .patch(
            role.metadata
                .name
                .as_ref()
                .ok_or_else(|| Error::MissingObjectKey(".metadata.name"))?,
            &PatchParams::apply("roles"),
            &Patch::Apply(&role),
        )
        .await
        .map_err(Error::HTTPDPodCreationFailed)?;

    let rolebinding_api = Api::<RoleBinding>::namespaced(client.clone(), current_namespace);

    rolebinding_api
        .patch(
            role_binding
                .metadata
                .name
                .as_ref()
                .ok_or_else(|| Error::MissingObjectKey(".metadata.name"))?,
            &PatchParams::apply("role_binding"),
            &Patch::Apply(&role_binding),
        )
        .await
        .map_err(Error::HTTPDPodCreationFailed)?;

    let cm_api = Api::<ConfigMap>::namespaced(client.clone(), &current_namespace);

    let mut cm_data = BTreeMap::new();
    cm_data.insert("pun.yaml".to_string(), format!("apiVersion: ondemand.dev/v1\nkind: Pun\nmetadata:\n  name: \"{ood_instance_name}-$DNS_OOD_USER\"\n  namespace: \"$NAMESPACE\"\nspec:\n  user: \"$OOD_USER\"\n  pun_class_ref:\n    name: {}\n  ood_instance_ref:\n    name: {ood_instance_name}\n    namespace: {current_namespace}",generator.spec.pun_class_ref.clone().unwrap().name.clone().unwrap()));
    let template_cm = ConfigMap {
        metadata: ObjectMeta {
            name: Some(format!(
                "pun-{}-{}-class-template",
                generator.spec.pun_class_ref.clone().unwrap().name.unwrap(),
                ood_instance_name
            )),
            owner_references: Some(vec![oref.clone()]),
            ..Default::default()
        },
        data: Some(cm_data),
        ..Default::default()
    };

    cm_api
        .patch(
            template_cm
                .metadata
                .name
                .as_ref()
                .ok_or_else(|| Error::MissingObjectKey(".metadata.name"))?,
            &PatchParams::apply("deployments"),
            &Patch::Apply(&template_cm),
        )
        .await
        .map_err(Error::HTTPDPodCreationFailed)?;

    let sa_api = Api::<ServiceAccount>::namespaced(client.clone(), current_namespace);

    sa_api
        .patch(
            service_account
                .metadata
                .name
                .as_ref()
                .ok_or_else(|| Error::MissingObjectKey(".metadata.name"))?,
            &PatchParams::apply("deployments"),
            &Patch::Apply(&service_account),
        )
        .await
        .map_err(Error::HTTPDPodCreationFailed)?;

    let mut volumes = vec![];
    let mut volume_mounts = vec![];

    let template_cm_vol = Volume {
        config_map: Some(ConfigMapVolumeSource {
            name: format!(
                "pun-{}-{}-class-template",
                generator.spec.pun_class_ref.clone().unwrap().name.unwrap(),
                ood_instance_name
            ),
            ..Default::default()
        }),
        name: "pun-template".to_string(),
        ..Default::default()
    };

    volumes.push(template_cm_vol);

    let template_cm_vol_mount = VolumeMount {
        mount_path: "/opt/krood/utils/templates".to_string(),
        name: "pun-template".to_string(),
        ..Default::default()
    };

    volume_mounts.push(template_cm_vol_mount);

    let config_volume = Volume {
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
    };
    volumes.push(config_volume);

    let config_vol_mount = VolumeMount {
        mount_path: "/etc/ood/config/ood_portal.yml".to_string(),
        name: "ood-portal".to_string(),
        sub_path: Some("ood_portal.yml".to_string()),
        ..Default::default()
    };
    volume_mounts.push(config_vol_mount);

    // Pod creation
    let pod = Pod {
        metadata: ObjectMeta {
            name: Some(generator.spec.name.clone()),
            namespace: Some(current_namespace.to_string()),
            owner_references: Some(vec![oref.clone()]),
            labels: Some(labels.clone()),
            ..ObjectMeta::default()
        },
        spec: Some(PodSpec {
            service_account_name: Some("fep-cluster".to_string()),
            containers: vec![Container {
                env: Some(vec![EnvVar {
                    name: "KROOD_OOD_NAME".to_string(),
                    value: Some(ood_instance_name.to_string()),
                    value_from: None,
                }]),
                image: Some(generator.spec.httpd.image.clone()),
                image_pull_policy: Some("Always".to_string()),
                name: generator.spec.name.clone(),
                volume_mounts: Some(volume_mounts),
                ..Default::default()
            }],
            volumes: Some(volumes),
            ..Default::default()
        }),
        ..Default::default()
    };

    let deploy = Deployment {
        metadata: ObjectMeta {
            name: Some(format!(
                "{}-fep",
                generator.metadata.name.clone().unwrap_or("ood".to_string())
            )),
            namespace: Some(current_namespace.to_string()),
            owner_references: Some(vec![oref.clone()]),
            ..Default::default()
        },
        spec: Some(DeploymentSpec {
            selector: LabelSelector {
                match_expressions: None,
                match_labels: Some(labels),
            },
            template: PodTemplateSpec {
                metadata: Some(pod.metadata),
                spec: pod.spec,
            },
            ..Default::default()
        }),
        ..Default::default()
    };

    let deploy_api = Api::<Deployment>::namespaced(client.clone(), current_namespace);

    deploy_api
        .patch(
            deploy
                .metadata
                .name
                .as_ref()
                .ok_or_else(|| Error::MissingObjectKey(".metadata.name"))?,
            &PatchParams::apply("deployments"),
            &Patch::Apply(&deploy),
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
    let client = Client::try_default().await?;

    // Api clients
    let feps = Api::<FrontEndProxy>::all(client.clone());
    let sas = Api::<ServiceAccount>::all(client.clone());
    let deployments = Api::<Deployment>::all(client.clone());
    let iapps = Api::<InteractiveApp>::all(client.clone());

    // limit the controller to running a maximum of two concurrent reconciliations
    let config = Config::default().concurrency(1);

    Controller::new(feps, watcher::Config::default())
        .owns(deployments, watcher::Config::default())
        .owns(iapps, watcher::Config::default())
        .owns(sas, watcher::Config::default())
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
    let fep_crd = serde_yaml::to_string(&FrontEndProxy::crd()).unwrap();
    let app_crd = serde_yaml::to_string(&InteractiveApp::crd()).unwrap();
    return format!("{}---\n{}", fep_crd, app_crd);
}
