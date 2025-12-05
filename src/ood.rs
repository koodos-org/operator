// Nightly clippy (0.1.64) considers Drop a side effect, see https://github.com/rust-lang/rust-clippy/issues/9608
#![allow(clippy::unnecessary_lazy_evaluations)]
use crate::crds::{FrontEndProxy, FrontEndProxySpec, OpenOnDemand, OpenOnDemandStatus};
use anyhow::Result;
use chrono::Utc;
use futures::StreamExt;
use k8s_openapi::api::apps::v1::{DaemonSet, DaemonSetSpec};
use k8s_openapi::api::core::v1::*;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::*;
use kube::{
    Client, CustomResourceExt,
    api::{Api, ObjectMeta, Patch, PatchParams, Resource},
    runtime::{
        controller::{Action, Config, Controller},
        watcher,
    },
};
use kube_runtime::reflector::ObjectRef;
use std::{collections::BTreeMap, sync::Arc};
use thiserror::Error;
use tokio::time::Duration;
use tracing::*;

#[derive(Debug, Error)]
enum Error {
    #[error("Failed to create Svc: {0}")]
    SvcCreationFailed(#[source] kube::Error),
    #[error("MissingObjectKey: {0}")]
    MissingObjectKey(&'static str),
}

fn merge_yaml(base: &mut serde_yaml::Value, overlay: serde_yaml::Value) {
    match (base, overlay) {
        (serde_yaml::Value::Mapping(under), serde_yaml::Value::Mapping(over)) => {
            for (k, v) in over {
                match under.get_mut(&k) {
                    Some(value) => {
                        merge_yaml(value, v);
                    }
                    None => {
                        under.insert(k, v);
                    }
                };
            }
        }
        (under, over) => *under = over,
    }
}

/// Controller triggers this whenever our main object or our children changed
async fn reconcile(generator: Arc<OpenOnDemand>, ctx: Arc<Data>) -> Result<Action, Error> {
    let client = &ctx.client;
    let oref = generator.controller_owner_ref(&()).unwrap();

    let cluster_name = generator.metadata.name.clone().unwrap();
    // Base configs to make the custom proxy and stage logic work in the container
    let mut krood_portal_config =
        serde_yaml::from_str(include_str!("../assets/ood_portal.yml")).unwrap();
    let mut krood_nginx_stage_config =
        serde_yaml::from_str(include_str!("../assets/nginx_stage.yml")).unwrap();

    let mut labels = BTreeMap::new();
    labels.insert("app".to_string(), "ood".to_string());
    labels.insert("ood-cluster".to_string(), cluster_name.clone());

    let mut config_files = BTreeMap::new();
    let site_portal_config =
        serde_yaml::from_str::<serde_yaml::Value>(&generator.spec.ood_portal_yml.clone()).unwrap();
    let site_nginx_stage_config =
        serde_yaml::from_str::<serde_yaml::Value>(&generator.spec.nginx_stage_yml.clone()).unwrap();

    // Merge site configs into base config. Note that the site config overrides the base config
    merge_yaml(&mut krood_portal_config, site_portal_config);
    merge_yaml(&mut krood_nginx_stage_config, site_nginx_stage_config);

    config_files.insert(
        "ood_portal.yml".to_string(),
        serde_yaml::to_string(&krood_portal_config).unwrap(),
    );
    config_files.insert(
        "nginx_stage.yml".to_string(),
        serde_yaml::to_string(&krood_nginx_stage_config).unwrap(),
    );

    let svc = Service {
        metadata: ObjectMeta {
            name: Some(cluster_name.clone()),
            namespace: generator.metadata.namespace.clone(),
            owner_references: Some(vec![oref.clone()]),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            ports: Some(vec![ServicePort {
                port: 443,
                ..Default::default()
            }]),
            type_: Some("LoadBalancer".to_string()),
            selector: Some(labels.clone()),
            ..Default::default()
        }),
        ..Default::default()
    };

    let ood_cm = ConfigMap {
        metadata: ObjectMeta {
            name: Some(format!("{}-ood-config-files", cluster_name.clone())),
            owner_references: Some(vec![oref.clone()]),
            ..Default::default()
        },
        data: Some(config_files),
        ..Default::default()
    };

    let mut sssd_config = BTreeMap::new();
    sssd_config.insert(
        "sssd.conf".to_string(),
        generator.spec.sssd.clone().unwrap().config,
    );

    let sssd_cm = ConfigMap {
        metadata: ObjectMeta {
            name: Some(format!("{}-sssd-config-file", cluster_name.clone())),
            owner_references: Some(vec![oref.clone()]),
            ..Default::default()
        },
        data: Some(sssd_config),
        ..Default::default()
    };

    let clusters_cm = ConfigMap {
        metadata: ObjectMeta {
            name: Some(format!("{}-ood-cluster-config-files", cluster_name.clone())),
            labels: Some(labels.clone()),
            owner_references: Some(vec![oref.clone()]),
            ..Default::default()
        },
        data: Some(generator.spec.clusters.clone()),
        ..Default::default()
    };

    let fep = FrontEndProxy {
        metadata: ObjectMeta {
            name: Some(format!("httpd")),
            labels: Some(labels.clone()),
            owner_references: Some(vec![oref.clone()]),
            ..Default::default()
        },
        spec: FrontEndProxySpec {
            name: "httpd".to_string(),
            image: generator.spec.image.clone(),
        },
    };

    let mut ds_label = BTreeMap::new();
    ds_label.insert("ood-component".to_string(), "sssd".to_string());
    let sssd_ds = DaemonSet {
        metadata: ObjectMeta {
            name: Some(format!("{}-sssd", cluster_name.clone())),
            labels: Some(ds_label.clone()),
            owner_references: Some(vec![oref.clone()]),
            ..Default::default()
        },
        spec: Some(DaemonSetSpec {
            selector: LabelSelector {
                match_labels: Some(ds_label.clone()),
                ..Default::default()
            },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(ds_label.clone()),
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    security_context: Some(PodSecurityContext {
                        run_as_user: Some(0),
                        ..Default::default()
                    }),
                    containers: vec![Container {
                        name: "sssd".to_string(),
                        image: Some(generator.spec.sssd.clone().unwrap().image),
                        volume_mounts: Some(vec![
                            VolumeMount {
                                mount_path: "/var/lib/sss/pipes".to_string(),
                                name: "sssd-pipes".to_string(),
                                ..Default::default()
                            },
                            VolumeMount {
                                mount_path: "/etc/sssd/sssd.conf".to_string(),
                                name: "sssd-config".to_string(),
                                sub_path: Some("sssd.conf".to_string()),
                                ..Default::default()
                            },
                        ]),
                        ..Default::default()
                    }],
                    volumes: Some(vec![
                        Volume {
                            name: "sssd-config".to_string(),
                            config_map: Some(ConfigMapVolumeSource {
                                name: format!("{}-sssd-config-file", cluster_name.clone()),
                                items: Some(vec![KeyToPath {
                                    key: "sssd.conf".to_string(),
                                    path: "sssd.conf".to_string(),
                                    mode: Some(0o0600),
                                }]),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                        Volume {
                            name: "sssd-pipes".to_string(),
                            host_path: Some(HostPathVolumeSource {
                                path: "/var/run/sssd-pipes".to_string(),
                                type_: Some("DirectoryOrCreate".to_string()),
                            }),
                            ..Default::default()
                        },
                    ]),
                    ..Default::default()
                }),
                ..Default::default()
            },
            ..Default::default()
        }),
        ..Default::default()
    };

    // Getting Kubernetes API clients for needed resources
    let svc_api = Api::<Service>::namespaced(
        client.clone(),
        generator
            .metadata
            .namespace
            .as_ref()
            .ok_or_else(|| Error::MissingObjectKey(".metadata.namespace"))?,
    );
    let cm_api = Api::<ConfigMap>::namespaced(
        client.clone(),
        generator
            .metadata
            .namespace
            .as_ref()
            .ok_or_else(|| Error::MissingObjectKey(".metadata.namespace"))?,
    );
    let fep_api = Api::<FrontEndProxy>::namespaced(
        client.clone(),
        generator
            .metadata
            .namespace
            .as_ref()
            .ok_or_else(|| Error::MissingObjectKey(".metadata.namespace"))?,
    );

    let ds_api = Api::<DaemonSet>::namespaced(
        client.clone(),
        generator
            .metadata
            .namespace
            .as_ref()
            .ok_or_else(|| Error::MissingObjectKey(".metadata.namespace"))?,
    );

    let ood_api = Api::<OpenOnDemand>::namespaced(
        client.clone(),
        generator
            .metadata
            .namespace
            .as_ref()
            .ok_or_else(|| Error::MissingObjectKey(".metadata.namespace"))?,
    );

    cm_api
        .patch(
            ood_cm.metadata.name.as_ref().unwrap(),
            &PatchParams::apply("openondemands.ondemand.dev"),
            &Patch::Apply(&ood_cm),
        )
        .await
        .unwrap();
    cm_api
        .patch(
            clusters_cm.metadata.name.as_ref().unwrap(),
            &PatchParams::apply("openondemands.ondemand.dev"),
            &Patch::Apply(&clusters_cm),
        )
        .await
        .unwrap();
    cm_api
        .patch(
            sssd_cm.metadata.name.as_ref().unwrap(),
            &PatchParams::apply("openondemands.ondemand.dev"),
            &Patch::Apply(&sssd_cm),
        )
        .await
        .unwrap();
    fep_api
        .patch(
            fep.metadata.name.as_ref().unwrap(),
            &PatchParams::apply("frontendproxies.ondemand.dev"),
            &Patch::Apply(&fep),
        )
        .await
        .unwrap();

    svc_api
        .patch(
            svc.metadata
                .name
                .as_ref()
                .ok_or_else(|| Error::MissingObjectKey(".metadata.namespace"))?,
            &PatchParams::apply("frontendproxies.ondemand.dev"),
            &Patch::Apply(&svc),
        )
        .await
        .map_err(Error::SvcCreationFailed)?;

    ds_api
        .patch(
            sssd_ds.metadata.name.as_ref().unwrap(),
            &PatchParams::apply("frontendproxies.ondemand.dev"),
            &Patch::Apply(&sssd_ds),
        )
        .await
        .map_err(Error::SvcCreationFailed)?;

    let pp = PatchParams::default();

    let data = serde_json::json!({
        "status": OpenOnDemandStatus {
            conditions: vec![]
        }
    });

    ood_api
        .patch_status(
            &generator.metadata.name.clone().unwrap(),
            &pp,
            &Patch::Merge(data),
        )
        .await
        .unwrap();

    Ok(Action::requeue(Duration::from_secs(300)))
}

/// The controller triggers this on reconcile errors
fn error_policy(_object: Arc<OpenOnDemand>, _error: &Error, _ctx: Arc<Data>) -> Action {
    Action::requeue(Duration::from_secs(1))
}

// Data we want access to in error/reconcile calls
struct Data {
    client: Client,
}

pub async fn controller() -> Result<()> {
    let client = Client::try_default().await?;

    // Api clients
    let oods = Api::<OpenOnDemand>::all(client.clone());
    let cms = Api::<ConfigMap>::all(client.clone());
    let svcs = Api::<Service>::all(client.clone());
    let dss = Api::<DaemonSet>::all(client.clone());

    // limit the controller to running a maximum of two concurrent reconciliations
    let config = Config::default().concurrency(2);

    Controller::new(oods, watcher::Config::default())
        .watches(cms, watcher::Config::default(), |ar| {
            if let Some(labels) = ar.metadata.labels {
                if let Some(app_label) = labels.get("ood-cluster") {
                    Some(ObjectRef::new(app_label).within(&ar.metadata.namespace.unwrap()))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .watches(svcs, watcher::Config::default(), |ar| {
            if let Some(labels) = ar.metadata.labels {
                if let Some(app_label) = labels.get("ood-cluster") {
                    Some(ObjectRef::new(app_label).within(&ar.metadata.namespace.unwrap()))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .watches(dss, watcher::Config::default(), |ar| {
            if let Some(labels) = ar.metadata.labels {
                if let Some(app_label) = labels.get("ood-cluster") {
                    Some(ObjectRef::new(app_label).within(&ar.metadata.namespace.unwrap()))
                } else {
                    None
                }
            } else {
                None
            }
        })
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
    serde_yaml::to_string(&OpenOnDemand::crd()).unwrap()
}
