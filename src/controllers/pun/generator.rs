use super::types::Error;
use crate::crds::{InteractiveApp, Pun, PunClass};
use anyhow::Result;
use k8s_openapi::{
    api::{
        apps::v1::{Deployment, DeploymentSpec},
        core::v1::*,
    },
    apimachinery::pkg::apis::meta::v1::LabelSelector,
};
use kube::api::{ObjectList, ObjectMeta, Resource};
use std::collections::BTreeMap;

fn get_ood_instance_name(pun: &Pun) -> Result<&String, Error> {
    return pun
        .spec
        .ood_instance_ref
        .name
        .as_ref()
        .ok_or_else(|| Error::MissingObjectKey(".spec.ood_instance_ref.name"));
}
pub fn build_deployment(
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

pub fn generate_svc(pun: &Pun, labels: BTreeMap<String, String>) -> Result<Service, Error> {
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

pub fn generate_volumes_mounts(
    pun: &Pun,
    punclass: &PunClass,
    iapps: &ObjectList<InteractiveApp>,
    config_hash: String,
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
            name: format!(
                "{}-ood-cluster-config-files-{}",
                ood_instance_name, config_hash
            ),
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
