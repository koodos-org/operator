use std::collections::BTreeMap;

use crate::{controllers::fep::types::Error, crds::FrontEndProxySpec};
use anyhow::Result;
use k8s_openapi::{
    api::{
        apps::v1::{Deployment, DeploymentSpec},
        core::v1::{
            ConfigMap, ConfigMapVolumeSource, Container, EnvVar, Pod, PodSpec, PodTemplateSpec,
            ServiceAccount, Volume, VolumeMount,
        },
        rbac::v1::{PolicyRule, Role, RoleBinding, RoleRef, Subject},
    },
    apimachinery::pkg::apis::meta::v1::{LabelSelector, OwnerReference},
};
use kube::api::ObjectMeta;

pub struct FEPSpecGenerator {
    spec: FrontEndProxySpec,
    current_namespace: String,
    oref: OwnerReference,
    labels: BTreeMap<String, String>,
    config_hash: String,
}

impl FEPSpecGenerator {
    pub fn new(
        spec: FrontEndProxySpec,
        current_namespace: String,
        oref: OwnerReference,
        labels: BTreeMap<String, String>,
        config_hash: String,
    ) -> Self {
        FEPSpecGenerator {
            spec,
            current_namespace,
            oref,
            labels,
            config_hash,
        }
    }

    fn get_base_obj_metadata(&self, name: String) -> Result<ObjectMeta, Error> {
        Ok(ObjectMeta {
            labels: Some(self.labels.clone()),
            name: Some(name),
            namespace: Some(self.current_namespace.to_string()),
            owner_references: Some(vec![self.oref.clone()]),
            ..Default::default()
        })
    }

    fn ood_instance_name(&self) -> Result<&String, Error> {
        self.spec
            .ood_instance_ref
            .name
            .as_ref()
            .ok_or_else(|| Error::MissingObjectKey(".spec.ood_instance_ref.name"))
    }

    fn pun_class_name(&self) -> Result<String, Error> {
        self.spec
            .pun_class_ref
            .as_ref()
            .ok_or_else(|| Error::MissingObjectKey(".spec.pun_class_ref"))?
            .name
            .clone()
            .ok_or_else(|| Error::MissingObjectKey(".spec.ood_instance_ref.name"))
    }
    pub fn pun_template_config_map(&self) -> Result<ConfigMap, Error> {
        let pun_class_name = self.pun_class_name()?;
        let ood_instance_name = self.ood_instance_name()?;
        let current_namespace = self.current_namespace.clone();
        let template_data = format!(
            r#"
apiVersion: ondemand.dev/v1
kind: Pun
metadata:
    name: \"{ood_instance_name}-$DNS_OOD_USER\"
    namespace: \"$NAMESPACE\"
spec:
    user: \"$OOD_USER\"
    pun_class_ref:
        name: {pun_class_name}
    ood_instance_ref:
        name: {ood_instance_name}
        namespace: {current_namespace}
"#,
        );

        let mut cm_data = BTreeMap::new();
        cm_data.insert("pun.yaml".to_string(), template_data);
        let template_cm = ConfigMap {
            metadata: ObjectMeta {
                name: Some(format!(
                    "pun-{}-{}-class-template",
                    pun_class_name, ood_instance_name
                )),
                owner_references: Some(vec![self.oref.clone()]),
                ..Default::default()
            },
            data: Some(cm_data),
            ..Default::default()
        };
        Ok(template_cm)
    }

    pub fn role_binding(&self) -> Result<RoleBinding, Error> {
        Ok(RoleBinding {
            metadata: self
                .get_base_obj_metadata(format!("{}-fep-rb", self.ood_instance_name()?))?,
            role_ref: RoleRef {
                api_group: "rbac.authorization.k8s.io".to_string(),
                kind: "Role".to_string(),
                name: format!("{}-fep-role", self.ood_instance_name()?),
            },
            subjects: Some(vec![Subject {
                kind: "ServiceAccount".to_string(),
                name: format!("{}-fep-svc-acct", self.ood_instance_name()?),
                namespace: Some(self.current_namespace.clone()),
                ..Default::default()
            }]),
        })
    }

    pub fn role(&self) -> Result<Role, Error> {
        Ok(Role {
            metadata: self
                .get_base_obj_metadata(format!("{}-fep-role", self.ood_instance_name()?))?,
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
        })
    }

    pub fn service_account(&self) -> Result<ServiceAccount, Error> {
        Ok(ServiceAccount {
            metadata: self
                .get_base_obj_metadata(format!("{}-fep-svc-acct", self.ood_instance_name()?))?,
            ..Default::default()
        })
    }

    fn volumes(&self) -> Result<Vec<Volume>, Error> {
        let mut volumes = vec![];
        let template_cm_vol = Volume {
            config_map: Some(ConfigMapVolumeSource {
                name: format!(
                    "pun-{}-{}-class-template",
                    self.pun_class_name()?,
                    self.ood_instance_name()?,
                ),
                ..Default::default()
            }),
            name: "pun-template".to_string(),
            ..Default::default()
        };

        volumes.push(template_cm_vol);
        let config_volume = Volume {
            name: "ood-portal".to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: format!(
                    "{}-ood-config-files-{}",
                    self.ood_instance_name()?,
                    self.config_hash
                ),
                ..Default::default()
            }),
            ..Default::default()
        };
        volumes.push(config_volume);
        let cluster_volume = Volume {
            name: "clusters-d".to_string(),
            config_map: Some(ConfigMapVolumeSource {
                name: format!(
                    "{}-ood-cluster-config-files-{}",
                    self.ood_instance_name()?,
                    self.config_hash
                ),
                ..Default::default()
            }),
            ..Default::default()
        };
        volumes.push(cluster_volume);
        Ok(volumes)
    }

    fn volume_mounts(&self) -> Result<Vec<VolumeMount>, Error> {
        let mut volume_mounts = vec![];
        let template_cm_vol_mount = VolumeMount {
            mount_path: "/opt/krood/utils/templates".to_string(),
            name: "pun-template".to_string(),
            ..Default::default()
        };

        volume_mounts.push(template_cm_vol_mount);
        let config_vol_mount = VolumeMount {
            mount_path: "/etc/ood/config/ood_portal.yml".to_string(),
            name: "ood-portal".to_string(),
            sub_path: Some("ood_portal.yml".to_string()),
            ..Default::default()
        };
        volume_mounts.push(config_vol_mount);

        let cluster_volume_mount = VolumeMount {
            mount_path: "/etc/ood/config/clusters.d".to_string(),
            name: "clusters-d".to_string(),
            ..Default::default()
        };
        volume_mounts.push(cluster_volume_mount);

        Ok(volume_mounts)
    }
    pub fn deployment(&self) -> Result<Deployment, Error> {
        let pod = Pod {
            metadata: self.get_base_obj_metadata(self.spec.name.clone())?,
            spec: Some(PodSpec {
                service_account_name: Some(format!("{}-fep-svc-acct", self.ood_instance_name()?)),
                containers: vec![Container {
                    env: Some(vec![EnvVar {
                        name: "KROOD_OOD_NAME".to_string(),
                        value: Some(self.ood_instance_name()?.to_string()),
                        value_from: None,
                    }]),
                    image: Some(self.spec.httpd.image.clone()),
                    image_pull_policy: Some("Always".to_string()),
                    name: self.spec.name.clone(),
                    volume_mounts: Some(self.volume_mounts()?),
                    ..Default::default()
                }],
                volumes: Some(self.volumes()?),
                ..Default::default()
            }),
            ..Default::default()
        };

        let deploy = Deployment {
            metadata: self.get_base_obj_metadata(format!("{}-fep", self.spec.name.clone()))?,
            spec: Some(DeploymentSpec {
                selector: LabelSelector {
                    match_expressions: None,
                    match_labels: Some(self.labels.clone()),
                },
                template: PodTemplateSpec {
                    metadata: Some(pod.metadata),
                    spec: pod.spec,
                },
                ..Default::default()
            }),
            ..Default::default()
        };
        Ok(deploy)
    }
}
