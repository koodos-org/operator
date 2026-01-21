use std::collections::BTreeMap;

use crate::{
    controllers::ood::types::Error,
    crds::{FrontEndProxy, FrontEndProxySpec, OpenOnDemandSpec},
};
use anyhow::Result;
use k8s_openapi::{
    api::core::v1::{ConfigMap, ObjectReference, Service, ServicePort, ServiceSpec},
    apimachinery::pkg::apis::meta::v1::OwnerReference,
};
use kube::api::ObjectMeta;

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

pub struct OODSpecGenerator {
    spec: OpenOnDemandSpec,
    current_namespace: String,
    ood_instance_name: String,
    oref: OwnerReference,
    labels: BTreeMap<String, String>,
}

impl OODSpecGenerator {
    pub fn new(
        spec: OpenOnDemandSpec,
        ood_instance_name: String,
        current_namespace: String,
        oref: OwnerReference,
        labels: BTreeMap<String, String>,
    ) -> Self {
        OODSpecGenerator {
            spec,
            ood_instance_name,
            current_namespace,
            oref,
            labels,
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

    pub fn ood_cm(&self) -> Result<ConfigMap, Error> {
        // Base configs to make the custom proxy and stage logic work in the container
        let mut krood_portal_config =
            serde_yaml::from_str(include_str!("../../../assets/ood_portal.yml")).unwrap();
        let mut krood_nginx_stage_config =
            serde_yaml::from_str(include_str!("../../../assets/nginx_stage.yml")).unwrap();

        let mut config_files = BTreeMap::new();
        let site_portal_config =
            serde_yaml::from_str::<serde_yaml::Value>(&self.spec.ood_portal_yml.clone()).unwrap();
        let site_nginx_stage_config =
            serde_yaml::from_str::<serde_yaml::Value>(&self.spec.nginx_stage_yml.clone()).unwrap();

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
        Ok(ConfigMap {
            metadata: self.get_base_obj_metadata(format!(
                "{}-ood-config-files",
                self.ood_instance_name.clone()
            ))?,
            data: Some(config_files),
            ..Default::default()
        })
    }

    pub fn cluster_cm(&self) -> Result<ConfigMap, Error> {
        Ok(ConfigMap {
            metadata: self.get_base_obj_metadata(format!(
                "{}-ood-cluster-config-files",
                self.ood_instance_name.clone()
            ))?,
            data: Some(self.spec.clusters.clone()),
            ..Default::default()
        })
    }

    pub fn svc(&self) -> Result<Service, Error> {
        Ok(Service {
            metadata: self.get_base_obj_metadata(self.ood_instance_name.clone())?,
            spec: Some(ServiceSpec {
                ports: Some(vec![ServicePort {
                    port: 443,
                    ..Default::default()
                }]),
                type_: Some("LoadBalancer".to_string()),
                selector: Some(self.labels.clone()),
                ..Default::default()
            }),
            ..Default::default()
        })
    }

    pub fn fep(&self, object_ref: ObjectReference) -> Result<FrontEndProxy, Error> {
        Ok(FrontEndProxy {
            metadata: self.get_base_obj_metadata(self.ood_instance_name.clone())?,
            spec: FrontEndProxySpec {
                name: format!("{}", self.ood_instance_name),
                pun_class_ref: self.spec.pun_class_ref.clone(),
                ood_instance_ref: object_ref,
                httpd: self.spec.httpd.clone(),
            },
            status: None,
        })
    }
}
