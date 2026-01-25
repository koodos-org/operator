use k8s_openapi::{
    api::core::v1::{ImageVolumeSource, ObjectReference, PodSpec},
    apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition,
    apimachinery::pkg::apis::meta::v1::Condition,
};
use kube::CustomResourceExt;
use kube_derive::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "ondemand.dev",
    version = "v1",
    kind = "Pun",
    status = PunStatus,
    selectable = ".spec.ood_instance_ref.name",
    selectable = ".spec.ood_instance_ref.namespace",
)]
#[kube(shortname = "pun", namespaced)]
pub struct PunSpec {
    pub user: String,
    pub pun_class_ref: ObjectReference,
    pub ood_instance_ref: ObjectReference,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, JsonSchema)]
pub struct PunStatus {
    pub conditions: Vec<Condition>,
}
#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[kube(group = "ondemand.dev", version = "v1", kind = "PunClass")]
#[kube(shortname = "punclass")]
pub struct PunClassSpec {
    pub httpd: HTTPDObj,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct HTTPDObj {
    pub image: String,
    pub replicas: Option<i32>,
    pub deployment_template: Option<PodSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, JsonSchema)]
pub struct FEPStatus {
    pub conditions: Vec<Condition>,
}

#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[kube(group = "ondemand.dev", version = "v1", kind = "FrontEndProxy", status = FEPStatus)]
#[kube(shortname = "fep", namespaced)]
pub struct FrontEndProxySpec {
    pub name: String,
    pub httpd: HTTPDObj,
    pub pun_class_ref: Option<ObjectReference>,
    pub ood_instance_ref: ObjectReference,
}

#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "ondemand.dev",
    version = "v1",
    kind = "OpenOnDemand",
    status = "OpenOnDemandStatus"
)]
#[kube(shortname = "ood", namespaced)]
pub struct OpenOnDemandSpec {
    #[serde(rename = "ood_portal.yml")]
    pub ood_portal_yml: String,
    #[serde(rename = "nginx_stage.yml")]
    pub nginx_stage_yml: String,
    #[serde(rename = "clusters.d")]
    pub clusters: BTreeMap<String, String>,
    /// Container spec parameters for FEP level pods
    pub httpd: HTTPDObj,
    /// Container spec parameters for PUN level pods
    pub pun_class_ref: Option<ObjectReference>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, JsonSchema)]
pub struct OpenOnDemandStatus {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
    pub config_hash: Option<String>,
}

#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[kube(group = "ondemand.dev", version = "v1", kind = "InteractiveApp")]
#[kube(shortname = "ia", namespaced)]
pub struct InteractiveAppSpec {
    pub name: String,
    pub source: ImageVolumeSource,
}

#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[kube(group = "ondemand.dev", version = "v1", kind = "ComputeCluster")]
#[kube(shortname = "cmpcl", namespaced)]
pub struct ComputeClusterSpec {
    #[serde(rename = "cluster.yml.erb")]
    pub cluster_yml_erb: String,
    pub name: String,
}

pub fn export_crds() -> String {
    fn crd_to_string(crd: CustomResourceDefinition) -> String {
        serde_yaml::to_string(&crd).unwrap()
    }

    let pun = crd_to_string(Pun::crd());
    let pun_class = crd_to_string(PunClass::crd());
    let ood = crd_to_string(OpenOnDemand::crd());
    let ia = crd_to_string(InteractiveApp::crd());
    let cluster = crd_to_string(ComputeCluster::crd());
    let fep = crd_to_string(FrontEndProxy::crd());

    let mut crd_bundle = String::new();
    for crd in vec![pun, pun_class, ood, ia, cluster, fep] {
        crd_bundle += &format!("---\n{crd}\n");
    }
    crd_bundle
}
