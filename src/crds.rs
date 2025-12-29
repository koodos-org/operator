use k8s_openapi::{
    api::core::v1::{ImageVolumeSource, Volume, VolumeMount},
    apimachinery::pkg::apis::meta::v1::Condition,
};
use kube_derive::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[kube(group = "ondemand.dev", version = "v1", kind = "Pun")]
#[kube(shortname = "pun", namespaced)]
pub struct PunSpec {
    pub user: String,
    pub httpd: HTTPDObj,
    pub sssd: Option<SssdObj>,
}

#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[kube(group = "ondemand.dev", version = "v1", kind = "FrontEndProxy")]
#[kube(shortname = "fep", namespaced)]
pub struct FrontEndProxySpec {
    pub name: String,
    pub httpd: HTTPDObj,
    pub sssd: Option<SssdObj>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct SssdObj {
    pub enabled: bool,
    pub image: String,
    pub config: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct HTTPDObj {
    pub image: String,
    pub extra_volume_mount: Option<Vec<VolumeMount>>,
    pub extra_volumes: Option<Vec<Volume>>,
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
    pub httpd: HTTPDObj,
    pub sssd: Option<SssdObj>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, JsonSchema)]
pub struct OpenOnDemandStatus {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
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
