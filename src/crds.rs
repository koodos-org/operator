use k8s_openapi::{
    api::core::v1::{ImageVolumeSource, ObjectReference, PodTemplateSpec, ServiceSpec},
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
    group = "ondemand.krood.dev",
    version = "v1alpha1",
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
#[kube(group = "ondemand.krood.dev", version = "v1alpha1", kind = "PunClass")]
#[kube(shortname = "punclass")]
pub struct PunClassSpec {
    pub deployment_template: DeploymentConfiguration,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct DeploymentConfiguration {
    pub image: String,
    pub replicas: Option<i32>,
    pub pod_template: Option<PodTemplateSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, JsonSchema)]
pub struct FEPStatus {
    pub conditions: Vec<Condition>,
}

#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[kube(group = "ondemand.krood.dev", version = "v1alpha1", kind = "FrontEndProxy", status = FEPStatus)]
#[kube(shortname = "fep", namespaced)]
pub struct FrontEndProxySpec {
    pub deployment_template: DeploymentConfiguration,
    pub pun_class_ref: Option<ObjectReference>,
    pub ood_instance_ref: ObjectReference,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct AdvancedFeatures {
    pub pod_per_pun: Option<bool>,
    pub managed_applications: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct ServiceTemplate {
    pub annotations: Option<BTreeMap<String, String>>,
    pub labels: Option<BTreeMap<String, String>>,
    pub spec: Option<ServiceSpec>,
}
#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "ondemand.krood.dev",
    version = "v1alpha1",
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
    pub clusters: Option<BTreeMap<String, String>>,
    #[serde(rename = "ondemand.d")]
    pub ondemand_configs: Option<BTreeMap<String, String>>,
    /// Container spec parameters for FEP level pods
    pub deployment_template: DeploymentConfiguration,

    pub service: Option<ServiceTemplate>,
    /// Container spec parameters for PUN level pods
    pub pun_class_ref: Option<ObjectReference>,
    pub advanced_features: Option<AdvancedFeatures>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, JsonSchema)]
pub struct OpenOnDemandStatus {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
    pub config_hash: Option<String>,
}

#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[kube(group = "ondemand.krood.dev", version = "v1alpha1", kind = "InteractiveApp")]
#[kube(shortname = "ia", namespaced)]
pub struct InteractiveAppSpec {
    pub source: ImageVolumeSource,
}

pub fn export_crds() -> String {
    fn crd_to_string(crd: CustomResourceDefinition) -> String {
        serde_yaml::to_string(&crd).unwrap()
    }

    let pun = crd_to_string(Pun::crd());
    let pun_class = crd_to_string(PunClass::crd());
    let ood = crd_to_string(OpenOnDemand::crd());
    let ia = crd_to_string(InteractiveApp::crd());
    let fep = crd_to_string(FrontEndProxy::crd());

    let mut crd_bundle = String::new();
    for crd in vec![pun, pun_class, ood, ia, fep] {
        crd_bundle += &format!("---\n{crd}\n");
    }
    crd_bundle
}
