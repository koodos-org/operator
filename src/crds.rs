use std::collections::BTreeMap;
use kube_derive::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use schemars::{json_schema, SchemaGenerator};

fn conditions(_: &mut SchemaGenerator) -> schemars::Schema {
    json_schema!({
        "type": "array",
        "x-kubernetes-list-type": "map",
        "x-kubernetes-list-map-keys": ["type"],
        "items": {
            "type": "object",
            "properties": {
                "lastTransitionTime": { "format": "date-time", "type": "string" },
                "message": { "type": "string" },
                "observedGeneration": { "type": "integer", "format": "int64", "default": 0 },
                "reason": { "type": "string" },
                "status": { "type": "string" },
                "type": { "type": "string" }
            },
            "required": [
                "lastTransitionTime",
                "message",
                "reason",
                "status",
                "type"
            ],
        },
    })
}

#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[kube(group = "ondemand.dev", version = "v1", kind = "Pun")]
#[kube(shortname = "pun", namespaced)]
pub struct PunSpec {
    pub user: String,
    pub image: String,
}

#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[kube(group = "ondemand.dev", version = "v1", kind = "FrontEndProxy")]
#[kube(shortname = "fep", namespaced)]
pub struct FrontEndProxySpec {
    pub name: String,
    pub image: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct SssdObj {
    pub enabled: bool,
    pub image: String,
    pub config: String,
}

#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[kube(group = "ondemand.dev", version = "v1", kind = "OpenOnDemand", status="OpenOnDemandStatus")]
#[kube(shortname = "ood", namespaced)]
pub struct OpenOnDemandSpec {
    #[serde(rename = "ood_portal.yml")]
    pub ood_portal_yml: String,
    #[serde(rename = "nginx_stage.yml")]
    pub nginx_stage_yml: String,
    #[serde(rename = "clusters.d")]
    pub clusters: BTreeMap<String, String>,
    pub image: String,
    pub sssd: Option<SssdObj>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, JsonSchema)]
pub struct OpenOnDemandStatus {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schemars(schema_with = "conditions")]
    pub conditions: Vec<Condition>,
}


#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[kube(group = "ondemand.dev", version = "v1", kind = "InteractiveApp")]
#[kube(shortname = "ia", namespaced)]
pub struct InteractiveAppSpec {
    #[serde(rename = "form.yml.erb")]
    pub form_yml_erb: String,
    pub git_url: String,
}

#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[kube(group = "ondemand.dev", version = "v1", kind = "ComputeCluster")]
#[kube(shortname = "cmpcl", namespaced)]
pub struct ComputeClusterSpec {
    #[serde(rename = "cluster.yml.erb")]
    pub cluster_yml_erb: String,
    pub name: String,
}
