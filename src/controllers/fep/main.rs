// Nightly clippy (0.1.64) considers Drop a side effect, see https://github.com/rust-lang/rust-clippy/issues/9608
#![allow(clippy::unnecessary_lazy_evaluations)]

use crate::controllers::fep::generator;
use crate::controllers::fep::types::Error;
use crate::crds::{FrontEndProxy, InteractiveApp, OpenOnDemand};
use crate::utils::status::FEPConditions;
use anyhow::Result;
use futures::StreamExt;
use k8s_openapi::api::{
    apps::v1::Deployment,
    core::v1::*,
    rbac::v1::{Role, RoleBinding},
};
use kube::Resource;
use kube::{
    Client,
    api::{Api, Patch, PatchParams},
    runtime::{
        controller::{Action, Config, Controller},
        watcher,
    },
};
use kube_runtime::reflector::ObjectRef;
use std::sync::Arc;
use tokio::time::Duration;
use tracing::*;

async fn reconcile(generator: Arc<FrontEndProxy>, ctx: Arc<Data>) -> Result<Action, Error> {
    let client = &ctx.client;

    let oref = generator
        .controller_owner_ref(&())
        .ok_or(Error::MissingObjectKey("owner_ref"))?;
    let fep_spec = generator.spec.clone();
    let labels = generator
        .metadata
        .labels
        .clone()
        .ok_or_else(|| Error::MissingObjectKey(".metadata.labels"))?;
    let current_namespace = generator
        .metadata
        .namespace
        .as_ref()
        .ok_or_else(|| Error::MissingObjectKey(".metadata.namespace"))?;
    let fep_name = generator
        .metadata
        .name
        .as_ref()
        .ok_or_else(|| Error::MissingObjectKey(".metadata.name"))?;

    let role_api = Api::<Role>::namespaced(client.clone(), current_namespace);
    let rolebinding_api = Api::<RoleBinding>::namespaced(client.clone(), current_namespace);
    let cm_api = Api::<ConfigMap>::namespaced(client.clone(), current_namespace);
    let sa_api = Api::<ServiceAccount>::namespaced(client.clone(), current_namespace);
    let deployment_api = Api::<Deployment>::namespaced(client.clone(), current_namespace);
    let fep_api = Api::<FrontEndProxy>::namespaced(client.clone(), current_namespace);
    let ood_api = Api::<OpenOnDemand>::namespaced(client.clone(), current_namespace);

    let ood_instance = ood_api
        .get_status(
            generator
                .spec
                .ood_instance_ref
                .name
                .as_ref()
                .ok_or(Error::MissingObjectKey(".spec.ood_instance_ref.name"))?,
        )
        .await
        .map_err(Error::OODResolutionFailed)?;

    let hash = ood_instance.status.and_then(|status| status.config_hash);
    let hash = if let Some(hash) = hash {
        hash
    } else {
        return Ok(Action::requeue(Duration::from_secs(300)));
    };

    let spec_generator = generator::FEPSpecGenerator::new(
        fep_name.clone(),
        fep_spec,
        current_namespace.to_string(),
        oref,
        labels,
        hash,
    );
    // Generate state
    let service_account = spec_generator.service_account()?;
    let template_cm = spec_generator.pun_template_config_map()?;
    let role_binding = spec_generator.role_binding()?;
    let role = spec_generator.role()?;
    let deployment = spec_generator.deployment()?;

    // Read current state
    let deployment_name = deployment
        .metadata
        .name
        .as_ref()
        .ok_or_else(|| Error::MissingObjectKey(".metadata.name"))?;

    // Get current status
    let current_deployment = deployment_api
        .get_opt(&deployment_name)
        .await
        .map_err(|_| Error::MissingObjectKey("."))?;
    let current_deploy_gen = current_deployment.and_then(|dep| dep.metadata.generation);

    // Apply desired state
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

    let mut new_deployment = deployment_api
        .patch(
            deployment
                .metadata
                .name
                .as_ref()
                .ok_or_else(|| Error::MissingObjectKey(".metadata.name"))?,
            &PatchParams::apply("deployments"),
            &Patch::Apply(&deployment),
        )
        .await
        .map_err(Error::HTTPDPodCreationFailed)?;

    let patch = generator.spec.httpd.deployment_template.clone();
    // If user provied patch exists apply patch with a different manager string to force conflict
    // if the user tries to edit a field that this controller sets.
    if let Some(patch) = patch {
        let mut pun_class_patch = serde_json::json!(
        {
            "apiVersion": <Deployment as k8s_openapi::Resource>::API_VERSION,
            "kind": <Deployment as k8s_openapi::Resource>::KIND,
            "spec": {
                "template": {
                    "spec" : patch
                }
            }
        }
        );

        fn extract_container_name(deployment: &Deployment) -> Option<String> {
            Some(
                deployment
                    .spec
                    .as_ref()?
                    .template
                    .spec
                    .as_ref()?
                    .containers
                    .get(0)?
                    .name
                    .clone(),
            )
        }

        let main_container_name = extract_container_name(&deployment).ok_or(
            Error::InvalidPodTemplate("Internal failure caused by deployment spec violation"),
        )?;

        let patch: json_patch::Patch = serde_json::from_value(serde_json::json!([
    {
        "op": "replace",
        "path": "/spec/template/spec/containers/0/name",
        "value": main_container_name
    }
    ])).map_err(|_|Error::InvalidPodTemplate("Failed to serialize patch of first container in pun class template, this container will always be used to replace fields on the primary PUN container"))
    ?;

        json_patch::patch(&mut pun_class_patch, &patch).map_err(|_| {
            Error::InvalidPodTemplate("Failed to patch template with name replacement")
        })?;
        new_deployment = deployment_api
            .patch(
                deployment_name,
                &PatchParams::apply("punclass.ondemand.dev"),
                &Patch::Apply(&pun_class_patch),
            )
            .await
            .map_err(Error::HTTPDPodCreationFailed)?;
    }
    // Check status of async resources
    let mut conditions = FEPConditions::new();
    let conditions = 'cond_check: {
        // If deployment spec was updated we have to wait for an new event to trust deployment status
        if current_deploy_gen != new_deployment.metadata.generation {
            info!("Deployment updated");
            conditions.deployment(
                generator.metadata.generation,
                "Deployment has been updated and is progressing".to_string(),
                "False".to_string(),
            );
            break 'cond_check conditions;
        }

        let ready_pods = new_deployment
            .status
            .and_then(|status| status.ready_replicas);

        // If spec is current and pods are ready
        if ready_pods == generator.spec.httpd.replicas.or(Some(1)) {
            info!("Deployment is ready; PUN is ready");
            if let Some(npods) = ready_pods {
                conditions.deployment(
                    generator.metadata.generation,
                    format!("Deployment has {npods} ready pods"),
                    "True".to_string(),
                );
            }
            conditions.ready(
                generator.metadata.generation,
                "Deployment ready, PUN ready".to_string(),
                "True".to_string(),
            );
        }
        conditions
    };

    let status = conditions.get_patch();
    fep_api
        .patch_status(fep_name, &PatchParams::default(), &Patch::Merge(status))
        .await
        .map_err(Error::StatusPatchError)?;

    Ok(Action::requeue(Duration::from_secs(300)))
}

fn error_policy(_object: Arc<FrontEndProxy>, _error: &Error, _ctx: Arc<Data>) -> Action {
    Action::requeue(Duration::from_secs(1))
}

struct Data {
    client: Client,
}

pub async fn controller() -> Result<()> {
    let client = Client::try_default().await?;

    // API clients
    let feps = Api::<FrontEndProxy>::all(client.clone());
    let sas = Api::<ServiceAccount>::all(client.clone());
    let deployments = Api::<Deployment>::all(client.clone());
    let iapps = Api::<InteractiveApp>::all(client.clone());
    let oods = Api::<OpenOnDemand>::all(client.clone());
    let config = Config::default().concurrency(1);

    Controller::new(feps, watcher::Config::default())
        .owns(deployments, watcher::Config::default())
        .owns(iapps, watcher::Config::default())
        .owns(sas, watcher::Config::default())
        .watches(oods, watcher::Config::default(), |ood| {
            // Makes each OOD update force an update on any FEP resource with the same name and
            // namespace
            let ood_name = ood.metadata.name;
            let ood_ns = ood.metadata.namespace;
            ood_name
                .zip(ood_ns)
                .map(|(name, namespace)| ObjectRef::new(&name).within(&namespace))
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
    Ok(())
}
