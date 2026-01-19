// Nightly clippy (0.1.64) considers Drop a side effect, see https://github.com/rust-lang/rust-clippy/issues/9608
#![allow(clippy::unnecessary_lazy_evaluations)]

use super::types::Error;
use crate::crds::{InteractiveApp, Pun, PunClass};
use crate::pun::generator;
use anyhow::Result;
use chrono::Utc;
use futures::StreamExt;
use k8s_openapi::{
    api::{apps::v1::Deployment, core::v1::*},
    apimachinery::pkg::apis::meta::v1::{Condition, Time},
};
use kube::{
    Client,
    api::{Api, ListParams, Patch, PatchParams},
    runtime::{
        controller::{Action, Config, Controller},
        watcher,
    },
};
use std::{collections::BTreeMap, sync::Arc};
use tokio::time::Duration;
use tracing::*;

/// Controller triggers this whenever our main object or our children changed
async fn reconcile(generator: Arc<Pun>, ctx: Arc<Data>) -> Result<Action, Error> {
    // Initial setup
    let client = &ctx.client;
    let current_namespace = generator
        .metadata
        .namespace
        .as_ref()
        .ok_or_else(|| Error::MissingObjectKey(".metadata.namespace"))?;

    let deployment_api = Api::<Deployment>::namespaced(client.clone(), current_namespace);

    let punclass_api = Api::<PunClass>::all(client.clone());
    let svc_api = Api::<Service>::namespaced(client.clone(), current_namespace);
    let ood_instance_name = generator
        .spec
        .ood_instance_ref
        .name
        .as_ref()
        .ok_or_else(|| Error::MissingObjectKey(".spec.ood_instance_ref.name"))?;

    let mut labels = generator
        .metadata
        .labels
        .clone()
        .unwrap_or_else(|| BTreeMap::new());

    labels.insert("ood-component".to_string(), "pun".to_string());
    labels.insert("user".to_string(), generator.spec.user.clone());
    labels.insert("ood-instance".to_string(), ood_instance_name.clone());

    // Read other resources for additional spec info

    let punclass = punclass_api
        .get(
            &generator
                .spec
                .pun_class_ref
                .name
                .clone()
                .ok_or_else(|| Error::MissingObjectKey(".spec.pun_class_ref.name"))?,
        )
        .await
        .map_err(Error::PunClassNotFound)?;

    let iapps_api = Api::<InteractiveApp>::namespaced(client.clone(), current_namespace);

    let lp = ListParams::default().labels(&format!("ood-cluster={}", ood_instance_name));
    let iapps = iapps_api.list(&lp).await.unwrap();

    // TODO: PUN observed gen handling

    // Generate desired world

    let svc = generator::generate_svc(&generator, labels.clone())?;

    let (volumes, volume_mounts) =
        generator::generate_volumes_mounts(&generator, &punclass, &iapps)?;

    let deployment =
        generator::build_deployment(&generator, &punclass, labels, volumes, volume_mounts)?;

    // Get current status
    let current_deployment = deployment_api
        .get_opt(
            &deployment
                .metadata
                .name
                .as_ref()
                .ok_or_else(|| Error::MissingObjectKey(".metadata.name"))?,
        )
        .await
        // Should be an API error, not a not found
        .map_err(Error::PunClassNotFound)?;

    let current_dep_gen = current_deployment.and_then(|dep| dep.metadata.generation);

    // Update World to match desired state

    let new_deployment = deployment_api
        .patch(
            deployment
                .metadata
                .name
                .as_ref()
                .ok_or_else(|| Error::MissingObjectKey(".metadata.name"))?,
            &PatchParams::apply("pun.ondemand.dev"),
            &Patch::Apply(&deployment),
        )
        .await
        .map_err(Error::PunPodCreationFailed)?;
    svc_api
        .patch(
            svc.metadata
                .name
                .as_ref()
                .ok_or_else(|| Error::MissingObjectKey(".metadata.namespace"))?,
            &PatchParams::apply("pun.ondemand.dev"),
            &Patch::Apply(&svc),
        )
        .await
        .map_err(Error::SvcCreationFailed)?;

    let pun_api = Api::<Pun>::namespaced(client.clone(), current_namespace);

    let status_patch = |conditions| {
        serde_json::json!({
                "status": {
                    "conditions": vec![conditions]
        }
        })
    };
    // 6 Check for generation change

    // Set as progressing: (spec change) if old_gen != new_gen
    if current_dep_gen != new_deployment.metadata.generation {
        warn!("Setting status: gen mismatch");
        let progressing_cond = Condition {
            status: "False".to_string(),
            type_: "DeploymentProgressing".to_string(),
            last_transition_time: Time(Utc::now()),
            message: format!("Deployment has been updated, waiting"),
            observed_generation: generator.metadata.generation,
            reason: format!("DeploymentUpdated"),
        };
        pun_api
            .patch_status(
                generator
                    .metadata
                    .name
                    .as_ref()
                    .ok_or_else(|| Error::MissingObjectKey(".metadata.name"))?,
                &PatchParams::default(),
                &Patch::Merge(status_patch(progressing_cond)),
            )
            .await
            .map_err(Error::SvcCreationFailed)?;

        return Ok(Action::requeue(Duration::from_secs(300)));
    }

    // Check for number of ready pods if this matches desired state, set condition to available
    let ready_pods = new_deployment
        .status
        .and_then(|status| status.ready_replicas);

    if ready_pods == Some(1) {
        warn!("Setting status: Ready");
        let available_cond = Condition {
            status: "True".to_string(),
            type_: "Available".to_string(),
            last_transition_time: Time(Utc::now()),
            message: format!("PUN is available"),
            observed_generation: generator.metadata.generation,
            reason: format!("DeploymentReady"),
        };
        pun_api
            .patch_status(
                generator
                    .metadata
                    .name
                    .as_ref()
                    .ok_or_else(|| Error::MissingObjectKey(".metadata.name"))?,
                &PatchParams::default(),
                &Patch::Merge(status_patch(available_cond)),
            )
            .await
            .map_err(Error::SvcCreationFailed)?;

        return Ok(Action::requeue(Duration::from_secs(300)));
    }
    Ok(Action::requeue(Duration::from_secs(300)))
}

/// The controller triggers this on reconcile errors
fn error_policy(_object: Arc<Pun>, _error: &Error, _ctx: Arc<Data>) -> Action {
    Action::requeue(Duration::from_secs(1))
}

// Data we want access to in error/reconcile calls
struct Data {
    client: Client,
}

pub async fn controller() -> Result<()> {
    let client = Client::try_default().await?;

    let feps = Api::<Pun>::all(client.clone());
    let deployments = Api::<Deployment>::all(client.clone());
    let svcs = Api::<Service>::all(client.clone());

    let config = Config::default().concurrency(2);

    Controller::new(feps, watcher::Config::default())
        .owns(deployments, watcher::Config::default())
        .owns(svcs, watcher::Config::default())
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
    error!("controller terminated");
    Ok(())
}
