// Nightly clippy (0.1.64) considers Drop a side effect, see https://github.com/rust-lang/rust-clippy/issues/9608
#![allow(clippy::unnecessary_lazy_evaluations)]

use super::types::Error;
use crate::crds::{InteractiveApp, Pun, PunClass};
use crate::pun::generator;
use crate::utils::status::PUNConditions;
use anyhow::Result;
use futures::StreamExt;
use k8s_openapi::api::{apps::v1::Deployment, core::v1::*};
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
#[instrument(skip_all, fields(resource_name = generator.metadata.name, resource_type = "PUN"))]
async fn reconcile(generator: Arc<Pun>, ctx: Arc<Data>) -> Result<Action, Error> {
    // Initial setup
    let client = &ctx.client;
    let current_namespace = generator
        .metadata
        .namespace
        .as_ref()
        .ok_or_else(|| Error::MissingObjectKey(".metadata.namespace"))?;

    let mut conditions = PUNConditions::new();

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
    let pun_name = generator
        .metadata
        .name
        .as_ref()
        .ok_or_else(|| Error::MissingObjectKey(".metadata.name"))?;

    let svc = generator::generate_svc(&generator, labels.clone())?;

    let (volumes, volume_mounts) =
        generator::generate_volumes_mounts(&generator, &punclass, &iapps)?;

    let deployment =
        generator::build_deployment(&generator, &punclass, labels, volumes, volume_mounts)?;

    let deployment_name = deployment
        .metadata
        .name
        .as_ref()
        .ok_or_else(|| Error::MissingObjectKey(".metadata.name"))?;
    // Get current status
    let current_deployment = deployment_api
        .get_opt(&deployment_name)
        .await
        // Should be an API error, not a not found
        .map_err(Error::PunClassNotFound)?;

    let current_deploy_gen = current_deployment.and_then(|dep| dep.metadata.generation);

    // Update World to match desired state
    let new_deployment = deployment_api
        .patch(
            deployment_name,
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

    // 6 Check status points for conditions
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
        if ready_pods == Some(1) {
            info!("Deployment is ready; PUN is ready");
            conditions.deployment(
                generator.metadata.generation,
                "Deployment has ready pod".to_string(),
                "True".to_string(),
            );
            conditions.ready(
                generator.metadata.generation,
                "Deployment ready, PUN ready".to_string(),
                "True".to_string(),
            );
        }
        conditions
    };

    let status = conditions.get_patch();
    pun_api
        .patch_status(pun_name, &PatchParams::default(), &Patch::Merge(status))
        .await
        .map_err(Error::SvcCreationFailed)?;
    Ok(Action::requeue(Duration::from_secs(300)))
}

fn error_policy(_object: Arc<Pun>, _error: &Error, _ctx: Arc<Data>) -> Action {
    Action::requeue(Duration::from_secs(1))
}

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
