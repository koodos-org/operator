// Nightly clippy (0.1.64) considers Drop a side effect, see https://github.com/rust-lang/rust-clippy/issues/9608
#![allow(clippy::unnecessary_lazy_evaluations)]
use crate::controllers::ood::generator::OODSpecGenerator;
use crate::controllers::ood::types::Error;
use crate::crds::{FrontEndProxy, OpenOnDemand};
use crate::utils::status::OODConditions;
use anyhow::Result;
use futures::StreamExt;
use k8s_openapi::api::core::v1::*;
use kube::{
    Client,
    api::{Api, Patch, PatchParams, Resource},
    runtime::{
        controller::{Action, Config, Controller},
        watcher,
    },
};
use kube_runtime::reflector::ObjectRef;
use std::{collections::BTreeMap, sync::Arc};
use tokio::time::Duration;
use tracing::*;

/// Controller triggers this whenever our main object or our children changed
async fn reconcile(generator: Arc<OpenOnDemand>, ctx: Arc<Data>) -> Result<Action, Error> {
    let client = &ctx.client;
    let oref = generator.controller_owner_ref(&()).unwrap();
    let ood_instance_name = generator.metadata.name.clone().unwrap();
    let current_namespace = generator
        .metadata
        .namespace
        .as_ref()
        .ok_or_else(|| Error::MissingObjectKey(".metadata.namespace"))?;
    let mut labels = BTreeMap::new();
    labels.insert("app".to_string(), "ood".to_string());
    labels.insert("ood-cluster".to_string(), ood_instance_name.clone());

    let spec_generator = OODSpecGenerator::new(
        generator.spec.clone(),
        ood_instance_name.clone(),
        current_namespace.to_string(),
        oref.clone(),
        labels.clone(),
    );

    let svc_api = Api::<Service>::namespaced(client.clone(), current_namespace);
    let cm_api = Api::<ConfigMap>::namespaced(client.clone(), current_namespace);
    let fep_api = Api::<FrontEndProxy>::namespaced(client.clone(), current_namespace);
    let ood_api = Api::<OpenOnDemand>::namespaced(client.clone(), current_namespace);

    // Generate desired state
    let svc = spec_generator.svc()?;
    let ood_cm = spec_generator.ood_cm()?;
    let clusters_cm = spec_generator.cluster_cm()?;
    let fep = spec_generator.fep(generator.object_ref(&()))?;

    // Apply desired state
    cm_api
        .patch(
            ood_cm.metadata.name.as_ref().unwrap(),
            &PatchParams::apply("openondemands.ondemand.dev"),
            &Patch::Apply(&ood_cm),
        )
        .await
        .unwrap();
    cm_api
        .patch(
            clusters_cm.metadata.name.as_ref().unwrap(),
            &PatchParams::apply("openondemands.ondemand.dev"),
            &Patch::Apply(&clusters_cm),
        )
        .await
        .unwrap();
    let new_fep = fep_api
        .patch(
            fep.metadata.name.as_ref().unwrap(),
            &PatchParams::apply("frontendproxies.ondemand.dev"),
            &Patch::Apply(&fep),
        )
        .await
        .unwrap();
    svc_api
        .patch(
            svc.metadata
                .name
                .as_ref()
                .ok_or_else(|| Error::MissingObjectKey(".metadata.namespace"))?,
            &PatchParams::apply("frontendproxies.ondemand.dev"),
            &Patch::Apply(&svc),
        )
        .await
        .map_err(Error::SvcCreationFailed)?;

    // Check for conditions
    let mut conditions = OODConditions::new();

    let fep_ready = {
        let gener = new_fep.metadata.generation;
        new_fep.status.as_ref().map(|status| {
            status.conditions.iter().any(|cond| {
                cond.type_ == "Ready" && cond.status == "True" && cond.observed_generation == gener
            })
        })
    }
    .unwrap_or(false);

    if fep_ready {
        conditions.fep(
            generator.metadata.generation,
            "FEP is ready".to_string(),
            "True".to_string(),
        );
        conditions.ready(
            generator.metadata.generation,
            "All resources ready".to_string(),
            "True".to_string(),
        );
    } else {
        conditions.fep(
            generator.metadata.generation,
            "Awaiting FEP readiness".to_string(),
            "False".to_string(),
        );
        conditions.ready(
            generator.metadata.generation,
            "Awaiting resources readiness".to_string(),
            "False".to_string(),
        );
    }

    // Patch status
    let status = conditions.get_patch();
    ood_api
        .patch_status(
            &generator.metadata.name.clone().unwrap(),
            &PatchParams::default(),
            &Patch::Merge(status),
        )
        .await
        .unwrap();
    Ok(Action::requeue(Duration::from_secs(300)))
}

/// The controller triggers this on reconcile errors
fn error_policy(_object: Arc<OpenOnDemand>, _error: &Error, _ctx: Arc<Data>) -> Action {
    Action::requeue(Duration::from_secs(1))
}

// Data we want access to in error/reconcile calls
struct Data {
    client: Client,
}

pub async fn controller() -> Result<()> {
    let client = Client::try_default().await?;

    // Api clients
    let oods = Api::<OpenOnDemand>::all(client.clone());
    let cms = Api::<ConfigMap>::all(client.clone());
    let svcs = Api::<Service>::all(client.clone());
    let feps = Api::<FrontEndProxy>::all(client.clone());

    // limit the controller to running a maximum of two concurrent reconciliations
    let config = Config::default().concurrency(2);

    Controller::new(oods, watcher::Config::default())
        .watches(cms, watcher::Config::default(), |ar| {
            if let Some(labels) = ar.metadata.labels {
                if let Some(app_label) = labels.get("ood-cluster") {
                    Some(ObjectRef::new(app_label).within(&ar.metadata.namespace.unwrap()))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .watches(svcs, watcher::Config::default(), |ar| {
            if let Some(labels) = ar.metadata.labels {
                if let Some(app_label) = labels.get("ood-cluster") {
                    Some(ObjectRef::new(app_label).within(&ar.metadata.namespace.unwrap()))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .owns(feps, watcher::Config::default())
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
    info!("controller terminated");
    Ok(())
}
