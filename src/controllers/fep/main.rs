// Nightly clippy (0.1.64) considers Drop a side effect, see https://github.com/rust-lang/rust-clippy/issues/9608
#![allow(clippy::unnecessary_lazy_evaluations)]

use crate::controllers::fep::generator;
use crate::controllers::fep::types::Error;
use crate::crds::{FrontEndProxy, InteractiveApp};
use anyhow::Result;
use futures::StreamExt;
use k8s_openapi::api::{
    apps::v1::Deployment,
    core::v1::*,
    rbac::v1::{Role, RoleBinding},
};
use kube::{
    Client,
    api::{Api, Patch, PatchParams, Resource},
    runtime::{
        controller::{Action, Config, Controller},
        watcher,
    },
};
use std::sync::Arc;
use tokio::time::Duration;
use tracing::*;

/// Controller triggers this whenever our main object or our children changed
async fn reconcile(generator: Arc<FrontEndProxy>, ctx: Arc<Data>) -> Result<Action, Error> {
    let client = &ctx.client;

    let oref = generator.controller_owner_ref(&()).unwrap();

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

    let fep_spec = generator.spec.clone();
    let spec_generator =
        generator::FEPSpecGenerator::new(fep_spec, current_namespace.to_string(), oref, labels);

    let role = spec_generator.role()?;

    let role_binding = spec_generator.role_binding()?;

    let service_account = spec_generator.service_account()?;

    let role_api = Api::<Role>::namespaced(client.clone(), current_namespace);

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

    let rolebinding_api = Api::<RoleBinding>::namespaced(client.clone(), current_namespace);

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

    let cm_api = Api::<ConfigMap>::namespaced(client.clone(), &current_namespace);

    let template_cm = spec_generator.pun_template_config_map()?;
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

    let sa_api = Api::<ServiceAccount>::namespaced(client.clone(), current_namespace);

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


    // Pod creation
    let deploy_api = Api::<Deployment>::namespaced(client.clone(), current_namespace);

    let deploy = spec_generator.deployment()?;

    deploy_api
        .patch(
            deploy
                .metadata
                .name
                .as_ref()
                .ok_or_else(|| Error::MissingObjectKey(".metadata.name"))?,
            &PatchParams::apply("deployments"),
            &Patch::Apply(&deploy),
        )
        .await
        .map_err(Error::HTTPDPodCreationFailed)?;

    Ok(Action::requeue(Duration::from_secs(300)))
}

/// The controller triggers this on reconcile errors
fn error_policy(_object: Arc<FrontEndProxy>, _error: &Error, _ctx: Arc<Data>) -> Action {
    Action::requeue(Duration::from_secs(1))
}

// Data we want access to in error/reconcile calls
struct Data {
    client: Client,
}

pub async fn controller() -> Result<()> {
    let client = Client::try_default().await?;

    // Api clients
    let feps = Api::<FrontEndProxy>::all(client.clone());
    let sas = Api::<ServiceAccount>::all(client.clone());
    let deployments = Api::<Deployment>::all(client.clone());
    let iapps = Api::<InteractiveApp>::all(client.clone());

    // limit the controller to running a maximum of two concurrent reconciliations
    let config = Config::default().concurrency(1);

    Controller::new(feps, watcher::Config::default())
        .owns(deployments, watcher::Config::default())
        .owns(iapps, watcher::Config::default())
        .owns(sas, watcher::Config::default())
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
