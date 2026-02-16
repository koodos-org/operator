// Nightly clippy (0.1.64) considers Drop a side effect, see https://github.com/rust-lang/rust-clippy/issues/9608
#![allow(clippy::unnecessary_lazy_evaluations)]
use crate::controllers::ood::generator::OODSpecGenerator;
use crate::controllers::ood::types::Error;
use crate::crds::{FrontEndProxy, OpenOnDemand, Pun};
use crate::utils::status::OODConditions;
use anyhow::Result;
use futures::StreamExt;
use k8s_openapi::api::core::v1::*;
use kube::api::{DeleteParams, ListParams};
use kube::{
    Client,
    api::{Api, Patch, PatchParams, Resource},
    runtime::{
        controller::{Action, Config, Controller},
        watcher,
    },
};
use kube_runtime::finalizer;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::{collections::BTreeMap, sync::Arc};
use tokio::time::Duration;
use tracing::*;

/// Controller triggers this whenever our main object or our children changed
async fn reconcile(generator: Arc<OpenOnDemand>, ctx: Arc<Data>) -> Result<Action, Error> {
    let client = &ctx.client;
    let oref = generator
        .controller_owner_ref(&())
        .ok_or(Error::GenericError("No owner ref from object"))?;
    let ood_instance_name = generator
        .metadata
        .name
        .clone()
        .ok_or(Error::MissingObjectKey(".metadata.name"))?;
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
    let fep = spec_generator.fep(generator.object_ref(&()))?;

    let ood_cm = spec_generator.ood_cm()?;
    let nginx_stage_cm = spec_generator.nginx_stage_cm()?;
    let clusters_cm = spec_generator.cluster_cm()?;
    let ondemand_conf_cm = spec_generator.ondemand_cm()?;
    let mut config_maps = vec![
        Some(ood_cm),
        Some(nginx_stage_cm),
        clusters_cm,
        ondemand_conf_cm,
    ];
    let mut hash_state = DefaultHasher::new();
    // Hash generation so that old config maps are not reused
    generator.metadata.generation.hash(&mut hash_state);
    for cm in &config_maps {
        cm.as_ref().map(|cm| cm.data.hash(&mut hash_state));
    }
    let config_hash = format!("{:x}", hash_state.finish());
    for cm in config_maps.iter_mut() {
        if let Some(cm) = cm {
            let hashed_name = cm
                .metadata
                .name
                .clone()
                .map(|name| format!("{name}-{config_hash}"));
            cm.metadata.name = hashed_name;
        }
    }
    // Hash based CMs
    for cm in config_maps {
        if let Some(cm) = cm {
            cm_api
                .patch(
                    cm.metadata
                        .name
                        .as_ref()
                        .ok_or(Error::MissingObjectKey(".metadata.name"))?,
                    &PatchParams::apply("openondemands.ondemand.dev"),
                    &Patch::Apply(&cm),
                )
                .await
                .map_err(Error::ApiCreationFailure)?;
        }
    }
    let hash_status = serde_json::json!({
        "status": {
            "config_hash": config_hash
        }
    });

    ood_api
        .patch_status(
            &generator
                .metadata
                .name
                .clone()
                .ok_or(Error::MissingObjectKey(".metadata.name"))?,
            &PatchParams::default(),
            &Patch::Merge(hash_status),
        )
        .await
        .map_err(Error::ApiCreationFailure)?;

    // Apply desired state
    let new_fep = fep_api
        .patch(
            fep.metadata
                .name
                .as_ref()
                .ok_or(Error::MissingObjectKey(".metadata.name"))?,
            &PatchParams::apply("frontendproxies.ondemand.dev"),
            &Patch::Apply(&fep),
        )
        .await
        .map_err(Error::ApiCreationFailure)?;
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
            &generator
                .metadata
                .name
                .clone()
                .ok_or(Error::MissingObjectKey(".metadata.name"))?,
            &PatchParams::default(),
            &Patch::Merge(status),
        )
        .await
        .map_err(Error::ApiCreationFailure)?;

    // Clean up old config maps
    let config_maps = cm_api
        .list(&ListParams::default().labels(&format!("app=ood,ood-cluster={}", ood_instance_name)))
        .await
        .map_err(Error::ListConfigMapFailed)?;

    let ood_cms = config_maps.clone();
    let mut ood_cms: Vec<&ConfigMap> = ood_cms
        .iter()
        .filter(|cm| {
            cm.metadata
                .name
                .as_ref()
                .unwrap_or(&"".to_string())
                .starts_with(&format!("{}-ood-config-files", ood_instance_name))
        })
        .collect();
    ood_cms.sort_by_key(|k| k.metadata.creation_timestamp.as_ref());
    for cm in ood_cms.iter().rev().skip(5) {
        cm_api
            .delete(
                &cm.metadata
                    .name
                    .as_ref()
                    .ok_or(Error::MissingObjectKey(".metadata.name"))?,
                &DeleteParams::default(),
            )
            .await
            .map_err(Error::DeleteFailed)?;
    }

    let cluster_cms = config_maps.clone();
    let mut cluster_cms: Vec<&ConfigMap> = cluster_cms
        .iter()
        .filter(|cm| {
            cm.metadata
                .name
                .as_ref()
                .unwrap_or(&"".to_string())
                .starts_with(&format!("{}-ood-cluster-config-files", ood_instance_name))
        })
        .collect();
    cluster_cms.sort_by_key(|k| k.metadata.creation_timestamp.as_ref());
    for cm in cluster_cms.iter().rev().skip(5) {
        cm_api
            .delete(
                &cm.metadata
                    .name
                    .as_ref()
                    .ok_or(Error::MissingObjectKey(".metadata.name"))?,
                &DeleteParams::default(),
            )
            .await
            .map_err(Error::DeleteFailed)?;
    }

    Ok(Action::requeue(Duration::from_secs(300)))
}

async fn cleanup_puns(generator: Arc<OpenOnDemand>, ctx: Arc<Data>) -> Result<Action, Error> {
    let client = &ctx.client;
    let ood_instance_name = generator
        .metadata
        .name
        .clone()
        .ok_or(Error::MissingObjectKey(".metadata.name"))?;

    let pun_api = Api::<Pun>::all(client.clone());

    let res = pun_api
        .list(
            &ListParams::default()
                .fields(&format!("spec.ood_instance_ref.name={}", ood_instance_name)),
        )
        .await;
    if let Ok(puns) = res {
        for pun in puns {
            let pun_api = Api::<Pun>::namespaced(
                client.clone(),
                &pun.metadata
                    .namespace
                    .ok_or(Error::MissingObjectKey(".metadata.namespace"))?,
            );
            pun_api
                .delete(
                    &pun.metadata
                        .name
                        .ok_or(Error::MissingObjectKey(".metadata.name"))?,
                    &DeleteParams::default(),
                )
                .await
                .map_err(Error::DeleteFailed)?;
        }
    };
    Ok(Action::await_change())
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
    let feps = Api::<FrontEndProxy>::all(client.clone());

    // limit the controller to running a maximum of two concurrent reconciliations
    let config = Config::default().concurrency(2);

    Controller::new(oods.clone(), watcher::Config::default())
        .owns(feps, watcher::Config::default())
        .with_config(config)
        .shutdown_on_signal()
        .run(
            |generator, ctx| {
                let namespace = generator
                    .metadata
                    .namespace
                    .clone()
                    .unwrap_or("default".to_string());
                let ood_api = Api::namespaced(ctx.client.clone(), &namespace);
                async move {
                    finalizer(
                        &ood_api,
                        "ondemand-pun.dev/cleanup",
                        generator,
                        |event| async {
                            match event {
                                finalizer::Event::Apply(ood) => reconcile(ood, ctx).await,
                                finalizer::Event::Cleanup(ood) => cleanup_puns(ood, ctx).await,
                            }
                        },
                    )
                    .await
                    .map_err(|e| Error::FinalizerFailure(Box::new(e)))
                }
            },
            error_policy,
            Arc::new(Data { client }),
        )
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
