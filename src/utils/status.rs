use chrono::Utc;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, Time};
use serde_json::Value;

fn status_patch(conditions: Vec<Condition>) -> Value {
    serde_json::json!(
    {
        "status": {
            "conditions": conditions
        }
    })
}

// PUN Controller conditions
pub struct PUNConditions {
    deployment: Condition,
    ready: Condition,
}

pub struct FEPConditions {
    deployment: Condition,
    ready: Condition,
}

pub struct OODConditions {
    fep: Condition,
    ready: Condition,
}

impl PUNConditions {
    pub fn new() -> Self {
        PUNConditions {
            deployment: Condition {
                status: "Unknown".to_string(),
                type_: "DeploymentReady".to_string(),
                last_transition_time: Time(Utc::now()),
                message: String::new(),
                observed_generation: None,
                reason: String::new(),
            },
            ready: Condition {
                status: "Unknown".to_string(),
                type_: "Ready".to_string(),
                last_transition_time: Time(Utc::now()),
                message: String::new(),
                observed_generation: None,
                reason: String::new(),
            },
        }
    }

    pub fn deployment(&mut self, generation: Option<i64>, message: String, status: String) {
        self.deployment = Condition {
            last_transition_time: Time(Utc::now()),
            message,
            observed_generation: generation,
            reason: String::new(),
            status,
            type_: format!("DeploymentReady"),
        }
    }
    pub fn ready(&mut self, generation: Option<i64>, message: String, status: String) {
        self.ready = Condition {
            last_transition_time: Time(Utc::now()),
            message,
            observed_generation: generation,
            reason: String::new(),
            status,
            type_: format!("Ready"),
        }
    }

    pub fn get_patch(self) -> Value {
        let mut conditions = vec![];
        conditions.push(self.deployment);
        conditions.push(self.ready);

        status_patch(conditions)
    }
}

impl FEPConditions {
    pub fn new() -> Self {
        FEPConditions {
            deployment: Condition {
                status: "Unknown".to_string(),
                type_: "DeploymentReady".to_string(),
                last_transition_time: Time(Utc::now()),
                message: String::new(),
                observed_generation: None,
                reason: String::new(),
            },
            ready: Condition {
                status: "Unknown".to_string(),
                type_: "Ready".to_string(),
                last_transition_time: Time(Utc::now()),
                message: String::new(),
                observed_generation: None,
                reason: String::new(),
            },
        }
    }

    pub fn deployment(&mut self, generation: Option<i64>, message: String, status: String) {
        self.deployment = Condition {
            last_transition_time: Time(Utc::now()),
            message,
            observed_generation: generation,
            reason: String::new(),
            status,
            type_: format!("DeploymentReady"),
        }
    }
    pub fn ready(&mut self, generation: Option<i64>, message: String, status: String) {
        self.ready = Condition {
            last_transition_time: Time(Utc::now()),
            message,
            observed_generation: generation,
            reason: String::new(),
            status,
            type_: format!("Ready"),
        }
    }

    pub fn get_patch(self) -> Value {
        let mut conditions = vec![];
        conditions.push(self.deployment);
        conditions.push(self.ready);

        status_patch(conditions)
    }
}

impl OODConditions {
    pub fn new() -> Self {
        OODConditions {
            fep: Condition {
                status: "Unknown".to_string(),
                type_: "FEPReady".to_string(),
                last_transition_time: Time(Utc::now()),
                message: String::new(),
                observed_generation: None,
                reason: String::new(),
            },
            ready: Condition {
                status: "Unknown".to_string(),
                type_: "Ready".to_string(),
                last_transition_time: Time(Utc::now()),
                message: String::new(),
                observed_generation: None,
                reason: String::new(),
            },
        }
    }

    pub fn fep(&mut self, generation: Option<i64>, message: String, status: String) {
        self.fep = Condition {
            last_transition_time: Time(Utc::now()),
            message,
            observed_generation: generation,
            reason: String::new(),
            status,
            type_: format!("DeploymentReady"),
        }
    }
    pub fn ready(&mut self, generation: Option<i64>, message: String, status: String) {
        self.ready = Condition {
            last_transition_time: Time(Utc::now()),
            message,
            observed_generation: generation,
            reason: String::new(),
            status,
            type_: format!("Ready"),
        }
    }

    pub fn get_patch(self) -> Value {
        let mut conditions = vec![];
        conditions.push(self.fep);
        conditions.push(self.ready);

        status_patch(conditions)
    }
}
