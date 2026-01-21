use serde::{Deserialize, Serialize};
use reqwest::blocking::Client;
use chrono::Utc;
use anyhow::{Result, anyhow};

pub struct MlflowClient {
    base_url: String,
    client: Client,
    experiment_id: String,
}

#[derive(Serialize)]
struct CreateRunRequest {
    experiment_id: String,
    start_time: i64,
}

#[derive(Deserialize)]
struct CreateRunResponse {
    run: RunInfo,
}

#[derive(Deserialize)]
struct RunInfo {
    info: RunInfoDetails,
}

#[derive(Deserialize)]
struct RunInfoDetails {
    run_id: String,
}

#[derive(Serialize)]
struct LogParamRequest {
    run_id: String,
    key: String,
    value: String,
}

#[derive(Serialize)]
struct LogMetricRequest {
    run_id: String,
    key: String,
    value: f64,
    timestamp: i64,
    step: i64,
}

impl MlflowClient {
    pub fn new(base_url: &str, experiment_name: &str) -> Result<Self> {
        let client = Client::new();
        let experiment_id = Self::get_or_create_experiment(&client, base_url, experiment_name)?;
        
        Ok(Self {
            base_url: base_url.to_string(),
            client,
            experiment_id,
        })
    }

    fn get_or_create_experiment(client: &Client, base_url: &str, name: &str) -> Result<String> {
        // For brevity, we'll try to create it. If it exists, we'll fetch ID (Simplified)
        let resp = client.post(format!("{}/api/2.0/mlflow/experiments/create", base_url))
            .json(&serde_json::json!({ "name": name }))
            .send();
        
        if let Ok(r) = resp {
            if let Ok(data) = r.json::<serde_json::Value>() {
                if let Some(id) = data["experiment_id"].as_str() {
                    return Ok(id.to_string());
                }
            }
        }
        
        // Fallback or handle error
        Ok("0".to_string()) // Default experiment
    }

    pub fn start_run(&self) -> Result<ActiveRun> {
        let req = CreateRunRequest {
            experiment_id: self.experiment_id.clone(),
            start_time: Utc::now().timestamp_millis(),
        };

        let resp = self.client.post(format!("{}/api/2.0/mlflow/runs/create", self.base_url))
            .json(&req)
            .send()?
            .json::<CreateRunResponse>()?;

        Ok(ActiveRun {
            client: self.client.clone(),
            base_url: self.base_url.clone(),
            run_id: resp.run.info.run_id,
        })
    }
}

pub struct ActiveRun {
    client: Client,
    base_url: String,
    run_id: String,
}

impl ActiveRun {
    pub fn log_param(&self, key: &str, value: &str) -> Result<()> {
        let req = LogParamRequest {
            run_id: self.run_id.clone(),
            key: key.to_string(),
            value: value.to_string(),
        };
        self.client.post(format!("{}/api/2.0/mlflow/runs/log-parameter", self.base_url))
            .json(&req)
            .send()?;
        Ok(())
    }

    pub fn log_metric(&self, key: &str, value: f64, step: i64) -> Result<()> {
        let req = LogMetricRequest {
            run_id: self.run_id.clone(),
            key: key.to_string(),
            value,
            timestamp: Utc::now().timestamp_millis(),
            step,
        };
        self.client.post(format!("{}/api/2.0/mlflow/runs/log-metric", self.base_url))
            .json(&req)
            .send()?;
        Ok(())
    }

    pub fn finish(&self) -> Result<()> {
        let req = serde_json::json!({
            "run_id": self.run_id,
            "status": "FINISHED",
            "end_time": Utc::now().timestamp_millis()
        });
        self.client.post(format!("{}/api/2.0/mlflow/runs/update", self.base_url))
            .json(&req)
            .send()?;
        Ok(())
    }
}
