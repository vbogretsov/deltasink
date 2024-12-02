use std::error::Error;

use reqwest::blocking::Client as HttpClient;
use serde::{Deserialize, Serialize};
use tracing::{info, debug, error};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Subject {
    pub id: i32,
    pub schema: String,
    pub schema_type: String,
    pub references: Option<Vec<Reference>>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Reference {
    pub name: String,
    pub subject: String,
    pub version: i32,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SubjectResponse {
    id: i32,
    version: i32,
    schema: String,
    schema_type: String,
    references: Option<Vec<Reference>>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct VersionsResponse(Vec<i32>);

pub struct Client {
    client: HttpClient,
    url: String,
}

#[inline]
fn sr_versions_url(base_url: &str, subject: &str) -> String {
    format!("{}/subjects/{}-value/versions", base_url, subject)
}

#[inline]
fn sr_schema_url(base_url: &str, subject: &str, version: i32) -> String {
    format!("{}/subjects/{}-value/versions/{}", base_url, subject, version)
}

impl Client {
    pub fn new(client: HttpClient, url: String) -> Self {
        Self { client, url }
    }

    pub fn get_versions(
        &self,
        subject: &str,
    ) -> Result<Vec<i32>, Box<dyn Error>> {
        info!(
            registry_url = self.url,
            subject = subject,
            "fetching versions IDs from schema registry",
        );

        let url = sr_versions_url(&self.url, subject);
        debug!(
            url = url,
            "performing HTTP GET",
        );
        let res = self.client.get(&url).send()?;

        match res.status() {
            reqwest::StatusCode::OK => {
                Ok(res.json::<VersionsResponse>()?.0)
            },
            _ => {
                error!(
                    url = url,
                    "HTTP request failed",
                );
                Err(format!("failed to get versions for '{}'", subject).into())
            }
        }
    }

    pub fn get_subject(
        &self,
        subject: &str,
        version: i32,
    ) -> Result<Subject, Box<dyn Error>> {
        info!(
            registry_url = self.url,
            subject = subject,
            version = version,
            "fetching schema version",
        );

        let url = sr_schema_url(&self.url, subject, version);
        debug!(
            url = url,
            "performing HTTP GET",
        );
        let res = self.client.get(&url).send()?;

        match res.status() {
            reqwest::StatusCode::OK => {
                let response = res.json::<SubjectResponse>()?;
                Ok(Subject{
                    id: response.id,
                    schema: response.schema,
                    schema_type: response.schema_type,
                    references: response.references,
                })
            }
            _ => {
                error!(
                    url = url,
                    status = res.status().as_u16(),
                    "HTTP request failed",
                );
                Err(format!("failed to get schema for '{}:{}'", subject, version).into())
            }
        }
    }
}
