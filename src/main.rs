mod utils;
use std::env;
use snowflake_connector_rs::{SnowflakeAuthMethod, SnowflakeClient, SnowflakeClientConfig, SnowflakeSession};
use std::time::{Duration,Instant};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let args: Vec<String> = env::args().collect();


    let user = args[1].clone();
    let password = args[2].clone();
    let account = "wy19158.central-india.azure".to_string();
    let role = Some("ACCOUNTADMIN".to_string());
    let warehouse = Some("COMPUTE_WH".to_string());
    let database = Some("TRAININGDB".to_string());
    let schema = Some("SALES".to_string());
    let timeout = 60; // Timeout in seconds (e.g., 60 seconds)
 
    // Initialize Snowflake client
    let client = SnowflakeClient::new(
        &user,
        SnowflakeAuthMethod::Password(password),
        SnowflakeClientConfig {
            account,
            role,
            warehouse,
            database,
            schema,
            timeout: Some(Duration::from_secs(timeout)),
        },
    )?;
    let session = client.create_session().await?;
    let action = &args[3];
    match action.as_str() {
        "create" => {
            utils::upload_csv_to_snowflake(&session).await?;
            println!("Files uploaded");

        }
        "query" => {
            println!("Querying the database.");
            utils::execute_req_query(&session).await?;
        }
        _ => {
            eprintln!("Invalid action. Use 'create and insert', or 'query'.");
        }
    }

    Ok(())
}
