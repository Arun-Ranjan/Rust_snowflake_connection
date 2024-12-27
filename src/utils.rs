use csv::ReaderBuilder;
use snowflake_connector_rs::{SnowflakeAuthMethod, SnowflakeClient, SnowflakeClientConfig, SnowflakeSession};
use std::fs::File;

// https://wy19158.central-india.azure.snowflakecomputing.com


// Functions
pub async  fn authenticate_user(username: &str, password: &str) -> Result<SnowflakeClient, Box<dyn std::error::Error>> {
    let config = SnowflakeClientConfig {
        account: "wy19158.central-india.azure".to_string(),
        role: Some("ACCOUNTADMIN".to_string()),
        warehouse: Some("COMPUTE_WH".to_string()),
        database: Some("TRAININGDB".to_string()),
        schema: Some("SALES".to_string()),
        timeout: Some(std::time::Duration::from_secs(30)),
    };

    let auth_method = SnowflakeAuthMethod::Password(password.to_string());

    // Create and return a SnowflakeClient
    let client = SnowflakeClient::new(username, auth_method, config)?;
    
    Ok(client)
}

pub async fn create_table(session: &SnowflakeSession) -> Result<(), Box<dyn std::error::Error>> {
    let create_table_sql = "
        CREATE OR REPLACE TABLE IRIS_TABLE (
            C1 NUMBER(5,2),
            C2 NUMBER(5,2),
            C3 NUMBER(5,2),
            C4 NUMBER(5,2),
            NAME VARCHAR(16777216)
        );
    ";

    session.execute(create_table_sql).await?;
    Ok(())
}

pub async fn insert_csv(session: &SnowflakeSession, file_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let file = File::open(file_path)?;
    let mut rdr = ReaderBuilder::new().has_headers(false).from_reader(file);

    for result in rdr.records() {
        let record = result?;
        let row: Vec<String> = record.iter().map(|field| field.to_string()).collect();
        
        let values_str: String = row
            .iter()
            .map(|field| format!("'{}'", field.replace("'", "''")))  // Escape single quotes
            .collect::<Vec<String>>()
            .join(", ");

        let insert_sql = format!(
            "INSERT INTO IRIS_TABLE (C1, C2, C3, C4, NAME) VALUES ({})",
            values_str
        );

        session.execute(insert_sql.as_str()).await?;
    }
    
    Ok(())
}

pub async fn query_data(session: &SnowflakeSession) -> Result<(), Box<dyn std::error::Error>> {
    let query_sql = "SELECT * FROM IRIS_TABLE WHERE C1=5.10;";
    let result = session.execute(query_sql).await?;
    let rows = result.fetch_all().await?;
    // Process the result (this will depend on your database client)
    for row in rows {
        println!("{:?}", row); // Adjust depending on the structure of row
    }

    Ok(())
}