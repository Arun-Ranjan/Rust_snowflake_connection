use csv::ReaderBuilder;
use snowflake_connector_rs::{SnowflakeAuthMethod, SnowflakeClient, SnowflakeClientConfig, SnowflakeSession};
use std::fs::File;
use std::io;
// https://wy19158.central-india.azure.snowflakecomputing.com




pub async fn upload_csv_to_snowflake(session: &SnowflakeSession) -> Result<(), Box<dyn std::error::Error>> {

    let mut create_table_sql = String::new();
    io::stdin().read_line(&mut create_table_sql)?;
    session.execute(create_table_sql).await?;
    println!("Table created successfully.");
 
    // File path to the CSV file
    let file_path = r"E:\Training\rust\snoflake_connector_rs\connector\iris_data.csv";
    let file = File::open(file_path)?;
 
    // Read CSV file and insert records into the Snowflake table
    let mut rdr = ReaderBuilder::new().has_headers(false).from_reader(file);

    // INSERT INTO MAIN_TABLE (SepalLength,SepalWidth,PetalLength,PetalWidth,Species)
    let mut insert_query:String = String::new();
    io::stdin().read_line(&mut insert_query)?;
    let insert_query = insert_query.trim(); 


    for result in rdr.records() {
        let record = result?;
        let row: Vec<String> = record.iter().map(|field| field.to_string()).collect();
        
 
        // Format the row values and execute the INSERT query
        let values_str = row.iter()
            .map(|field| format!("'{}'", field.replace("'", "''")))
            .collect::<Vec<String>>()
            .join(", ");

        
        let insert_sql = format!("{} VALUES ({})", insert_query,values_str);
        session.execute(insert_sql).await?;
    }

    Ok(())
}

pub async fn execute_req_query(session: &SnowflakeSession) -> Result<(), Box<dyn std::error::Error>> {
    
    let mut query = String::new();
    io::stdin().read_line(&mut query)?;
    
    // Execute the query
    let results = session.execute(query.trim()).await?;
    let rows = results.fetch_all().await?;
    for row in rows{
        println!("{:?}",row);
    }
    println!("Query executed successfully.");
    Ok(())
}


// CREATE OR REPLACE TABLE MAIN_TABLE (SepalLength NUMBER(3,2),SepalWidth NUMBER(3,2),PetalLength NUMBER(3,2),PetalWidth NUMBER(3,2),Species STRING);
// INSERT INTO MAIN_TABLE (SepalLength,SepalWidth,PetalLength,PetalWidth,Species)