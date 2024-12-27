mod utils;
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let client = utils::authenticate_user("arunsahu159", "2786Arun!").await?;
    
    let session = client.create_session().await?;
 
    let action = env::args().nth(1).expect("No action specified. Please provide 'create and insert' or 'query'.");

    match action.as_str() {
        "create" => {
            println!("Creating the table & uploading the data into the table");
            let file_path = r"E:\Training\rust\snoflake_connector_rs\connector\iris_data.csv";
            utils::create_table(&session).await?;
            utils::insert_csv(&session, file_path).await?;

        }
        "query" => {
            println!("Querying the database...");
            utils::query_data(&session).await?;
        }
        _ => {
            eprintln!("Invalid action. Use 'create and insert', or 'query'.");
        }
    }

    Ok(())
}
