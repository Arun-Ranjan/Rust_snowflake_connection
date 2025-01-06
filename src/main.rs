mod utils;
use std::env;


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let client = utils::authenticate_user("arunsahu159", "2786Arun!").await?;

    let session = client.create_session().await?;
 
    let action = env::args().nth(1).expect("No action specified. Please provide 'create and insert' or 'query'.");

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
