use aws_lambda_events::event::sqs::SqsEvent;
use lambda_runtime::{run, service_fn, tracing, Error, LambdaEvent};

use aws_config::BehaviorVersion;
use aws_sdk_s3::primitives::ByteStream;
use datafusion::arrow::json;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use url::Url;
// use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::*;
// use env_logger::Env;
use object_store::aws::AmazonS3Builder;
use tokio::time::Instant;

/// This is the main body for the function.
/// Write your code inside it.
/// There are some code example in the following URLs:
/// - https://github.com/awslabs/aws-lambda-rust-runtime/tree/main/examples
/// - https://github.com/aws-samples/serverless-rust-demo/
async fn function_handler(event: LambdaEvent<SqsEvent>) -> Result<(), Error> {
    // Extract some useful information from the request
    let recs = event.payload.records;
    let bek_ids: Vec<u16> = recs
        .iter()
        .filter_map(|sqs_message| sqs_message.body.as_ref()) // Filter out None values
        .filter_map(|body| {
            body.split(":")
                .nth(1) // Extract the second part after splitting by ":"
                .and_then(|bek_id_str| bek_id_str.trim().parse().ok()) // Parse to u16
        })
        .collect();

    let mut query_tasks = Vec::new();
    for i in bek_ids {
        query_tasks.push(tokio::spawn(compute(i)));
    }

    for task in query_tasks {
        let _ = task.await.expect("waiting failed");
    }

    let message = "Finished executing all tasks";

    Ok(())
}

async fn compute(id: u16) -> Result<(), DataFusionError> {
    let start = Instant::now();
    let bucket_name = "pensioncalcseast1";

    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let s3_client = aws_sdk_s3::Client::new(&config);
    let s3 = AmazonS3Builder::from_env()
        .with_bucket_name(bucket_name)
        .build()
        .expect("Failed to initialize s3");

    let s3_path = format!("s3://{bucket_name}");
    let s3_url = Url::parse(&s3_path).unwrap();
    // create local session context
    let ctx = SessionContext::new();

    let file_name = format!("part_account/{id}/pa_detail.parquet");
    ctx.runtime_env()
        .register_object_store(&s3_url, Arc::new(s3));
    let path = format!("s3://{bucket_name}/{file_name}");
    ctx.register_parquet(&format!("pad{id}"), &path, ParquetReadOptions::default())
        .await?;
    let query = (1..=48)
        .map(|i| format!("amount{}", i))
        .map(|column_name| format!("SUM({}) as {}", column_name, column_name))
        .collect::<Vec<String>>()
        .join(", ");

    let sql_query = format!(
        "SELECT stop_date, {} FROM pad{} GROUP BY stop_date",
        query, id
    );
    // execute the query
    let df = ctx.sql(&sql_query).await?;

    let filename = format!("/tmp/result{}.json", id);

    let path = Path::new(&filename);
    let file = fs::File::create(path)?;

    let mut writer = json::LineDelimitedWriter::new(file);

    let recs = df.collect().await?;
    for rec in recs {
        writer.write(&rec).expect("Write failed")
    }
    writer.finish().unwrap();

    let s3_key = format!("results/result{}.json", id);

    let body = ByteStream::from_path(Path::new(&filename)).await;

    let response = s3_client
        .put_object()
        .bucket(bucket_name)
        .body(body.unwrap())
        .key(&s3_key)
        .send()
        .await;

    match response {
        Ok(_) => {
            tracing::info!(
                filename = %filename,
                "data successfully stored in S3",
            );
            // Return `Response` (it will be serialized to JSON automatically by the runtime)
        }
        Err(err) => {
            // In case of failure, log a detailed error to CloudWatch.
            tracing::error!(
                err = %err,
                filename = %filename,
                "failed to upload data to S3"
            );
        }
    }

    let end = Instant::now();
    tracing::info!(
        "Finished executing for task {} in time {:?}",
        id,
        end - start
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing::init_default_subscriber();

    run(service_fn(function_handler)).await
}
