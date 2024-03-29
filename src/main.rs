use aws_lambda_events::event::sqs::SqsEvent;
use aws_lambda_events::sqs::{BatchItemFailure, SqsBatchResponse};
use lambda_runtime::{run, service_fn, tracing, Error, LambdaEvent};

use aws_config::BehaviorVersion;
use aws_sdk_s3::primitives::ByteStream;
use datafusion::arrow::json;
use datafusion::error::Result;
use datafusion::prelude::*;
use object_store::aws::AmazonS3Builder;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use url::Url;

async fn function_handler(event: LambdaEvent<SqsEvent>) -> Result<SqsBatchResponse, Error> {
    let recs = event.payload.records;
    let mut batch_item_failures: Vec<BatchItemFailure> = vec![];

    let (bek_ids, bek_id_map): (Vec<u16>, HashMap<u16, String>) = recs
        .iter()
        .filter_map(|sqs_message| {
            sqs_message
                .body
                .as_ref()
                .map(|body| (sqs_message.message_id.clone(), body))
        })
        .filter_map(|(message_id, body)| {
            body.split(":")
                .nth(1) // Extract the second part after splitting by ":"
                .and_then(|bek_id_str| bek_id_str.trim().parse().ok()) // Parse to u16
                .map(|bek_id: u16| (bek_id, message_id.clone()))
        })
        .fold(
            (Vec::new(), HashMap::new()),
            |(mut ids, mut map), (bek_id, message_id)| {
                ids.push(bek_id);
                map.insert(bek_id, message_id.unwrap());
                (ids, map)
            },
        );
    let mut query_tasks = Vec::new();
    for i in bek_ids {
        query_tasks.push(tokio::spawn(compute(i)));
    }
    // Await on all handles to ensure all tasks have completed
    for task in query_tasks {
        if let Err((bek_id, _)) = task.await.unwrap() {
            // If there was an error, retrieve the corresponding message ID and add it to failed_message_ids
            if let Some(&ref message_id) = bek_id_map.get(&bek_id) {
                batch_item_failures.push(BatchItemFailure {
                    item_identifier: message_id.clone(),
                });
            }
        }
    }

    Ok(SqsBatchResponse {
        batch_item_failures,
    })
}

async fn compute(id: u16) -> Result<(), (u16, Error)> {
    if let Err(err) = compute_pension_summary(id.into()).await {
        tracing::error!(err=%err, "Failed to compute pension summary data.");
        return Err((id, err));
    }

    if let Err(err) = transfer_to_s3(id.into()).await {
        tracing::error!(err=%err, "Failed to compute pension summary data.");
        return Err((id, err));
    }
    Ok(())
}

async fn compute_pension_summary(id: i32) -> Result<(), Error> {
    let bucket_name = "pensioncalcseast1";

    let s3 = AmazonS3Builder::from_env()
        .with_bucket_name(bucket_name)
        .build()?;
    let s3_path = format!("s3://{bucket_name}");
    let s3_url = Url::parse(&s3_path).unwrap();
    // create local session context
    let ctx = SessionContext::new();

    let file_name = format!("part_account_from_rds/{id}/pa_detail.parquet");
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
    writer.finish()?;

    Ok(())
}

async fn transfer_to_s3(id: i32) -> Result<(), Error> {
    let filename = format!("/tmp/result{}.json", id);
    let bucket_name = "pensioncalcseast1";
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let s3_client = aws_sdk_s3::Client::new(&config);
    let s3_key = format!("results/result{}.json", id);

    let body = ByteStream::from_path(Path::new(&filename)).await;

    s3_client
        .put_object()
        .bucket(bucket_name)
        .body(body.unwrap())
        .key(&s3_key)
        .send()
        .await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing::init_default_subscriber();

    run(service_fn(function_handler)).await
}
