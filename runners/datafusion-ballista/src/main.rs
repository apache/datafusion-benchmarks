use ballista::prelude::*;
use datafusion::prelude::ParquetReadOptions;
use serde::Serialize;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use structopt::StructOpt;
use tokio::time::Instant;

#[derive(StructOpt, Debug)]
#[structopt(name = "basic")]
struct Opt {
    /// Activate debug mode
    #[structopt(long)]
    debug: bool,

    /// Optional path to config file
    #[structopt(short, long, parse(from_os_str))]
    config_path: Option<PathBuf>,

    /// Path to queries
    #[structopt(long, parse(from_os_str))]
    query_path: PathBuf,

    /// Path to data
    #[structopt(short, long, parse(from_os_str))]
    data_path: PathBuf,

    /// Output path
    #[structopt(short, long, parse(from_os_str))]
    output: PathBuf,

    /// Query number. If no query number specified then all queries will be executed.
    #[structopt(short, long)]
    query: Option<u8>,

    /// Number of queries in this benchmark suite
    #[structopt(short, long)]
    num_queries: Option<u8>,

    /// List of queries to exclude
    #[structopt(short, long)]
    exclude: Vec<u8>,

    /// Concurrency
    #[structopt(short, long)]
    concurrency: u8,

    /// Iterations (number of times to run each query)
    #[structopt(short, long)]
    iterations: u8,
}

#[derive(Debug, PartialEq, Serialize, Default)]
pub struct Results {
    system_time: u128,
    config: HashMap<String, String>,
    command_line_args: Vec<String>,
    register_tables_time: u128,
    /// Vector of (query_number, query_times)
    query_times: Vec<(u8, Vec<u128>)>,
}

impl Results {
    fn new() -> Self {
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        Self {
            system_time: current_time.as_millis(),
            config: HashMap::new(),
            command_line_args: vec![],
            register_tables_time: 0,
            query_times: vec![],
        }
    }
}

#[tokio::main]
pub async fn main() -> Result<()> {
    let mut results = Results::new();
    for arg in std::env::args() {
        results.command_line_args.push(arg);
    }

    let opt = Opt::from_args();
    let query_path = format!("{}", opt.query_path.display());
    let output_path = format!("{}", opt.output.display());

    let mut config = BallistaConfig::builder().set(
        BALLISTA_DEFAULT_SHUFFLE_PARTITIONS,
        &format!("{}", opt.concurrency),
    );

    if let Some(config_path) = &opt.config_path {
        let file = File::open(config_path)?;
        let reader = BufReader::new(file);
        let lines = reader.lines();
        for line in lines {
            let line = line?;
            if line.starts_with("#") {
                continue;
            }
            let parts = line.split('=');
            let parts = parts.collect::<Vec<&str>>();
            if parts.len() == 2 {
                config = config.set(parts[0], parts[1]);
            } else {
                println!("Warning! Skipping config entry {}", line);
            }
        }
    }

    let config = config.build()?;

    for (key, value) in config.settings() {
        results.config.insert(key.to_string(), value.to_string());
    }

    // register all tables in data directory
    let start = Instant::now();
    let ctx = BallistaContext::remote("localhost", 50050, &config).await?;
    for file in fs::read_dir(&opt.data_path)? {
        let file = file?;
        let file_path = file.path();
        let path = format!("{}", file.path().display());
        if path.ends_with(".parquet") {
            let filename = Path::file_name(&file_path).unwrap().to_str().unwrap();
            let table_name = &filename[0..filename.len() - 8];
            println!("Registering table {} as {}", table_name, path);
            ctx.register_parquet(&table_name, &path, ParquetReadOptions::default())
                .await?;
        }
    }

    let setup_time = start.elapsed().as_millis();
    println!("Setup time was {} ms", setup_time);
    results.register_tables_time = setup_time;

    match opt.query {
        Some(query) => {
            execute_query(
                &ctx,
                &query_path,
                query,
                opt.debug,
                &output_path,
                opt.iterations,
                &mut results,
            )
            .await?;
        }
        _ => {
            let num_queries = opt.num_queries.unwrap();
            for query in 1..=num_queries {
                if opt.exclude.contains(&query) {
                    println!("Skipping query {}", query);
                    continue;
                }

                let result = execute_query(
                    &ctx,
                    &query_path,
                    query,
                    opt.debug,
                    &output_path,
                    opt.iterations,
                    &mut results,
                )
                .await;
                match result {
                    Ok(_) => {}
                    Err(e) => println!("Fail: {}", e),
                }
            }
        }
    }

    // write results json file
    let json = serde_json::to_string_pretty(&results).unwrap();
    let f = File::create(&format!(
        "{}/results-{}.json",
        output_path, results.system_time
    ))?;
    let mut w = BufWriter::new(f);
    w.write(json.as_bytes())?;

    // write simple csv summary file
    let mut w = File::create(&format!("{}/results.csv", output_path))?;
    w.write(format!("setup,{}\n", results.register_tables_time).as_bytes())?;
    for (query, times) in &results.query_times {
        w.write(format!("q{},{}\n", query, times[0]).as_bytes())?;
    }

    Ok(())
}

pub async fn execute_query(
    ctx: &BallistaContext,
    query_path: &str,
    query_no: u8,
    debug: bool,
    output_path: &str,
    iterations: u8,
    results: &mut Results,
) -> Result<()> {
    let filename = format!("{}/q{query_no}.sql", query_path);
    println!("Executing query {} from {}", query_no, filename);
    let sql = fs::read_to_string(&filename)?;

    // some queries have multiple statements
    let sql = sql
        .split(';')
        .filter(|s| !s.trim().is_empty())
        .collect::<Vec<_>>();

    let multipart = sql.len() > 1;

    let mut durations = vec![];
    for iteration in 0..iterations {
        // duration for executing all queries in the file
        let mut total_duration_millis = 0;

        for (i, sql) in sql.iter().enumerate() {
            if debug {
                println!("Query {}: {}", query_no, sql);
            }

            let file_suffix = if multipart {
                format!("_part_{}", i + 1)
            } else {
                "".to_owned()
            };

            let start = Instant::now();
            let df = ctx.sql(sql).await?;
            let _batches = df.clone().collect().await?;
            let duration = start.elapsed();
            total_duration_millis += duration.as_millis();
            println!(
                "Query {}{} executed in: {:?}",
                query_no, file_suffix, duration
            );

            if iteration == 0 {
                let plan = df.into_optimized_plan()?;
                let formatted_query_plan = format!("{}", plan.display_indent());
                let filename = format!(
                    "{}/q{}{}_logical_plan.txt",
                    output_path, query_no, file_suffix
                );
                let mut file = File::create(&filename)?;
                write!(file, "{}", formatted_query_plan)?;

                // write results to disk

                // TODO broken due to https://github.com/apache/arrow-ballista/issues/894
                // if batches.is_empty() {
                //     println!("Empty result set returned");
                // } else {
                //     let filename = format!("{}/q{}{}.csv", output_path, query_no, file_suffix);
                //     let t: Arc<dyn TableProvider> =
                //         Arc::new(MemTable::try_new(batches[0].schema(), vec![batches])?);
                //     let table_name = format!("q{query_no}");
                //     ctx.register_table(&table_name, t)?;
                //     let df = ctx.sql(&format!("SELECT * FROM {table_name}")).await?;
                //     df.write_csv(&filename, DataFrameWriteOptions::default(), None)
                //         .await?;
                // }
            }
        }
        durations.push(total_duration_millis);
    }
    results.query_times.push((query_no, durations));
    Ok(())
}
