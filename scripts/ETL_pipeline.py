import time
import logging
from uuid import UUID
import findspark
findspark.init()
import time_uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType


# ================================================================
# CONFIGURATION
# ================================================================

CASSANDRA_TABLE    = "tracking"
CASSANDRA_KEYSPACE = "logs"

MYSQL_HOST     = "localhost"
MYSQL_PORT     = "3306"
MYSQL_DATABASE = "logs"
MYSQL_USER     = "root"
MYSQL_PASSWORD = "1"
MYSQL_DRIVER   = "com.mysql.cj.jdbc.Driver"
MYSQL_URL      = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"

MYSQL_EVENTS_TABLE = "events"
LOOP_INTERVAL      = 5  # seconds
LOG_FILE           = "pipeline.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
log = logging.getLogger("ETL")

spark = (
    SparkSession.builder
    .appName("ETL_Cassandra_MySQL")
    .config(
        "spark.jars.packages",
        "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0",
    )
    .getOrCreate()
)

def calculating_clicks(df):
    click_df = (
        df.filter(col("custom_track") == "click")
        .withColumn("bid", col("bid").cast("double"))
        .groupBy(
            to_date(col("ts")).alias("date"),
            hour(col("ts")).alias("hour"),
            col("job_id"),
            col("campaign_id"),
            col("group_id"),
            col("publisher_id"),
        )
        .agg(
            round(avg("bid"), 2).alias("avg_bid"),
            round(sum("bid"), 2).alias("spent_hour"),
            count("*").alias("clicks"),
        )
    )
    return click_df


def calculating_conversion(df):
    conversion_df = (
        df.filter(col("custom_track") == "conversion")
        .groupBy(
            to_date(col("ts")).alias("date"),
            hour(col("ts")).alias("hour"),
            col("job_id"),
            col("campaign_id"),
            col("group_id"),
            col("publisher_id"),
        )
        .agg(count("*").alias("conversion"))
    )
    return conversion_df


def calculating_qualified(df):
    qualified_df = (
        df.filter(col("custom_track") == "qualified")
        .groupBy(
            to_date(col("ts")).alias("date"),
            hour(col("ts")).alias("hour"),
            col("job_id"),
            col("campaign_id"),
            col("group_id"),
            col("publisher_id"),
        )
        .agg(count("*").alias("qualified_application"))
    )
    return qualified_df


def calculating_unqualified(df):
    unqualified_df = (
        df.filter(col("custom_track") == "unqualified")
        .groupBy(
            to_date(col("ts")).alias("date"),
            hour(col("ts")).alias("hour"),
            col("job_id"),
            col("campaign_id"),
            col("group_id"),
            col("publisher_id"),
        )
        .agg(count("*").alias("disqualified_application"))
    )
    return unqualified_df


# ================================================================
# STEP 2 — PROCESS CASSANDRA DATA (TimeUUID → timestamp)
# ================================================================

def process_cassandra_data(df):
    @udf(returnType=StringType())
    def timeuuid_to_str(uuid_str):
        return (
            time_uuid.TimeUUID(bytes=UUID(uuid_str).bytes)
            .get_datetime()
            .strftime("%Y-%m-%d %H:%M:%S")
        )
    df_with_time = df.withColumn(
        "normal_time", timeuuid_to_str(col("create_time"))
    )
    time_rows = df_with_time.select("create_time", "normal_time").collect()
    if not time_rows:
        log.warning("No rows after TimeUUID conversion.")
        return None
    uuid_list      = [row.create_time for row in time_rows]
    timestamp_list = [row.normal_time  for row in time_rows]
    time_lookup_df = spark.createDataFrame(
        zip(uuid_list, timestamp_list),
        schema=["create_time", "ts"],
    )
    processed_df = (
        df.join(time_lookup_df, on="create_time", how="inner")
        .drop(df.ts)
        .select(
            "create_time",
            "ts",
            "job_id",
            "custom_track",
            "bid",
            "campaign_id",
            "group_id",
            "publisher_id",
        )
    )
    return processed_df


# ================================================================
# STEP 3 — RETRIEVE COMPANY / JOB DATA FROM MYSQL
# ================================================================

def retrieve_company_data():
    sql = "(SELECT id AS job_id, company_id, group_id, campaign_id FROM job) A"
    job_df = (
        spark.read.format("jdbc")
        .options(
            url=MYSQL_URL,
            driver=MYSQL_DRIVER,
            dbtable=sql,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
        )
        .load()
    )
    return job_df


# ================================================================
# STEP 4 — JOIN & BUILD FINAL DATAFRAME
# ================================================================

def process_final_data(click_df, conversion_df, qualified_df, unqualified_df, job_df, latest_ts):
    JOIN_KEYS = [
        "date", "hour",
        "job_id", "campaign_id", "group_id", "publisher_id",
    ]
    combined_df = (
        click_df
        .join(qualified_df,   JOIN_KEYS, "full")
        .join(unqualified_df, JOIN_KEYS, "full")
        .join(conversion_df,  JOIN_KEYS, "full")
    )
    enriched_df = (
        combined_df
        .join(job_df, on="job_id", how="left")
        .drop(job_df.group_id)
        .drop(job_df.campaign_id)
    )
    final_df = (
        enriched_df
        .withColumnRenamed("date",       "dates")
        .withColumnRenamed("hour",       "hours")
        .withColumnRenamed("avg_bid",    "bid_set")
        .withColumnRenamed("clicks",     "clicks")
        .withColumnRenamed("spent_hour", "spend_hour")
        .withColumn("updated_at", lit(latest_ts))
        .withColumn("sources",    lit("Cassandra"))
    )
    return final_df


# ================================================================
# STEP 5 — WRITE TO MYSQL
# ================================================================

def import_to_mysql(final_df):
    int_cols = ["group_id", "job_id", "campaign_id", "publisher_id", "company_id"]
    for c in int_cols:
        final_df = final_df.withColumn(
            c,
            when(col(c).cast("string") == "", None)
            .otherwise(col(c).cast("int"))
        )
    output_columns = [
        "job_id",
        "dates",
        "hours",
        "disqualified_application",
        "qualified_application",
        "conversion",
        "company_id",
        "group_id",
        "campaign_id",
        "publisher_id",
        "bid_set",
        "clicks",
        "spend_hour",
        "sources",
        "updated_at",
    ]
    (
        final_df.select(*output_columns)
        .write.format("jdbc")
        .options(
            url=MYSQL_URL,
            driver=MYSQL_DRIVER,
            dbtable=MYSQL_EVENTS_TABLE,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
        )
        .mode("append")
        .save()
    )

    log.info("Write to MySQL completed successfully.")


# ================================================================
# STEP 6 — CHECKPOINT HELPERS
# ================================================================
def get_latest_time_cassandra():
    df = (
        spark.read
        .format("org.apache.spark.sql.cassandra")
        .options(
            table=CASSANDRA_TABLE,
            keyspace=CASSANDRA_KEYSPACE,
        )
        .load()
    )
    latest = df.agg({"ts": "max"}).take(1)[0][0]
    return latest, df  # return df to reuse in main_task


def get_mysql_latest_time():
    sql = "(SELECT MAX(updated_at) AS ts FROM events) A"
    result_df = (
        spark.read.format("jdbc")
        .options(
            url=MYSQL_URL,
            driver=MYSQL_DRIVER,
            dbtable=sql,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
        )
        .load()
    )
    row = result_df.take(1)[0][0]
    latest = row if row is not None else "1980-01-01 00:00:00"
    return latest


# ================================================================
# MAIN TASK  (orchestration only)
# ================================================================

def main_task(cassandra_df, latest_ts):
    t0 = time.perf_counter()
    log.info("=" * 50)
    # ── 1. Filter only new records ─────────────────────────────
    log.info(f"Filtering records after checkpoint: {latest_ts}")
    new_df = cassandra_df.filter(col("ts") > lit(latest_ts))
    new_count = new_df.count()
    log.info(f"New records to process: {new_count}")

    if new_count == 0:
        log.info("No new data. Skipping this cycle.")
        return

    # ── 2. Process Cassandra data (TimeUUID → ts) ──────────────
    processed_df = process_cassandra_data(new_df)
    if processed_df is None:
        log.warning("Processing returned empty result. Skipping.")
        return

        # ── 3. Aggregate event types ───────────────────────────────
    click_df       = calculating_clicks(processed_df)
    conversion_df  = calculating_conversion(processed_df)
    qualified_df   = calculating_qualified(processed_df)
    unqualified_df = calculating_unqualified(processed_df)

    # ── 4. Retrieve job dimension from MySQL ───────────────────
    job_df = retrieve_company_data()

    # ── 5. Join everything into final DataFrame ────────────────
    batch_latest_ts = processed_df.agg({"ts": "max"}).take(1)[0][0]
    final_df = process_final_data(
            click_df,
            conversion_df,
            qualified_df,
            unqualified_df,
            job_df,
            latest_ts=batch_latest_ts,
    )

    # ── 6. Write to MySQL ──────────────────────────────────────
    import_to_mysql(final_df)
    elapsed = time.perf_counter() - t0
    log.info(f"ETL cycle completed in {elapsed:.2f}s | Checkpoint → {batch_latest_ts}")

# ================================================================
# NEAR REAL-TIME LOOP
# ================================================================

if __name__ == "__main__":
    log.info("=" * 50)
    log.info("Starting near real-time ETL pipeline")
    log.info(f"Log file  : {LOG_FILE}")
    log.info(f"Loop interval: {LOOP_INTERVAL}s")
    log.info("=" * 50)

    while True:
        cassandra_latest_ts, cassandra_df = get_latest_time_cassandra()
        mysql_latest_ts = get_mysql_latest_time()

        log.info(f"Cassandra latest : {cassandra_latest_ts}")
        log.info(f"MySQL latest     : {mysql_latest_ts}")

        if cassandra_latest_ts is None:
            log.warning("Cassandra is empty. Sleeping...")
        elif str(cassandra_latest_ts) > str(mysql_latest_ts):
            log.info("New data detected. Running main_task...")
            main_task(cassandra_df, mysql_latest_ts)
        else:
            log.info("No new data. Skipping cycle.")

        log.info(f"Sleeping {LOOP_INTERVAL}s...")
        time.sleep(LOOP_INTERVAL)
