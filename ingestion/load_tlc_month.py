"""
NYC TLC yellow-taxi ingestion: TLC CDN (monthly parquet) -> GCS landing -> BigQuery `raw`.

Why this is not just `bq load`
------------------------------
* Schema drift: TLC files change across years (e.g. `airport_fee` vs `Airport_fee`,
  passenger_count INT vs DOUBLE, `cbd_congestion_fee` added in 2025). Every file is
  normalised to one canonical schema with pyarrow before loading, so the raw table
  never breaks and new columns are explicit.
* Idempotency: each month is written to its own partition with a partition decorator
  (`raw.yellow_trips$YYYYMM`) and WRITE_TRUNCATE, so reloading a month replaces it
  instead of duplicating ~3M rows.
* Lineage: every row carries source_month, _source_file and _ingested_at; the dbt
  cleaned layer uses _ingested_at to reprocess only months that were (re)loaded.

Usage
-----
    python ingestion/load_tlc_month.py --project P --bucket B --months 2024-01 2024-02
    python ingestion/load_tlc_month.py --project P --bucket B --start 2023-01 --end 2024-12
    # no download bandwidth? copy a year from the BigQuery public dataset instead:
    python ingestion/load_tlc_month.py --project P --from-public-dataset --start 2022-01 --end 2022-12
"""
from __future__ import annotations

import argparse
import logging
import sys
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests
from google.cloud import bigquery, storage

log = logging.getLogger("tlc_ingest")

TLC_BASE = "https://d37ci6vzurychx.cloudfront.net"
TRIP_URL = TLC_BASE + "/trip-data/yellow_tripdata_{ym}.parquet"
ZONES_URL = TLC_BASE + "/misc/taxi_zone_lookup.csv"

# canonical name -> (arrow type, BigQuery type, known source spellings)
CANONICAL: dict[str, tuple[pa.DataType, str, tuple[str, ...]]] = {
    "vendor_id":             (pa.int64(),       "INT64",    ("vendorid",)),
    "pickup_datetime":       (pa.timestamp("us"), "DATETIME", ("tpep_pickup_datetime",)),
    "dropoff_datetime":      (pa.timestamp("us"), "DATETIME", ("tpep_dropoff_datetime",)),
    "passenger_count":       (pa.int64(),       "INT64",    ("passenger_count",)),
    "trip_distance":         (pa.float64(),     "FLOAT64",  ("trip_distance",)),
    "ratecode_id":           (pa.int64(),       "INT64",    ("ratecodeid",)),
    "store_and_fwd_flag":    (pa.string(),      "STRING",   ("store_and_fwd_flag",)),
    "pu_location_id":        (pa.int64(),       "INT64",    ("pulocationid",)),
    "do_location_id":        (pa.int64(),       "INT64",    ("dolocationid",)),
    "payment_type":          (pa.int64(),       "INT64",    ("payment_type",)),
    "fare_amount":           (pa.float64(),     "FLOAT64",  ("fare_amount",)),
    "extra":                 (pa.float64(),     "FLOAT64",  ("extra",)),
    "mta_tax":               (pa.float64(),     "FLOAT64",  ("mta_tax",)),
    "tip_amount":            (pa.float64(),     "FLOAT64",  ("tip_amount",)),
    "tolls_amount":          (pa.float64(),     "FLOAT64",  ("tolls_amount",)),
    "improvement_surcharge": (pa.float64(),     "FLOAT64",  ("improvement_surcharge",)),
    "total_amount":          (pa.float64(),     "FLOAT64",  ("total_amount",)),
    "congestion_surcharge":  (pa.float64(),     "FLOAT64",  ("congestion_surcharge",)),
    "airport_fee":           (pa.float64(),     "FLOAT64",  ("airport_fee",)),
    "cbd_congestion_fee":    (pa.float64(),     "FLOAT64",  ("cbd_congestion_fee",)),
}

RAW_SCHEMA = [bigquery.SchemaField(name, bq_type) for name, (_, bq_type, _) in CANONICAL.items()] + [
    # NULLABLE on purpose: parquet/query outputs are nullable and BigQuery rejects nullable -> REQUIRED loads
    bigquery.SchemaField("source_month", "DATE"),
    bigquery.SchemaField("_source_file", "STRING"),
    bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
]


# ---------------------------------------------------------------- helpers
def month_range(start: str, end: str) -> list[str]:
    s = datetime.strptime(start, "%Y-%m").date()
    e = datetime.strptime(end, "%Y-%m").date()
    out, cur = [], s
    while cur <= e:
        out.append(cur.strftime("%Y-%m"))
        cur = date(cur.year + (cur.month // 12), cur.month % 12 + 1, 1)
    return out


def download(url: str, dest: Path) -> Path:
    log.info("Downloading %s", url)
    with requests.get(url, stream=True, timeout=300) as resp:
        resp.raise_for_status()
        with dest.open("wb") as fh:
            for chunk in resp.iter_content(chunk_size=8 << 20):
                fh.write(chunk)
    log.info("Downloaded %.1f MB", dest.stat().st_size / 1e6)
    return dest


def normalise(src: Path, dest: Path, ym: str, source_uri: str) -> int:
    """Rename + cast every column to the canonical schema; add missing ones as NULL."""
    table = pq.read_table(src)
    lower = {c.lower(): c for c in table.column_names}
    unknown = set(lower) - {alias for _, _, aliases in CANONICAL.values() for alias in aliases}
    if unknown:
        log.warning("New/unmapped source columns in %s (kept out of raw, review!): %s", ym, sorted(unknown))

    n = table.num_rows
    arrays, names = [], []
    for name, (arrow_type, _, aliases) in CANONICAL.items():
        src_col = next((lower[a] for a in aliases if a in lower), None)
        if src_col is None:
            arrays.append(pa.nulls(n, type=arrow_type))
        else:
            col = table[src_col]
            if pa.types.is_timestamp(col.type) and col.type.tz is not None:
                col = pc.local_timestamp(col)          # tz-aware -> NYC wall-clock, like every other year
            arrays.append(pc.cast(col, arrow_type, safe=False))
        names.append(name)

    month_start = datetime.strptime(ym, "%Y-%m").date()
    arrays += [
        pa.array([month_start] * n, type=pa.date32()),
        pa.array([source_uri] * n, type=pa.string()),
        pa.array([datetime.now(timezone.utc)] * n, type=pa.timestamp("us", tz="UTC")),
    ]
    names += ["source_month", "_source_file", "_ingested_at"]
    pq.write_table(pa.Table.from_arrays(arrays, names=names), dest, compression="snappy")
    return n


def ensure_raw_table(bq: bigquery.Client, table_id: str) -> None:
    table = bigquery.Table(table_id, schema=RAW_SCHEMA)
    table.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.MONTH, field="source_month")
    table.clustering_fields = ["pu_location_id"]
    table.labels = {"layer": "raw", "source": "nyc_tlc"}
    table.description = "Yellow taxi trips as published by NYC TLC, normalised to one schema. One partition per source file month."
    bq.create_table(table, exists_ok=True)


# ---------------------------------------------------------------- loaders
def load_month_from_tlc(bq, gcs, project, dataset, bucket, ym, workdir: Path) -> int:
    raw_file = download(TRIP_URL.format(ym=ym), workdir / f"yellow_{ym}.parquet")
    blob = f"nyc_tlc/landing/yellow/source_month={ym}/yellow_tripdata_{ym}.parquet"
    landing_uri = f"gs://{bucket}/{blob}"
    gcs.bucket(bucket).blob(blob).upload_from_filename(str(raw_file))       # keep the untouched original

    norm_file = workdir / f"yellow_{ym}_normalised.parquet"
    rows = normalise(raw_file, norm_file, ym, landing_uri)
    norm_blob = blob.replace("/landing/", "/normalised/")
    gcs.bucket(bucket).blob(norm_blob).upload_from_filename(str(norm_file))

    target = f"{project}.{dataset}.yellow_trips${ym.replace('-', '')}"
    job = bq.load_table_from_uri(
        f"gs://{bucket}/{norm_blob}",
        target,
        job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            schema=RAW_SCHEMA,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,   # replaces only this month's partition
        ),
    )
    job.result()
    log.info("%s: %s rows -> %s", ym, f"{rows:,}", target)
    for f in (raw_file, norm_file):
        f.unlink(missing_ok=True)
    return rows


def public_select_sql(year: str, month: str) -> str:
    return f"""
    select
      safe_cast(vendor_id as int64)              as vendor_id,
      datetime(pickup_datetime)                  as pickup_datetime,
      datetime(dropoff_datetime)                 as dropoff_datetime,
      safe_cast(passenger_count as int64)        as passenger_count,
      cast(trip_distance as float64)             as trip_distance,
      safe_cast(rate_code as int64)              as ratecode_id,
      store_and_fwd_flag,
      safe_cast(pickup_location_id as int64)     as pu_location_id,
      safe_cast(dropoff_location_id as int64)    as do_location_id,
      safe_cast(payment_type as int64)           as payment_type,
      cast(fare_amount as float64)               as fare_amount,
      cast(extra as float64)                     as extra,
      cast(mta_tax as float64)                   as mta_tax,
      cast(tip_amount as float64)                as tip_amount,
      cast(tolls_amount as float64)              as tolls_amount,
      cast(imp_surcharge as float64)             as improvement_surcharge,
      cast(total_amount as float64)              as total_amount,
      cast(null as float64)                      as congestion_surcharge,
      cast(airport_fee as float64)               as airport_fee,
      cast(null as float64)                      as cbd_congestion_fee,
      date({year}, {int(month)}, 1)              as source_month,
      'bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_{year}' as _source_file,
      current_timestamp()                        as _ingested_at
    from `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_{year}`
    where data_file_month = {int(month)}
    """


def load_month_from_public(bq, project, dataset, ym) -> int:
    """Fallback: same canonical schema, sourced from bigquery-public-data (2009-2022 available)."""
    year, month = ym.split("-")
    target = f"{project}.{dataset}.yellow_trips${year}{month}"
    sql = public_select_sql(year, month)
    job = bq.query(sql, job_config=bigquery.QueryJobConfig(
        destination=target,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    ))
    job.result()
    rows = next(iter(bq.query(
        f"select count(*) as n from `{project}.{dataset}.yellow_trips` where source_month = date({year}, {int(month)}, 1)"
    ).result())).n
    log.info("%s: copied from public dataset -> %s (%.2f GB processed)", ym, target, (job.total_bytes_processed or 0) / 1e9)
    return rows


def load_all_from_public_sandbox(bq, project, dataset, months: list[str]) -> int:
    """BigQuery Sandbox expires every date partition older than 60 days, so historical
    month partitions vanish as soon as they are written. In sandbox mode raw is rebuilt as
    one unpartitioned, clustered table with CREATE OR REPLACE (DDL only, no DML)."""
    target = f"{project}.{dataset}.yellow_trips"
    bq.delete_table(target, not_found_ok=True)      # partitioning spec cannot change in place
    union = "\n    union all\n".join(public_select_sql(*ym.split("-")) for ym in months)
    job = bq.query(f"""
    create table `{target}`
    cluster by source_month, pu_location_id
    options (description = "Yellow taxi trips (sandbox mode: unpartitioned)")
    as
    {union}
    """)
    job.result()
    rows = bq.get_table(target).num_rows
    log.info("Sandbox load: %s rows for %s -> %s (%.2f GB processed)",
             f"{rows:,}", ", ".join(months), target, (job.total_bytes_processed or 0) / 1e9)
    return rows


def load_zones(bq, gcs, project, dataset, bucket, workdir: Path) -> None:
    path = download(ZONES_URL, workdir / "taxi_zone_lookup.csv")
    uri = None
    if bucket:
        blob = "nyc_tlc/landing/reference/taxi_zone_lookup.csv"
        gcs.bucket(bucket).blob(blob).upload_from_filename(str(path))
        uri = f"gs://{bucket}/{blob}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        schema=[bigquery.SchemaField(c, t) for c, t in
                [("location_id", "INT64"), ("borough", "STRING"), ("zone", "STRING"), ("service_zone", "STRING")]],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    target = f"{project}.{dataset}.taxi_zones"
    if uri:
        bq.load_table_from_uri(uri, target, job_config=job_config).result()
    else:
        with path.open("rb") as fh:
            bq.load_table_from_file(fh, target, job_config=job_config).result()
    log.info("Loaded taxi zone lookup -> %s", target)


# ---------------------------------------------------------------- main
def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--project", required=True)
    p.add_argument("--bucket", help="GCS landing bucket (required unless --from-public-dataset)")
    p.add_argument("--dataset", default="raw")
    p.add_argument("--location", default="US")
    p.add_argument("--months", nargs="*", help="YYYY-MM list")
    p.add_argument("--start", help="YYYY-MM (inclusive)")
    p.add_argument("--end", help="YYYY-MM (inclusive)")
    p.add_argument("--from-public-dataset", action="store_true")
    p.add_argument("--skip-zones", action="store_true")
    p.add_argument("--sandbox", action="store_true",
                   help="BigQuery Sandbox (no billing): unpartitioned raw, needs --from-public-dataset")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    months = args.months or (month_range(args.start, args.end) if args.start and args.end else None)
    if not months:
        p.error("give --months or --start/--end")
    if args.sandbox and not args.from_public_dataset:
        p.error("--sandbox works with --from-public-dataset only")
    if not args.from_public_dataset and not args.bucket:
        p.error("--bucket is required when downloading from TLC")

    bq = bigquery.Client(project=args.project, location=args.location)
    gcs = storage.Client(project=args.project) if args.bucket else None
    bq.create_dataset(bigquery.Dataset(f"{args.project}.{args.dataset}"), exists_ok=True)
    if not args.sandbox:
        ensure_raw_table(bq, f"{args.project}.{args.dataset}.yellow_trips")

    total, failed = 0, []
    with tempfile.TemporaryDirectory() as tmp:
        workdir = Path(tmp)
        if not args.skip_zones:
            load_zones(bq, gcs, args.project, args.dataset, args.bucket, workdir)
        if args.sandbox:
            total = load_all_from_public_sandbox(bq, args.project, args.dataset, months)
            months = []
        for ym in months:
            try:
                if args.from_public_dataset:
                    total += load_month_from_public(bq, args.project, args.dataset, ym)
                else:
                    total += load_month_from_tlc(bq, gcs, args.project, args.dataset, args.bucket, ym, workdir)
            except Exception:                       # keep going; report at the end so the DAG task fails loudly
                log.exception("Month %s failed", ym)
                failed.append(ym)

    log.info("Loaded %s rows in total; failed months: %s", f"{total:,}", failed or "none")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
