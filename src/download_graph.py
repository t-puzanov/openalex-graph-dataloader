import argparse
import gzip
import json
import logging
import os
import sys
import time
from contextlib import closing
from pathlib import Path
import plyvel
import boto3
from botocore import UNSIGNED
from botocore.config import Config
from more_itertools import chunked
from tqdm import tqdm

import utils
from openalex import extract_numeric_id

logger = logging.getLogger("download_graph")

BUCKET = "openalex"
S3_PREFIX = "data/jsonl/works/updated_date="


def stream_data(s3, obj_key, desc=""):
    """Yield parsed JSON records from a gzipped JSONL object in S3."""
    response = s3.get_object(Bucket=BUCKET, Key=obj_key)
    total_size = response["ContentLength"]

    with closing(response["Body"]) as body:
        with tqdm(total=total_size, unit="B", unit_scale=True, desc=desc) as pbar:
            progress_stream = utils.ProgressReader(body, pbar)

            with gzip.open(progress_stream, mode="rb") as gz:
                for line_number, line in enumerate(gz, start=1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        yield json.loads(line.decode("utf-8"))
                    except json.JSONDecodeError:
                        logger.warning(
                            "Skipping malformed JSON line %d in %s",
                            line_number,
                            obj_key,
                        )


def transform_data(iterator):
    for row in iterator:
        key   = extract_numeric_id(row["id"]).encode("utf-8")
        value = utils.encode_work_data(row)
        yield key, value


def get_obj_key_list(s3):
    """Return sorted keys of all works partitions under the S3 prefix."""
    paginator = s3.get_paginator("list_objects_v2")

    obj_keys = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=S3_PREFIX):
        for obj in page.get("Contents", []):
            # Skip folder markers and non-gzip files.
            key = obj["Key"]
            if key.endswith(".gz"):
                obj_keys.append(key)

    obj_keys.sort(reverse=True)
    return obj_keys


def save_progress(progress_file, obj_key):
    """Atomically persist the last fully processed object key."""
    tmp_file = str(progress_file) + ".tmp"
    with open(tmp_file, "w", encoding="utf-8") as f:
        f.write(obj_key)
    os.replace(tmp_file, str(progress_file))


def get_last_processed(progress_file, obj_keys):
    """Return the index of the first object that still needs processing."""
    if not os.path.exists(progress_file):
        return 0

    with open(progress_file, "r", encoding="utf-8") as f:
        last_processed = f.read().strip()

    if not last_processed:
        return 0

    try:
        return obj_keys.index(last_processed) + 1
    except ValueError:
        logger.warning(
            "Progress key %r not found in current object list; starting over.",
            last_processed,
        )
        return 0


def process_file(s3, db, obj_key, batch_size, desc):
    """Download one object and upsert it in batches."""
    iterator = transform_data(stream_data(s3, obj_key, desc=desc))

    for batch in chunked(iterator, n=batch_size):
        with db.write_batch() as wb:
            for key, value in batch:
                wb.put(key, value)


def parse_args():
    script_dir = Path(__file__).resolve().parent
    default_workdir = (script_dir / ".." / "workdir").resolve()

    parser = argparse.ArgumentParser(
        description="Download OpenAlex works snapshots into a LevelDB database."
    )
    parser.add_argument(
        "--workdir",
        type=Path,
        default=default_workdir,
        help="Directory for the database and progress file (default: %(default)s)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100_000,
        help="Rows per transaction (default: %(default)s)",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Attempts per file before skipping it (default: %(default)s)",
    )
    parser.add_argument(
        "--retry-delay",
        type=float,
        default=15.0,
        help="Base delay between retries in seconds (default: %(default)s)",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )

    root_dir = args.workdir
    root_dir.mkdir(parents=True, exist_ok=True)

    progress_file = root_dir / "last_processed.txt"

    db = plyvel.DB(str(root_dir / "dataset"), create_if_missing=True)

    s3_config = Config(
        signature_version=UNSIGNED,
        retries={"max_attempts": 10, "mode": "standard"},
        connect_timeout=30,
        read_timeout=120,
        max_pool_connections=20,
    )
    s3 = boto3.client("s3", region_name="us-east-1", config=s3_config)

    obj_keys    = get_obj_key_list(s3)
    total       = len(obj_keys)
    start_index = get_last_processed(str(progress_file), obj_keys)
    skipped     = []

    try:
        for i in range(start_index, total):
            obj_key = obj_keys[i]
            desc = f"{i + 1}/{total}"

            for attempt in range(args.max_retries):
                try:
                    process_file(s3, db, obj_key, args.batch_size, desc)
                except KeyboardInterrupt:
                    logger.info("Interrupted by user. Shutting down...")
                    raise
                except Exception as e:
                    logger.error(
                        "Error processing %s (attempt %d/%d): %s",
                        obj_key,
                        attempt + 1,
                        args.max_retries,
                        e,
                    )
                    if attempt < args.max_retries - 1:
                        wait_time = min(args.retry_delay * (2 ** attempt), 300)
                        logger.info("Retrying in %.0f s", wait_time)
                        time.sleep(wait_time)
                    else:
                        # The file is skipped; record it so resume starts
                        # after it and the operator sees the full list.
                        skipped.append(obj_key)
                        logger.critical(
                            "Failed to process %s after %d attempts. Skipping.",
                            obj_key,
                            args.max_retries,
                        )
                else:
                    save_progress(progress_file, obj_key)
                    break
    finally:
        db.close()

    if skipped:
        logger.warning("%d file(s) were skipped:", len(skipped))
        for key in skipped:
            logger.warning("  %s", key)

    logger.info("Done.")


if __name__ == "__main__":
    main()
