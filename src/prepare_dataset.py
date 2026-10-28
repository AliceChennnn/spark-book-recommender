"""Prepare a new, explicitly configured CSV dataset. Never replace a published release."""

import argparse
import json
from pathlib import Path

try:
    from .dataset_adapter import IdMappings, adapt_frames
    from .datasets import DatasetSpec, require_artifact_binding
    from .output_safety import ensure_outputs_available, write_parquet
except ImportError:
    from dataset_adapter import IdMappings, adapt_frames
    from datasets import DatasetSpec, require_artifact_binding
    from output_safety import ensure_outputs_available, write_parquet


def prepare(spark, spec, previous_spec=None):
    # No configurable output-root or overwrite escape hatch into legacy Goodbooks.
    output = spec.prepared_root
    ensure_outputs_available(spark, [output])
    previous = None
    if previous_spec is not None:
        if (previous_spec.dataset_id, previous_spec.mapping_version) != (spec.dataset_id, spec.mapping_version):
            raise ValueError("Previous release must use the same dataset and mapping version")
        rows = spark.read.text(f"{previous_spec.prepared_root}/manifest").collect()
        if len(rows) != 1:
            raise ValueError("Expected one prepared-release manifest")
        require_artifact_binding(json.loads(rows[0].value), previous_spec)
        previous = IdMappings(spec.dataset_id, spec.mapping_version,
            spark.read.parquet(f"{previous_spec.prepared_root}/mappings/users"),
            spark.read.parquet(f"{previous_spec.prepared_root}/mappings/books"))
    def read_csv(name):
        return (spark.read.option("header", True).option("mode", "FAILFAST")
                .option("escape", '"').csv(f"{spec.raw_root}/{name}.csv"))
    adapted = adapt_frames(read_csv("ratings"), read_csv("books"), spec, previous)
    report = {"artifact_kind": "prepared", "binding": spec.data_binding,
              "adapter_config": spec.preparation_config, "summary": adapted.summary(),
              "id_mapping": "exact source strings; lexical initial IDs; append-only when reusing mappings",
              "source_root": spec.raw_root}
    for name, frame in (("processed/ratings", adapted.ratings), ("processed/books", adapted.books),
                        ("mappings/users", adapted.mappings.users), ("mappings/books", adapted.mappings.books)):
        write_parquet(frame, f"{output}/{name}")
    spark.createDataFrame([(json.dumps(report, ensure_ascii=False, sort_keys=True),)], ["value"]).coalesce(1) \
        .write.mode("errorifexists").text(f"{output}/manifest")
    return report


def load_spec(path):
    return DatasetSpec.from_dict(json.loads(Path(path).read_text(encoding="utf-8")))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--config", required=True, help="Explicit dataset schema/identity JSON")
    parser.add_argument("--previous-config", help="Prior release whose ID mappings must be extended")
    args = parser.parse_args(argv)
    spec = load_spec(args.config)
    previous = load_spec(args.previous_config) if args.previous_config else None
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName(f"BookRec-Prepare-{spec.dataset_id}").getOrCreate()
    try:
        spark.sparkContext.setLogLevel("WARN")
        print(json.dumps(prepare(spark, spec, previous), ensure_ascii=False, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
