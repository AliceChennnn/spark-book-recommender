"""Prepare a new, explicitly configured CSV dataset. Never replace a published release."""

import argparse
import json
from pathlib import Path

try:
    from .dataset_adapter import IdMappings, adapt_frames
    from .datasets import DatasetSpec, require_artifact_binding, require_mapping_mode
    from .output_safety import ensure_outputs_available, write_parquet
except ImportError:
    from dataset_adapter import IdMappings, adapt_frames
    from datasets import DatasetSpec, require_artifact_binding, require_mapping_mode
    from output_safety import ensure_outputs_available, write_parquet


def _check_mapping_history(spark, spec, previous_spec, mapping_mode):
    """Inspect only release metadata; never infer a fresh map from a new data_version."""
    if previous_spec is not None and (
            previous_spec.dataset_id, previous_spec.mapping_version) != (spec.dataset_id, spec.mapping_version):
        raise ValueError("Previous release must use the same dataset and mapping version")
    path = spark._jvm.org.apache.hadoop.fs.Path(f"{spec.root}/releases/*/{spec.mapping_version}")
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    releases = fs.globStatus(path)
    if mapping_mode == "create":
        if releases:
            raise ValueError("mapping_version already exists; use extend with --previous-config")
        return
    if not releases:
        raise ValueError("No published mapping version to extend")
    nodes = {}
    for status in releases:
        release = status.getPath()
        location = release.toString()
        if not status.isDirectory():
            raise ValueError(f"Invalid mapping release path: {location}")
        try:
            rows = spark.read.text(f"{location}/manifest").collect()
            if len(rows) != 1:
                raise ValueError("Expected one prepared-release manifest")
            manifest = json.loads(rows[0].value)
        except Exception as exc:
            raise ValueError(f"Incomplete or unreadable mapping release: {location}") from exc
        if not isinstance(manifest, dict):
            raise ValueError(f"Invalid mapping release manifest: {location}")
        binding = manifest.get("binding")
        if (manifest.get("artifact_kind") != "prepared" or not isinstance(binding, dict)
                or set(binding) != set(spec.data_binding)
                or binding["dataset_id"] != spec.dataset_id
                or binding["mapping_version"] != spec.mapping_version
                or binding["data_version"] != release.getParent().getName()):
            raise ValueError(f"Invalid mapping release binding: {location}")
        key = json.dumps(binding, sort_keys=True)
        if key in nodes:
            raise ValueError("Duplicate mapping release binding")
        parent = manifest.get("mapping_parent")  # A sole pre-fix release is an initial root.
        if parent is not None and not isinstance(parent, dict):
            raise ValueError("Invalid mapping parent binding")
        nodes[key] = (manifest, json.dumps(parent, sort_keys=True) if parent is not None else None)
    parents = {parent for _, parent in nodes.values() if parent is not None}
    heads = set(nodes) - parents
    roots = [key for key, (_, parent) in nodes.items() if parent is None]
    if not parents.issubset(nodes) or len(heads) != 1 or len(roots) != 1:
        raise ValueError("Ambiguous or broken mapping lineage; refusing to renumber existing IDs")
    head = next(iter(heads))
    visited = set()
    current = head
    while current is not None and current not in visited:
        visited.add(current)
        current = nodes[current][1]
    if current is not None or len(visited) != len(nodes):
        raise ValueError("Cyclic or disconnected mapping lineage")
    # A stale predecessor can omit IDs added by a later release and reuse their numbers.
    if json.dumps(previous_spec.data_binding, sort_keys=True) != head:
        raise ValueError("--previous-config must reference the latest mapping release")
    require_artifact_binding(nodes[head][0], previous_spec)


def prepare(spark, spec, previous_spec=None, *, mapping_mode=None):
    require_mapping_mode(mapping_mode, previous_spec)
    # No configurable output-root or overwrite escape hatch into legacy Goodbooks.
    output = spec.prepared_root
    ensure_outputs_available(spark, [output])
    _check_mapping_history(spark, spec, previous_spec, mapping_mode)
    previous = None
    if previous_spec is not None:
        previous = IdMappings(spec.dataset_id, spec.mapping_version,
            spark.read.parquet(f"{previous_spec.prepared_root}/mappings/users"),
            spark.read.parquet(f"{previous_spec.prepared_root}/mappings/books"))
    def read_csv(name):
        return (spark.read.option("header", True).option("mode", "FAILFAST")
                .option("escape", '"').csv(f"{spec.raw_root}/{name}.csv"))
    adapted = adapt_frames(read_csv("ratings"), read_csv("books"), spec, previous, mapping_mode=mapping_mode)
    report = {"artifact_kind": "prepared", "binding": spec.data_binding,
              "adapter_config": spec.preparation_config, "summary": adapted.summary(),
              "id_mapping": "exact source strings; lexical initial IDs; append-only when reusing mappings",
              "mapping_mode": mapping_mode,
              "mapping_parent": previous_spec.data_binding if previous_spec is not None else None,
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
    parser.add_argument("--mapping-mode", required=True, choices=("create", "extend"),
                        help="Create a previously unused mapping version, or extend its latest release")
    parser.add_argument("--previous-config", help="Prior release whose ID mappings must be extended")
    args = parser.parse_args(argv)
    try:
        require_mapping_mode(args.mapping_mode, args.previous_config)
    except ValueError as exc:
        parser.error(str(exc))
    spec = load_spec(args.config)
    previous = load_spec(args.previous_config) if args.previous_config else None
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName(f"BookRec-Prepare-{spec.dataset_id}").getOrCreate()
    try:
        spark.sparkContext.setLogLevel("WARN")
        print(json.dumps(prepare(spark, spec, previous, mapping_mode=args.mapping_mode),
                         ensure_ascii=False, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
