"""Explicit overwrite policy for the preparation and ALS training entry points."""


def add_overwrite_argument(parser):
    parser.add_argument(
        "--overwrite", action="store_true",
        help="Explicitly allow replacement of this script's existing outputs (default: refuse)",
    )


def ensure_outputs_available(spark, paths, overwrite=False):
    """Check every destination before doing work; filesystem errors fail closed."""
    existing = []
    for destination in paths:
        path = spark._jvm.org.apache.hadoop.fs.Path(destination)
        fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
        if fs.exists(path):
            existing.append(destination)
    if existing and not overwrite:
        raise FileExistsError(
            "Refusing to overwrite existing output(s): " + ", ".join(existing)
            + ". Use --overwrite only if replacement is intended."
        )


def save_model(model, path, overwrite=False):
    """Keep Spark's native refusal at save time, including after a preflight race."""
    writer = model.write()
    if overwrite:
        writer = writer.overwrite()
    writer.save(path)


def write_parquet(frame, path, overwrite=False):
    frame.write.mode("overwrite" if overwrite else "errorifexists").parquet(path)
