"""Dataset identities and paths; no Spark, filesystem discovery or network calls."""

import hashlib
import json
import math
import re
from dataclasses import asdict, dataclass


GOODBOOKS_ROOT = "hdfs://master:9000/user/root/bookrec"
GOODBOOKS_VERSION = "goodbooks10k-processed-v1-als_full_v1-topics_v1"
_TOKEN = re.compile(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}")


def _token(value, name):
    if not isinstance(value, str) or not _TOKEN.fullmatch(value):
        raise ValueError(f"{name} must be a simple nonempty path token")


@dataclass(frozen=True)
class RatingScale:
    source_min: float
    source_max: float
    target_min: float
    target_max: float
    transform: str

    def __post_init__(self):
        bounds = (self.source_min, self.source_max, self.target_min, self.target_max)
        if any(type(v) not in (int, float) or not math.isfinite(v) for v in bounds):
            raise ValueError("Rating bounds must be finite numbers")
        if self.source_min >= self.source_max or self.target_min >= self.target_max:
            raise ValueError("Rating ranges must have increasing bounds")
        if not math.isfinite(self.source_max - self.source_min) or not math.isfinite(self.target_max - self.target_min):
            raise ValueError("Rating range widths must be finite")
        if self.transform not in ("preserve", "linear"):
            raise ValueError("Declare an explicit preserve or linear rating transform")
        if self.transform == "preserve" and bounds[:2] != bounds[2:]:
            raise ValueError("preserve requires identical source and target rating ranges")


@dataclass(frozen=True)
class DatasetSpec:
    dataset_id: str
    data_version: str
    mapping_version: str
    model_version: str
    ratings_columns: dict
    books_columns: dict
    rating_scale: RatingScale
    topics: str

    def __post_init__(self):
        for name in ("dataset_id", "data_version", "mapping_version", "model_version"):
            _token(getattr(self, name), name)
        if self.dataset_id.lower().startswith("goodbooks"):
            raise ValueError("Goodbooks is reserved; use the unchanged legacy entry points")
        for mapping, keys in ((self.ratings_columns, {"user_id", "book_id", "rating"}),
                              (self.books_columns, {"book_id", "title", "authors"})):
            if not isinstance(mapping, dict) or set(mapping) != keys:
                raise ValueError(f"Declare exactly these canonical fields: {sorted(keys)}")
            fields = []
            for key, source in mapping.items():
                if key == "authors" and source is None:
                    continue
                if not isinstance(source, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", source):
                    raise ValueError("Source column names must be simple names; authors may be null")
                fields.append(source)
            if len(fields) != len(set(fields)):
                raise ValueError("Different canonical fields cannot reference the same source column")
        if not isinstance(self.rating_scale, RatingScale):
            raise ValueError("rating_scale must explicitly describe the source and target scales")
        if self.topics != "none":
            raise ValueError("This stage supports topics=none; Goodreads tags keep their legacy adapter")
        # Defensively copy caller-owned dictionaries before computing identities.
        object.__setattr__(self, "ratings_columns", dict(self.ratings_columns))
        object.__setattr__(self, "books_columns", dict(self.books_columns))

    @classmethod
    def from_dict(cls, value):
        try:
            fields = dict(value)
            fields["rating_scale"] = RatingScale(**fields["rating_scale"])
            return cls(**fields)
        except (KeyError, TypeError) as exc:
            raise ValueError(f"Invalid dataset configuration: {exc}") from exc

    @property
    def preparation_config(self):
        value = asdict(self)
        value.pop("model_version")
        value["adapter_version"] = "csv-fields-v1"
        return value

    @property
    def data_binding(self):
        payload = json.dumps(self.preparation_config, sort_keys=True, separators=(",", ":"))
        return {"dataset_id": self.dataset_id, "data_version": self.data_version,
                "mapping_version": self.mapping_version,
                "preparation_sha256": hashlib.sha256(payload.encode()).hexdigest()}

    @property
    def model_binding(self):
        return dict(self.data_binding, model_version=self.model_version)

    @property
    def cache_version(self):
        payload = json.dumps(self.model_binding, sort_keys=True).encode()
        return f"dataset-{self.dataset_id}-{hashlib.sha256(payload).hexdigest()}"

    @property
    def root(self):
        return f"{GOODBOOKS_ROOT}/datasets/{self.dataset_id}"

    @property
    def raw_root(self):
        return f"{self.root}/raw/{self.data_version}"

    @property
    def prepared_root(self):
        return f"{self.root}/releases/{self.data_version}/{self.mapping_version}"

    @property
    def model_path(self):
        return f"{self.prepared_root}/models/{self.model_version}"

    @property
    def local_namespace(self):
        return f"outputs/datasets/{self.dataset_id}/{self.data_version}/{self.mapping_version}"


def require_artifact_binding(manifest, spec, kind="prepared"):
    """Future readers must validate this before loading any prepared data/model."""
    if kind not in ("prepared", "als"):
        raise ValueError("Unknown artifact kind")
    expected = spec.data_binding if kind == "prepared" else spec.model_binding
    if manifest.get("artifact_kind") != kind or manifest.get("binding") != expected:
        raise ValueError("Dataset/data/mapping/model binding mismatch; refusing artifact reuse")
