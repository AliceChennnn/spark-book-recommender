"""Register existing local JSON snapshots for the current code/data version."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.web_backend import WebBackend  # noqa: E402


if __name__ == "__main__":
    count = WebBackend().prepare_demo_manifest()
    print(f"Registered {count} validated demo snapshots; no Spark jobs submitted.")
