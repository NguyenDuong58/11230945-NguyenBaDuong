"""CLI entry point for the verified pandas–Polars–PySpark processing experiment."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from fdic_pipeline.scalable_processing import main

if __name__ == "__main__":
    raise SystemExit(main())
