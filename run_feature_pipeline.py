"""Entry point for the verified FDIC feature and outcome pipeline."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
from fdic_pipeline.feature_pipeline import main

if __name__ == "__main__":
    raise SystemExit(main())
