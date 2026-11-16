"""CLI entry point for physical Parquet layout measurements."""
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
from fdic_pipeline.layout_benchmark import main  # noqa: E402
if __name__ == "__main__":
    raise SystemExit(main())
