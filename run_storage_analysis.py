"""CLI: audit measured trials and prepare the Part 1 evidence package."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
from fdic_pipeline.storage_analysis import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
