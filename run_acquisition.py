"""CLI: acquire and verify the FDIC tables used by BFT65B Part 1."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
from fdic_pipeline.acquisition import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
