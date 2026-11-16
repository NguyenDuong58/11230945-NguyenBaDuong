"""Part 2 command-line entry point. Run from the repository root or Colab."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
from fdic_pipeline.panel import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
