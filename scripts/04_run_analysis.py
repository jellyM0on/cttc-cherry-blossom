import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from src.analysis.run_analysis import run_analysis

def main() -> None:
    run_analysis()

if __name__ == "__main__":
    main()