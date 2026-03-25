import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from src.cleaning.clean_and_merge import run_cleaning_and_merging

def main() -> None:
    run_cleaning_and_merging()

if __name__ == "__main__":
    main()