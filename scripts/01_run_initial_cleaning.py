import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from src.cleaning.clean_initial_blossom_data import clean_initial_blossom_data

def main() -> None:
    clean_initial_blossom_data()

if __name__ == "__main__":
    main()