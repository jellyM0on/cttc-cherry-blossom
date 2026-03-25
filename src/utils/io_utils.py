from pathlib import Path
import pandas as pd

def append_row_to_csv(row_dict: dict, output_file: Path) -> None:
    row_df = pd.DataFrame([row_dict])
    write_header = not output_file.exists()
    row_df.to_csv(
        output_file,
        mode="a",
        header=write_header,
        index=False,
        encoding="utf-8-sig" if write_header else "utf-8",
    )

def load_completed_station_codes(output_file: Path) -> set[str]:
    if not output_file.exists():
        return set()

    try:
        existing_df = pd.read_csv(output_file, dtype={"jma_station_code": str})
    except Exception as e:
        print(f"Warning: could not read existing output file {output_file}: {e}")
        return set()

    if "jma_station_code" not in existing_df.columns:
        return set()

    completed = existing_df["jma_station_code"].dropna().astype(str).str.strip()
    completed = completed[completed != ""]
    return set(completed.tolist())