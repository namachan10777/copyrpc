# /// script
# requires-python = ">=3.10"
# dependencies = ["polars"]
# ///
"""Consolidate benchkv parquet files into a single unified parquet."""

import argparse
import glob
import os
import re
import sys

import polars as pl


def parse_np_from_filename(filename: str) -> int | None:
    m = re.search(r"_np(\d+)", filename)
    return int(m.group(1)) if m else None


def infer_batch_hold_us(filename: str, mode_values: set[str]) -> float | None:
    basename = os.path.basename(filename)
    if "nohold" in basename:
        return 0.0
    if "delegation" in mode_values:
        return 10.0
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Merge benchkv parquet files")
    parser.add_argument(
        "input_glob",
        nargs="*",
        default=["../result/paper_*.parquet"],
        help="Glob patterns for input parquet files (default: ../result/paper_*.parquet)",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="../result/benchkv_unified.parquet",
        help="Output parquet path (default: ../result/benchkv_unified.parquet)",
    )
    args = parser.parse_args()

    files = sorted(set(f for pattern in args.input_glob for f in glob.glob(pattern)))
    if not files:
        print(f"No files matched: {args.input_glob}", file=sys.stderr)
        sys.exit(1)

    frames: list[pl.DataFrame] = []
    for path in files:
        basename = os.path.basename(path)
        df = pl.read_parquet(path)

        np_val = parse_np_from_filename(basename)
        if np_val is None:
            print(f"  WARN: cannot parse NP from {basename}, skipping", file=sys.stderr)
            continue
        mode_values = set(df["mode"].unique().to_list())
        hold = infer_batch_hold_us(basename, mode_values)

        df = df.with_columns(
            pl.lit(np_val).cast(pl.UInt32).alias("np"),
            pl.lit(hold).cast(pl.Float64).alias("batch_hold_us"),
            pl.lit(basename).alias("source_file"),
        )
        frames.append(df)
        print(f"  {basename}: {len(df)} rows, np={np_val}, hold={hold}")

    merged = pl.concat(frames)
    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    merged.write_parquet(args.output)
    print(f"Written {len(merged)} rows to {args.output}")


if __name__ == "__main__":
    main()
