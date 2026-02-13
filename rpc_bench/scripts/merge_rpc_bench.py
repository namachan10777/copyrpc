# /// script
# requires-python = ">=3.10"
# dependencies = ["polars"]
# ///
"""Consolidate rpc_bench parquet files into a single unified parquet."""

import argparse
import glob
import os
import sys

import polars as pl


def main() -> None:
    parser = argparse.ArgumentParser(description="Merge rpc_bench parquet files")
    parser.add_argument(
        "input_glob",
        nargs="?",
        default="../result/paper/*.parquet",
        help="Glob pattern for input parquet files (default: ../result/paper/*.parquet)",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="../result/rpc_bench_unified.parquet",
        help="Output parquet path (default: ../result/rpc_bench_unified.parquet)",
    )
    args = parser.parse_args()

    files = sorted(glob.glob(args.input_glob))
    if not files:
        print(f"No files matched: {args.input_glob}", file=sys.stderr)
        sys.exit(1)

    frames: list[pl.DataFrame] = []
    for path in files:
        basename = os.path.basename(path)
        df = pl.read_parquet(path)
        df = df.with_columns(pl.lit(basename).alias("source_file"))
        frames.append(df)
        print(f"  {basename}: {len(df)} rows")

    merged = pl.concat(frames)
    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    merged.write_parquet(args.output)
    print(f"Written {len(merged)} rows to {args.output}")


if __name__ == "__main__":
    main()
