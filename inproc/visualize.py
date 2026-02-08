#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "polars",
#     "pyarrow",
#     "pandas",
#     "altair",
#     "vl-convert-python",
# ]
# ///
"""
Visualize Flux vs Mesh benchmark results.

Auto-discovers qd{N}_n{M}.parquet files and generates per-queue-depth
throughput charts. Falls back to results.parquet for legacy mode.

Usage:
    ./visualize.py [--bench-dir DIR] [--output-dir DIR]
"""

import argparse
import re
from pathlib import Path

import altair as alt
import polars as pl


# Color palettes: blue shades for Flux, orange shades for Mesh
FLUX_COLORS = {
    "onesided": "#1f77b4",
    "fastforward": "#4a90d9",
    "lamport": "#7eb3e8",
    "rtrb": "#a8d0f0",
    "omango": "#c6e2f7",
}

MESH_COLORS = {
    "std_mpsc": "#ff7f0e",
    "crossbeam": "#ffb366",
}


def load_qd_results(bench_dir: Path) -> dict[int, pl.DataFrame]:
    """Load per-queue-depth parquet files into a dict keyed by QD."""
    pattern = re.compile(r"qd(\d+)_n(\d+)\.parquet")
    qd_frames: dict[int, list[pl.DataFrame]] = {}

    for f in sorted(bench_dir.glob("qd*_n*.parquet")):
        m = pattern.match(f.name)
        if not m:
            continue
        qd = int(m.group(1))
        df = pl.read_parquet(f)
        if "kind" not in df.columns:
            df = df.with_columns(pl.lit("flux").alias("kind"))
        df = df.with_columns(
            (pl.col("kind") + "/" + pl.col("transport")).alias("implementation")
        )
        qd_frames.setdefault(qd, []).append(df)

    return {qd: pl.concat(frames) for qd, frames in sorted(qd_frames.items())}


def _build_color_scale(df: pl.DataFrame) -> alt.Scale:
    """Build a color scale mapping implementation labels to colors."""
    impls = df["implementation"].unique().sort().to_list()
    colors = []
    for impl_name in impls:
        kind, transport = impl_name.split("/", 1)
        if kind == "flux":
            colors.append(FLUX_COLORS.get(transport, "#1f77b4"))
        else:
            colors.append(MESH_COLORS.get(transport, "#ff7f0e"))
    return alt.Scale(domain=impls, range=colors)


def create_throughput_chart(df: pl.DataFrame, title: str) -> alt.Chart:
    """Create throughput vs threads chart with all implementation variants."""
    color_scale = _build_color_scale(df)

    chart = (
        alt.Chart(df.to_pandas())
        .mark_line(point=True)
        .encode(
            x=alt.X("threads:Q", title="Number of Threads"),
            y=alt.Y("throughput_mops_mean:Q", title="Throughput (Mops/s)"),
            color=alt.Color("implementation:N", title="Implementation",
                            scale=color_scale),
            strokeDash=alt.StrokeDash("kind:N", title="Kind"),
        )
        .properties(title=title, width=700, height=450)
    )
    return chart


def main():
    parser = argparse.ArgumentParser(description="Visualize Flux vs Mesh benchmark results")
    parser.add_argument(
        "--bench-dir",
        type=Path,
        default=Path(__file__).parent / "bench_results",
        help="Directory containing benchmark parquet files",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).parent / "bench_results",
        help="Directory to save output charts",
    )
    args = parser.parse_args()

    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    qd_data = load_qd_results(args.bench_dir)

    if not qd_data:
        print("No qd*_n*.parquet files found")
        return

    for qd, df in qd_data.items():
        print(f"\n=== Queue Depth {qd} ({len(df)} rows) ===")
        print(df.select(["threads", "kind", "transport", "throughput_mops_mean"]))

        title = f"All-to-All Throughput (Queue Depth = {qd})"
        chart = create_throughput_chart(df, title)

        svg_path = output_dir / f"throughput_qd{qd}.svg"
        png_path = output_dir / f"throughput_qd{qd}.png"
        chart.save(str(svg_path))
        chart.save(str(png_path), scale_factor=2)
        print(f"Saved {svg_path} and {png_path}")

    print("\nDone!")


if __name__ == "__main__":
    main()
