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
Visualize Flux benchmark results.

Usage:
    ./visualize.py [--output-dir DIR]
"""

import argparse
from pathlib import Path

import altair as alt
import polars as pl


def load_results(bench_dir: Path) -> pl.DataFrame:
    """Load and combine all parquet files in the benchmark directory."""
    frames = []
    for parquet_file in bench_dir.glob("*.parquet"):
        df = pl.read_parquet(parquet_file)
        frames.append(df)

    if not frames:
        raise FileNotFoundError(f"No parquet files found in {bench_dir}")

    return pl.concat(frames)


def create_throughput_chart(df: pl.DataFrame) -> alt.Chart:
    """Create throughput vs threads chart."""
    # Create label for each configuration
    df_labeled = df.with_columns(
        pl.when(pl.col("api") == "legacy")
        .then(pl.lit("Legacy API"))
        .otherwise(pl.concat_str([pl.lit("Batch (size="), pl.col("batch_size").cast(pl.Utf8), pl.lit(")")]))
        .alias("config")
    )

    chart = (
        alt.Chart(df_labeled.to_pandas())
        .mark_line(point=True)
        .encode(
            x=alt.X("threads:Q", title="Number of Threads", scale=alt.Scale(domain=[2, 24])),
            y=alt.Y("throughput_mops_mean:Q", title="Throughput (Mops/s)"),
            color=alt.Color("config:N", title="Configuration"),
            strokeDash=alt.StrokeDash("config:N"),
        )
        .properties(title="Flux All-to-All Throughput", width=600, height=400)
    )
    return chart


def create_latency_chart(df: pl.DataFrame) -> alt.Chart:
    """Create latency vs threads chart."""
    df_labeled = df.with_columns(
        pl.when(pl.col("api") == "legacy")
        .then(pl.lit("Legacy API"))
        .otherwise(pl.concat_str([pl.lit("Batch (size="), pl.col("batch_size").cast(pl.Utf8), pl.lit(")")]))
        .alias("config")
    )

    chart = (
        alt.Chart(df_labeled.to_pandas())
        .mark_line(point=True)
        .encode(
            x=alt.X("threads:Q", title="Number of Threads", scale=alt.Scale(domain=[2, 24])),
            y=alt.Y("latency_ns_mean:Q", title="Latency (ns/call)"),
            color=alt.Color("config:N", title="Configuration"),
            strokeDash=alt.StrokeDash("config:N"),
        )
        .properties(title="Flux All-to-All Latency", width=600, height=400)
    )
    return chart


def create_duration_chart(df: pl.DataFrame) -> alt.Chart:
    """Create duration vs threads chart with error bars."""
    df_labeled = df.with_columns(
        [
            pl.when(pl.col("api") == "legacy")
            .then(pl.lit("Legacy API"))
            .otherwise(pl.concat_str([pl.lit("Batch (size="), pl.col("batch_size").cast(pl.Utf8), pl.lit(")")]))
            .alias("config"),
            (pl.col("duration_ns_mean") / 1_000_000).alias("duration_ms_mean"),
            (pl.col("duration_ns_min") / 1_000_000).alias("duration_ms_min"),
            (pl.col("duration_ns_max") / 1_000_000).alias("duration_ms_max"),
        ]
    )

    base = alt.Chart(df_labeled.to_pandas())

    line = base.mark_line(point=True).encode(
        x=alt.X("threads:Q", title="Number of Threads", scale=alt.Scale(domain=[2, 24])),
        y=alt.Y("duration_ms_mean:Q", title="Duration (ms)"),
        color=alt.Color("config:N", title="Configuration"),
    )

    error_bars = base.mark_errorbar().encode(
        x=alt.X("threads:Q"),
        y=alt.Y("duration_ms_min:Q", title="Duration (ms)"),
        y2=alt.Y2("duration_ms_max:Q"),
        color=alt.Color("config:N"),
    )

    chart = (line + error_bars).properties(title="Flux All-to-All Duration", width=600, height=400)
    return chart


def create_scaling_chart(df: pl.DataFrame) -> alt.Chart:
    """Create scaling efficiency chart (throughput / total_calls)."""
    df_labeled = df.with_columns(
        [
            pl.when(pl.col("api") == "legacy")
            .then(pl.lit("Legacy API"))
            .otherwise(pl.concat_str([pl.lit("Batch (size="), pl.col("batch_size").cast(pl.Utf8), pl.lit(")")]))
            .alias("config"),
            # Normalize throughput by number of thread pairs: n*(n-1)
            (pl.col("throughput_mops_mean") / (pl.col("threads") * (pl.col("threads") - 1))).alias(
                "throughput_per_pair"
            ),
        ]
    )

    chart = (
        alt.Chart(df_labeled.to_pandas())
        .mark_line(point=True)
        .encode(
            x=alt.X("threads:Q", title="Number of Threads", scale=alt.Scale(domain=[2, 24])),
            y=alt.Y("throughput_per_pair:Q", title="Throughput per Thread Pair (Mops/s)"),
            color=alt.Color("config:N", title="Configuration"),
            strokeDash=alt.StrokeDash("config:N"),
        )
        .properties(title="Flux Scaling Efficiency", width=600, height=400)
    )
    return chart


def main():
    parser = argparse.ArgumentParser(description="Visualize Flux benchmark results")
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

    print(f"Loading results from {args.bench_dir}")
    df = load_results(args.bench_dir)

    print(f"Loaded {len(df)} benchmark results")
    print(df)

    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    # Create and save charts
    charts = [
        ("throughput", create_throughput_chart(df)),
        ("latency", create_latency_chart(df)),
        ("duration", create_duration_chart(df)),
        ("scaling", create_scaling_chart(df)),
    ]

    for name, chart in charts:
        svg_path = output_dir / f"{name}.svg"
        png_path = output_dir / f"{name}.png"
        chart.save(str(svg_path))
        chart.save(str(png_path), scale_factor=2)
        print(f"Saved {svg_path} and {png_path}")

    # Create combined chart
    throughput_chart = create_throughput_chart(df)
    latency_chart = create_latency_chart(df)
    combined = alt.vconcat(throughput_chart, latency_chart).properties(
        title="Flux All-to-All Benchmark Results"
    )
    combined_path = output_dir / "combined.svg"
    combined_png_path = output_dir / "combined.png"
    combined.save(str(combined_path))
    combined.save(str(combined_png_path), scale_factor=2)
    print(f"Saved {combined_path} and {combined_png_path}")

    print("\nDone!")


if __name__ == "__main__":
    main()
