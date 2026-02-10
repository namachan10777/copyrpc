#!/usr/bin/env python3
"""Visualize rpc_bench results (Parquet-based)."""

from pathlib import Path

import polars as pl
import altair as alt

ROOT = Path(__file__).resolve().parent.parent
RESULTDIR = ROOT / "result"
DISTDIR = ROOT / "dist"


def load_parquet_dir(subdir: str) -> pl.DataFrame:
    """Load and concatenate all Parquet files in a result subdirectory."""
    d = RESULTDIR / subdir
    if not d.exists():
        return pl.DataFrame()
    files = sorted(d.glob("*.parquet"))
    if not files:
        return pl.DataFrame()
    frames = [pl.read_parquet(f) for f in files]
    return pl.concat(frames, how="diagonal_relaxed")


def compute_median_rps(df: pl.DataFrame, group_cols: list[str]) -> pl.DataFrame:
    """Compute median RPS per configuration (across epochs and runs)."""
    return df.group_by(group_cols).agg(
        pl.col("rps").median().alias("median_mrps") / 1e6,
    )


def plot_one_to_one_vs_endpoints(df: pl.DataFrame):
    """Throughput vs Endpoints, faceted by Threads, for each QD."""
    if df.height == 0:
        print("  (skipped 1to1 vs endpoints: no data)")
        return

    med = compute_median_rps(
        df,
        ["system", "endpoints", "inflight_per_ep", "threads"],
    )

    for qd in sorted(med["inflight_per_ep"].unique().to_list()):
        subset = med.filter(pl.col("inflight_per_ep") == qd)
        pdf = subset.to_pandas()

        base = (
            alt.Chart(pdf)
            .mark_line(point=True)
            .encode(
                x=alt.X(
                    "endpoints:Q",
                    title="Endpoints",
                    scale=alt.Scale(type="log", base=2),
                ),
                y=alt.Y("median_mrps:Q", title="Throughput (Mrps)"),
                color=alt.Color(
                    "system:N",
                    title="System",
                    sort=alt.EncodingSortField(
                        field="median_mrps", op="max", order="descending"
                    ),
                ),
                tooltip=["system", "endpoints", "threads", "median_mrps"],
            )
            .properties(width=400, height=300)
        )
        chart = (
            base.facet(column=alt.Column("threads:N", title="Threads"))
            .resolve_scale(y="independent")
            .properties(title=f"1-to-1 Throughput vs Endpoints (QD={qd})")
        )
        outpath = DISTDIR / f"one_to_one_vs_endpoints_qd{qd}.png"
        chart.save(str(outpath), scale_factor=2)
        print(f"  -> {outpath.name}")


def plot_one_to_one_vs_qd(df: pl.DataFrame):
    """Throughput vs QD, faceted by Threads. Uses max endpoints per (system, T, QD)."""
    if df.height == 0:
        print("  (skipped 1to1 vs QD: no data)")
        return

    med = compute_median_rps(
        df,
        ["system", "endpoints", "inflight_per_ep", "threads"],
    )

    # For each (system, threads, qd), take the best endpoints config
    best = med.group_by(["system", "threads", "inflight_per_ep"]).agg(
        pl.col("median_mrps").max(),
    )

    pdf = best.to_pandas()

    base = (
        alt.Chart(pdf)
        .mark_line(point=True)
        .encode(
            x=alt.X("inflight_per_ep:O", title="Queue Depth (inflight)"),
            y=alt.Y("median_mrps:Q", title="Throughput (Mrps)"),
            color=alt.Color(
                "system:N",
                title="System",
                sort=alt.EncodingSortField(
                    field="median_mrps", op="max", order="descending"
                ),
            ),
            tooltip=["system", "threads", "inflight_per_ep", "median_mrps"],
        )
        .properties(width=300, height=250)
    )
    chart = (
        base.facet(column=alt.Column("threads:N", title="Threads"))
        .resolve_scale(y="independent")
        .properties(title="1-to-1 Best Throughput vs QD")
    )
    outpath = DISTDIR / "one_to_one_vs_qd.png"
    chart.save(str(outpath), scale_factor=2)
    print(f"  -> {outpath.name}")


def plot_multi_client(df: pl.DataFrame):
    """Throughput vs number of clients, colored by system."""
    if df.height == 0:
        print("  (skipped multi-client: no data)")
        return

    med = compute_median_rps(df, ["system", "clients"])

    pdf = med.to_pandas()

    chart = (
        alt.Chart(pdf)
        .mark_line(point=True)
        .encode(
            x=alt.X("clients:O", title="Clients"),
            y=alt.Y("median_mrps:Q", title="Throughput (Mrps)"),
            color=alt.Color(
                "system:N",
                title="System",
                sort=alt.EncodingSortField(
                    field="median_mrps", op="max", order="descending"
                ),
            ),
            tooltip=["system", "clients", "median_mrps"],
        )
        .properties(
            width=500, height=350, title="Multi-Client Throughput vs Clients"
        )
    )
    outpath = DISTDIR / "multi_client_vs_clients.png"
    chart.save(str(outpath), scale_factor=2)
    print(f"  -> {outpath.name}")


def main():
    DISTDIR.mkdir(parents=True, exist_ok=True)

    # One-to-one
    df_1to1 = load_parquet_dir("one_to_one")
    if df_1to1.height > 0:
        print(f"Loaded one_to_one: {df_1to1.height} rows")
        print("Generating one-to-one plots...")
        plot_one_to_one_vs_endpoints(df_1to1)
        plot_one_to_one_vs_qd(df_1to1)
    else:
        print("No one-to-one data found")

    # Multi-client
    df_mc = load_parquet_dir("multi_client")
    if df_mc.height > 0:
        print(f"Loaded multi_client: {df_mc.height} rows")
        print("Generating multi-client plots...")
        plot_multi_client(df_mc)
    else:
        print("No multi-client data found")

    print("Done!")


if __name__ == "__main__":
    main()
