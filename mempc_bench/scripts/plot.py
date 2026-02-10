#!/usr/bin/env python3
"""Visualize mempc benchmark results (QD sweep, manyclient + alltoall)."""

import json
from pathlib import Path

import polars as pl
import altair as alt

ROOT = Path(__file__).resolve().parent.parent
DISTDIR = ROOT / "dist"

MANYCLIENT_SUMMARY = DISTDIR / "manyclient_summary.json"
ALLTOALL_SUMMARY = DISTDIR / "alltoall_summary.json"


def load_summary(path: Path) -> pl.DataFrame:
    with open(path) as f:
        raw = json.load(f)

    rows = []
    for entry in raw:
        mops = entry.get("median_mops")
        if mops is None:
            continue

        perf = {}
        for ev in entry.get("perf_events", []):
            name = ev["event"].replace("ls_dmnd_fills_from_sys.", "")
            val = ev.get("counter-value", "0")
            if val == "<not counted>":
                val = 0.0
            else:
                val = float(val)
            perf[name] = val

        total_fills = sum(perf.values())
        perf_mops = entry.get("perf_run_mops")
        if perf_mops and perf_mops > 0 and total_fills > 0:
            duration = entry.get("perf_duration_secs", 5)
            total_ops = perf_mops * 1e6 * duration
            fills_per_op = total_fills / total_ops if total_ops > 0 else None
        else:
            fills_per_op = None

        row = {
            "scenario": entry.get("scenario", "unknown"),
            "transport": entry["transport"],
            "threads": entry["threads"],
            "qd": entry["qd"],
            "median_mops": mops,
            "fills_per_op": fills_per_op,
            **{f"fills_{k}": v for k, v in perf.items()},
        }
        if "clients" in entry:
            row["clients"] = entry["clients"]
        rows.append(row)

    return pl.DataFrame(rows)


def categorize(transport: str) -> str:
    if transport.startswith("tbb_"):
        return "TBB (C++)"
    if transport in ("onesided", "fast-forward", "lamport"):
        return "SPSC-per-client"
    return "Shared MPSC"


def plot_throughput_vs_qd(df: pl.DataFrame, scenario: str, facet_col: str):
    """Throughput vs QD, faceted by n (threads or clients)."""
    filtered = df.filter(pl.col("scenario") == scenario)
    if filtered.height == 0:
        print(f"  (skipped {scenario} throughput_vs_qd: no data)")
        return

    pdf = filtered.to_pandas()
    pdf["category"] = pdf["transport"].apply(categorize)

    base = (
        alt.Chart(pdf)
        .mark_line(point=True)
        .encode(
            x=alt.X("qd:O", title="Queue Depth (inflight)"),
            y=alt.Y("median_mops:Q", title="Throughput (Mops/s)"),
            color=alt.Color(
                "transport:N",
                title="Transport",
                sort=alt.EncodingSortField(
                    field="median_mops", op="max", order="descending"
                ),
            ),
            strokeDash=alt.StrokeDash("category:N", title="Category"),
            tooltip=["transport", facet_col, "qd", "median_mops"],
        )
        .properties(width=350, height=280)
    )
    chart = (
        base.facet(column=alt.Column(f"{facet_col}:N", title="Threads"))
        .resolve_scale(y="independent")
        .properties(title=f"Throughput vs QD ({scenario})")
    )
    outpath = DISTDIR / f"{scenario}_throughput_vs_qd.png"
    chart.save(str(outpath), scale_factor=2)
    print(f"  -> {outpath.name}")


def plot_throughput_vs_threads(df: pl.DataFrame, scenario: str, facet_col: str):
    """Throughput vs threads/clients, faceted by QD subset."""
    filtered = df.filter(pl.col("scenario") == scenario)
    if filtered.height == 0:
        print(f"  (skipped {scenario} throughput_vs_threads: no data)")
        return

    qd_subset = [1, 8, 64, 256]
    filtered = filtered.filter(pl.col("qd").is_in(qd_subset))
    if filtered.height == 0:
        print(f"  (skipped {scenario} throughput_vs_threads: no matching QDs)")
        return

    pdf = filtered.to_pandas()
    pdf["category"] = pdf["transport"].apply(categorize)

    base = (
        alt.Chart(pdf)
        .mark_line(point=True)
        .encode(
            x=alt.X(f"{facet_col}:O", title="Threads"),
            y=alt.Y("median_mops:Q", title="Throughput (Mops/s)"),
            color=alt.Color(
                "transport:N",
                title="Transport",
                sort=alt.EncodingSortField(
                    field="median_mops", op="max", order="descending"
                ),
            ),
            strokeDash=alt.StrokeDash("category:N", title="Category"),
            tooltip=["transport", facet_col, "qd", "median_mops"],
        )
        .properties(width=300, height=250)
    )
    chart = (
        base.facet(column=alt.Column("qd:N", title="Queue Depth"))
        .resolve_scale(y="independent")
        .properties(title=f"Throughput vs Threads ({scenario})")
    )
    outpath = DISTDIR / f"{scenario}_throughput_vs_threads.png"
    chart.save(str(outpath), scale_factor=2)
    print(f"  -> {outpath.name}")


def plot_fills_per_op(df: pl.DataFrame):
    """Cache fills/op vs throughput (log-log scatter), all scenarios."""
    filtered = df.filter(pl.col("fills_per_op").is_not_null() & (pl.col("fills_per_op") > 0))
    if filtered.height == 0:
        print("  (skipped fills_per_op: no data)")
        return

    pdf = filtered.to_pandas()
    pdf["category"] = pdf["transport"].apply(categorize)
    pdf["label"] = pdf["transport"] + " n=" + pdf["threads"].astype(str)

    chart = (
        alt.Chart(pdf)
        .mark_circle(size=80)
        .encode(
            x=alt.X(
                "fills_per_op:Q",
                title="Cache Line Fills / Op",
                scale=alt.Scale(type="log"),
            ),
            y=alt.Y(
                "median_mops:Q",
                title="Throughput (Mops/s)",
                scale=alt.Scale(type="log"),
            ),
            color=alt.Color("transport:N", title="Transport"),
            shape=alt.Shape("scenario:N", title="Scenario"),
            tooltip=["transport", "scenario", "threads", "qd", "median_mops", "fills_per_op"],
        )
        .properties(width=500, height=400, title="Cache Fills/Op vs Throughput (log-log)")
    )
    outpath = DISTDIR / "fills_vs_throughput.png"
    chart.save(str(outpath), scale_factor=2)
    print(f"  -> {outpath.name}")


def plot_fill_breakdown(df: pl.DataFrame):
    """Stacked bar of cache fill sources, QD=8, smallest thread count per scenario."""
    fill_cols = [c for c in df.columns if c.startswith("fills_") and c != "fills_per_op"]
    if not fill_cols:
        print("  (skipped fill_breakdown: no perf data)")
        return

    filtered = df.filter(pl.col("qd") == 8)
    if filtered.height == 0:
        print("  (skipped fill_breakdown: no QD=8 data)")
        return

    # Take the smallest thread count per scenario
    min_threads = filtered.group_by("scenario").agg(pl.col("threads").min().alias("min_threads"))
    filtered = filtered.join(min_threads, on="scenario").filter(
        pl.col("threads") == pl.col("min_threads")
    )

    melted = filtered.select("transport", "scenario", *fill_cols).unpivot(
        index=["transport", "scenario"],
        on=fill_cols,
        variable_name="source",
        value_name="fills",
    )
    melted = melted.with_columns(pl.col("source").str.replace("fills_", ""))

    pdf = melted.to_pandas()
    chart = (
        alt.Chart(pdf)
        .mark_bar()
        .encode(
            x=alt.X("transport:N", title="Transport", sort="-y"),
            y=alt.Y("fills:Q", title="Total Cache Line Fills", stack="zero"),
            color=alt.Color(
                "source:N",
                title="Fill Source",
                scale=alt.Scale(
                    domain=[
                        "local_l2",
                        "local_ccx",
                        "near_cache",
                        "far_cache",
                        "dram_io_near",
                        "dram_io_far",
                    ],
                    range=[
                        "#4c78a8",
                        "#f58518",
                        "#e45756",
                        "#72b7b2",
                        "#54a24b",
                        "#b279a2",
                    ],
                ),
            ),
            column=alt.Column("scenario:N", title="Scenario"),
            tooltip=["transport", "scenario", "source", "fills"],
        )
        .properties(width=400, height=350, title="Cache Fill Breakdown (QD=8)")
    )
    outpath = DISTDIR / "fill_breakdown.png"
    chart.save(str(outpath), scale_factor=2)
    print(f"  -> {outpath.name}")


def main():
    DISTDIR.mkdir(parents=True, exist_ok=True)

    frames = []
    for path in [MANYCLIENT_SUMMARY, ALLTOALL_SUMMARY]:
        if path.exists():
            frames.append(load_summary(path))
            print(f"Loaded {path.name}: {frames[-1].height} entries")
        else:
            print(f"Warning: {path.name} not found, skipping")

    if not frames:
        print("No data found. Run benchmark scripts first.")
        return

    df = pl.concat(frames, how="diagonal_relaxed")

    # Filter out near-zero entries
    df_valid = df.filter(pl.col("median_mops") > 0.05)
    print(f"Valid entries (>0.05 Mops/s): {df_valid.height}")

    print("Generating plots...")

    # 1. Throughput vs QD (manyclient, faceted by clients)
    plot_throughput_vs_qd(df_valid, "manyclient", "clients")

    # 2. Throughput vs QD (alltoall, faceted by threads)
    plot_throughput_vs_qd(df_valid, "alltoall", "threads")

    # 3. Throughput vs threads (faceted by QD subset)
    plot_throughput_vs_threads(df_valid, "manyclient", "clients")
    plot_throughput_vs_threads(df_valid, "alltoall", "threads")

    # 4. Fills/op vs throughput (log-log)
    plot_fills_per_op(df_valid)

    # 5. Fill breakdown (stacked bar)
    plot_fill_breakdown(df_valid)

    print("Done!")


if __name__ == "__main__":
    main()
