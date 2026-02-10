#!/usr/bin/env python3
"""Visualize mempc benchmark results (QD=1/8, all transports + TBB)."""

import json
from pathlib import Path

import polars as pl
import altair as alt

ROOT = Path(__file__).resolve().parent.parent
SUMMARY = ROOT / "bench_results" / "qd_perf" / "summary.json"
OUTDIR = ROOT / "bench_results" / "viz"


def load_data() -> pl.DataFrame:
    with open(SUMMARY) as f:
        raw = json.load(f)

    rows = []
    for entry in raw:
        mops = entry.get("median_mops")
        if mops is None:
            continue

        # Parse perf events into flat columns
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
        # perf_run_mops is throughput during perf run (1 run)
        perf_mops = entry.get("perf_run_mops")
        if perf_mops and perf_mops > 0 and total_fills > 0:
            duration = entry["perf_duration_secs"]
            total_ops = perf_mops * 1e6 * duration
            fills_per_op = total_fills / total_ops if total_ops > 0 else None
        else:
            fills_per_op = None

        rows.append(
            {
                "transport": entry["transport"],
                "clients": entry["clients"],
                "qd": entry["qd"],
                "median_mops": mops,
                "fills_per_op": fills_per_op,
                **{f"fills_{k}": v for k, v in perf.items()},
            }
        )

    return pl.DataFrame(rows)


def categorize(transport: str) -> str:
    if transport.startswith("tbb_"):
        return "TBB (C++)"
    if transport in ("onesided", "fast-forward", "lamport"):
        return "SPSC-per-client"
    return "Shared MPSC"


def plot_throughput(df: pl.DataFrame):
    """Throughput vs clients, faceted by QD."""
    pdf = df.to_pandas()
    pdf["category"] = pdf["transport"].apply(categorize)

    base = (
        alt.Chart(pdf)
        .mark_line(point=True)
        .encode(
            x=alt.X("clients:O", title="Clients"),
            y=alt.Y("median_mops:Q", title="Throughput (Mops/s)"),
            color=alt.Color(
                "transport:N",
                title="Transport",
                sort=alt.EncodingSortField(field="median_mops", op="max", order="descending"),
            ),
            strokeDash=alt.StrokeDash("category:N", title="Category"),
            tooltip=["transport", "clients", "qd", "median_mops"],
        )
        .properties(width=400, height=300)
    )
    chart = (
        base.facet(column=alt.Column("qd:N", title="Queue Depth"))
        .resolve_scale(y="shared")
        .properties(title="Throughput vs Clients")
    )
    chart.save(f"{OUTDIR}/throughput.png", scale_factor=2)
    print("  -> throughput.png")


def plot_qd_effect(df: pl.DataFrame):
    """QD=8/QD=1 speedup ratio."""
    qd1 = df.filter(pl.col("qd") == 1).select("transport", "clients", "median_mops").rename({"median_mops": "mops_qd1"})
    qd8 = df.filter(pl.col("qd") == 8).select("transport", "clients", "median_mops").rename({"median_mops": "mops_qd8"})
    joined = qd1.join(qd8, on=["transport", "clients"])
    joined = joined.with_columns((pl.col("mops_qd8") / pl.col("mops_qd1")).alias("speedup"))

    pdf = joined.to_pandas()
    pdf["category"] = pdf["transport"].apply(categorize)

    chart = (
        alt.Chart(pdf)
        .mark_bar()
        .encode(
            x=alt.X("transport:N", title="Transport", sort="-y"),
            y=alt.Y("speedup:Q", title="QD=8 / QD=1 Speedup"),
            color=alt.Color("category:N", title="Category"),
            column=alt.Column("clients:O", title="Clients"),
            tooltip=["transport", "clients", "speedup", "mops_qd1", "mops_qd8"],
        )
        .properties(width=120, height=250, title="QD Effect (QD=8 / QD=1)")
    )
    chart.save(f"{OUTDIR}/qd_effect.png", scale_factor=2)
    print("  -> qd_effect.png")


def plot_fills_per_op(df: pl.DataFrame):
    """Cache fills/op vs throughput, colored by transport."""
    filtered = df.filter(pl.col("fills_per_op").is_not_null() & (pl.col("fills_per_op") > 0))
    if filtered.height == 0:
        print("  (skipped fills_per_op: no data)")
        return

    pdf = filtered.to_pandas()
    pdf["category"] = pdf["transport"].apply(categorize)
    pdf["label"] = pdf["transport"] + " " + pdf["clients"].astype(str) + "c"

    chart = (
        alt.Chart(pdf)
        .mark_circle(size=80)
        .encode(
            x=alt.X("fills_per_op:Q", title="Cache Line Fills / Op", scale=alt.Scale(type="log")),
            y=alt.Y("median_mops:Q", title="Throughput (Mops/s)", scale=alt.Scale(type="log")),
            color=alt.Color("transport:N", title="Transport"),
            shape=alt.Shape("qd:N", title="QD"),
            tooltip=["transport", "clients", "qd", "median_mops", "fills_per_op"],
        )
        .properties(width=500, height=400, title="Cache Fills/Op vs Throughput (log-log)")
    )
    chart.save(f"{OUTDIR}/fills_vs_throughput.png", scale_factor=2)
    print("  -> fills_vs_throughput.png")


def plot_fill_breakdown(df: pl.DataFrame):
    """Stacked bar of cache fill sources, QD=8, clients=2."""
    fill_cols = [c for c in df.columns if c.startswith("fills_") and c != "fills_per_op"]
    filtered = df.filter((pl.col("qd") == 8) & (pl.col("clients") == 2))
    if filtered.height == 0:
        print("  (skipped fill_breakdown: no data)")
        return

    melted = filtered.select("transport", *fill_cols).unpivot(
        index="transport",
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
                    domain=["local_l2", "local_ccx", "near_cache", "far_cache", "dram_io_near", "dram_io_far"],
                    range=["#4c78a8", "#f58518", "#e45756", "#72b7b2", "#54a24b", "#b279a2"],
                ),
            ),
            tooltip=["transport", "source", "fills"],
        )
        .properties(width=500, height=350, title="Cache Fill Breakdown (QD=8, 2 clients)")
    )
    chart.save(f"{OUTDIR}/fill_breakdown.png", scale_factor=2)
    print("  -> fill_breakdown.png")


def main():
    OUTDIR.mkdir(parents=True, exist_ok=True)

    df = load_data()
    print(f"Loaded {df.height} entries")

    # Filter out near-zero entries for cleaner plots
    df_valid = df.filter(pl.col("median_mops") > 0.05)
    print(f"Valid entries (>0.05 Mops/s): {df_valid.height}")

    print("Generating plots...")
    plot_throughput(df_valid)
    plot_qd_effect(df_valid)
    plot_fills_per_op(df_valid)
    plot_fill_breakdown(df_valid)
    print("Done!")


if __name__ == "__main__":
    main()
