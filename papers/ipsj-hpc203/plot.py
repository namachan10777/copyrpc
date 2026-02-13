# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "polars",
#     "altair",
#     "vl-convert-python",
#     "pyarrow",
#     "pandas",
# ]
# ///

from pathlib import Path

import altair as alt
import polars as pl

ROOT = Path(__file__).resolve().parent
RESULT_DIR = ROOT / "result"
OUT_DIR = ROOT / "figures"


def compute_benchkv_rps(df: pl.DataFrame, trim_frac: float = 0.2) -> pl.DataFrame:
    """benchkv raw rows -> per-batch RPS with time-based trimming.

    For each (mode, np, batch_hold_us, run_index) group, compute the global
    time window [t_min, t_max] across all ranks/clients, then keep only
    batches whose elapsed_ns falls within the middle (1 - 2*trim_frac)
    portion of that window.
    """
    group_cols = ["mode", "np", "batch_hold_us", "run_index"]
    stream_cols = group_cols + ["rank", "client_id"]

    df = df.sort(stream_cols + ["batch_index"])

    # Compute per-batch RPS via diff
    df = df.with_columns(
        pl.col("elapsed_ns").diff().over(stream_cols).alias("delta_ns")
    )
    df = df.with_columns(
        (pl.col("batch_size").cast(pl.Float64) * 1e9 / pl.col("delta_ns")).alias("rps")
    )
    # Drop first batch per stream (null delta)
    df = df.filter(pl.col("delta_ns").is_not_null() & (pl.col("delta_ns") > 0))

    # Time-based trimming: keep middle 60% of global time window per group
    df = df.with_columns(
        pl.col("elapsed_ns").min().over(group_cols).alias("_t_min"),
        pl.col("elapsed_ns").max().over(group_cols).alias("_t_max"),
    )
    df = df.with_columns(
        (pl.col("_t_min") + (pl.col("_t_max") - pl.col("_t_min")) * trim_frac).alias(
            "_t_lo"
        ),
        (pl.col("_t_max") - (pl.col("_t_max") - pl.col("_t_min")) * trim_frac).alias(
            "_t_hi"
        ),
    )
    df = df.filter(
        (pl.col("elapsed_ns") >= pl.col("_t_lo"))
        & (pl.col("elapsed_ns") <= pl.col("_t_hi"))
    )
    return df.drop(["_t_min", "_t_max", "_t_lo", "_t_hi"])


def aggregate_benchkv_total_rps(
    df: pl.DataFrame, group_cols: list[str]
) -> pl.DataFrame:
    """per-batch RPS -> summary (mean_rps, std_rps) per group."""
    # per-client mean RPS
    per_client = df.group_by(group_cols + ["run_index", "rank", "client_id"]).agg(
        pl.col("rps").mean().alias("client_rps")
    )
    # per-rank total = sum of client means
    per_rank = per_client.group_by(group_cols + ["run_index", "rank"]).agg(
        pl.col("client_rps").sum().alias("rank_rps")
    )
    per_run = (
        per_rank.filter(pl.col("rank") == 0)
        .with_columns((pl.col("rank_rps") * pl.col("np")).alias("total_rps"))
        .select(group_cols + ["run_index", "total_rps"])
    )
    summary = (
        per_run.group_by(group_cols)
        .agg(
            pl.col("total_rps").mean().alias("mean_rps"),
            pl.col("total_rps").std().alias("std_rps"),
        )
        .sort(group_cols)
    )
    summary = summary.with_columns(
        (pl.col("mean_rps") / 1e6).alias("mean_miops"),
        (pl.col("std_rps") / 1e6).alias("std_miops"),
    )
    return summary.with_columns(
        (pl.col("mean_miops") - pl.col("std_miops").fill_null(0))
        .clip(lower_bound=1e-6)
        .alias("miops_lo"),
        (pl.col("mean_miops") + pl.col("std_miops").fill_null(0)).alias("miops_hi"),
    )


def make_line_chart(
    pdf,
    x_field: str,
    x_title: str,
    y_field: str,
    y_title: str,
    color_field: str,
    color_title: str,
    color_domain: list[str],
    color_range: list[str],
    *,
    has_error_band: bool = True,
    lo_field: str = "lo",
    hi_field: str = "hi",
    x_log_base: int = 2,
    stroke_dash: bool = True,
):
    """Shared chart builder."""
    color_scale = alt.Scale(domain=color_domain, range=color_range)
    color_enc = alt.Color(
        f"{color_field}:N", title=color_title, scale=color_scale, sort=color_domain
    )

    encodings = dict(
        x=alt.X(
            f"{x_field}:Q", title=x_title, scale=alt.Scale(type="log", base=x_log_base)
        ),
        y=alt.Y(f"{y_field}:Q", title=y_title),
        color=color_enc,
    )
    if stroke_dash:
        encodings["strokeDash"] = alt.StrokeDash(f"{color_field}:N", sort=color_domain)

    line = (
        alt.Chart(pdf).mark_line(point=alt.OverlayMarkDef(size=60)).encode(**encodings)
    )

    layers = [line]
    if has_error_band:
        band = (
            alt.Chart(pdf)
            .mark_area(opacity=0.15)
            .encode(
                x=f"{x_field}:Q",
                y=f"{lo_field}:Q",
                y2=f"{hi_field}:Q",
                color=color_enc,
            )
        )
        layers.insert(0, band)

    chart = (
        alt.layer(*layers)
        .properties(width=400, height=280)
        .configure_axis(labelFontSize=12, titleFontSize=13)
    )
    return chart


def plot_benchkv_throughput(df_rps: pl.DataFrame):
    """Chart 1: node count vs total RPS (UCX / direct / proxied)."""
    # delegation: batch_hold_us == 0 only; others: batch_hold_us is null
    df = df_rps.filter(
        (pl.col("mode") != "delegation") | (pl.col("batch_hold_us") == 0.0)
    )
    summary = aggregate_benchkv_total_rps(df, ["mode", "np"])

    labels = {
        "ucx_am": "UCX",
        "copyrpc_direct": "direct connection",
        "delegation": "proxied connection",
    }
    summary = summary.with_columns(
        pl.col("mode").replace_strict(labels, default=pl.col("mode")).alias("backend")
    )

    domain = ["UCX", "direct connection", "proxied connection"]
    colors = ["#d62728", "#ff7f0e", "#1f77b4"]

    pdf = summary.to_pandas()
    has_band = pdf["std_miops"].notna().any()

    chart = make_line_chart(
        pdf,
        x_field="np",
        x_title="Number of nodes",
        y_field="mean_miops",
        y_title="Total throughput (MIOPS)",
        color_field="backend",
        color_title="Backend",
        color_domain=domain,
        color_range=colors,
        has_error_band=has_band,
        lo_field="miops_lo",
        hi_field="miops_hi",
    )
    chart.save(str(OUT_DIR / "benchkv_throughput.png"), scale_factor=2)

    csv_df = summary.select(["backend", "np", "mean_miops", "std_miops"])
    csv_df.write_csv(OUT_DIR / "benchkv_throughput.csv")
    print(f"  -> benchkv_throughput.png / .csv")


def plot_benchkv_batch_hold(df_rps: pl.DataFrame):
    """Chart 2: delegation batch hold comparison."""
    df = df_rps.filter(pl.col("mode") == "delegation")
    summary = aggregate_benchkv_total_rps(df, ["mode", "np", "batch_hold_us"])

    summary = summary.with_columns(
        pl.when(pl.col("batch_hold_us") == 0.0)
        .then(pl.lit("no batch hold"))
        .otherwise(pl.lit("batch hold (10\u00b5s)"))
        .alias("variant")
    )

    domain = ["no batch hold", "batch hold (10\u00b5s)"]
    colors = ["#1f77b4", "#ff7f0e"]

    pdf = summary.to_pandas()
    has_band = pdf["std_miops"].notna().any()

    chart = make_line_chart(
        pdf,
        x_field="np",
        x_title="Number of nodes",
        y_field="mean_miops",
        y_title="Total throughput (MIOPS)",
        color_field="variant",
        color_title="Variant",
        color_domain=domain,
        color_range=colors,
        has_error_band=has_band,
        lo_field="miops_lo",
        hi_field="miops_hi",
    )
    chart.save(str(OUT_DIR / "benchkv_batch_hold.png"), scale_factor=2)

    csv_df = summary.select(["variant", "np", "mean_miops", "std_miops"])
    csv_df.write_csv(OUT_DIR / "benchkv_batch_hold.csv")
    print(f"  -> benchkv_batch_hold.png / .csv")


def plot_rpc_bench_throughput(df: pl.DataFrame):
    """Chart 3: QP count vs RPS."""
    med = df.group_by(["system", "endpoints", "inflight_per_ep"]).agg(
        (pl.col("rps").median() / 1e6).alias("median_mrps"),
    )
    med = med.with_columns(
        (
            pl.col("system") + " (QD=" + pl.col("inflight_per_ep").cast(pl.Utf8) + ")"
        ).alias("label")
    )
    med = med.sort(["label", "endpoints"])

    labels_sorted = sorted(med["label"].unique().to_list())
    palette = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b"]
    colors = palette[: len(labels_sorted)]

    color_scale = alt.Scale(domain=labels_sorted, range=colors)

    chart = (
        alt.Chart(med.to_pandas())
        .mark_line(point=alt.OverlayMarkDef(size=60))
        .encode(
            x=alt.X(
                "endpoints:Q",
                title="Number of QPs",
                scale=alt.Scale(type="log", base=2),
            ),
            y=alt.Y("median_mrps:Q", title="Throughput (Mrps)"),
            color=alt.Color(
                "label:N", title="System", scale=color_scale, sort=labels_sorted
            ),
            strokeDash=alt.StrokeDash("label:N", sort=labels_sorted),
        )
        .properties(width=400, height=280)
        .configure_axis(labelFontSize=12, titleFontSize=13)
    )
    chart.save(str(OUT_DIR / "rpc_bench_throughput.png"), scale_factor=2)

    csv_df = med.select(["label", "endpoints", "median_mrps"])
    csv_df.write_csv(OUT_DIR / "rpc_bench_throughput.csv")
    print(f"  -> rpc_bench_throughput.png / .csv")


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # --- benchkv ---
    benchkv_path = RESULT_DIR / "benchkv" / "benchkv_unified.parquet"
    if benchkv_path.exists():
        print("Loading benchkv data...")
        df_bkv = pl.read_parquet(benchkv_path)
        df_rps = compute_benchkv_rps(df_bkv)
        plot_benchkv_throughput(df_rps)
        plot_benchkv_batch_hold(df_rps)
    else:
        print(f"benchkv data not found: {benchkv_path}")

    # --- rpc_bench ---
    rpc_path = RESULT_DIR / "rpc_bench" / "rpc_bench_unified.parquet"
    if rpc_path.exists():
        print("Loading rpc_bench data...")
        df_rpc = pl.read_parquet(rpc_path)
        plot_rpc_bench_throughput(df_rpc)
    else:
        print(f"rpc_bench data not found: {rpc_path}")

    print("Done!")


if __name__ == "__main__":
    main()
