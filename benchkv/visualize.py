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

import polars as pl
import altair as alt
from pathlib import Path
import re

RESULT_DIR = Path(__file__).parent / "result"
OUT_DIR = Path(__file__).parent / "figures"
OUT_DIR.mkdir(exist_ok=True)

# ---------- Load all parquet files ----------
frames = []
for p in sorted(RESULT_DIR.glob("*.parquet")):
    m = re.match(r"(.+)_np(\d+)\.parquet", p.name)
    if not m:
        continue
    np_val = int(m.group(2))
    df = pl.read_parquet(p).with_columns(pl.lit(np_val).alias("np").cast(pl.Int32))
    frames.append(df)

df_all = pl.concat(frames)
print(f"Loaded {len(frames)} files, {df_all.height} rows")
print("Unique modes:", df_all["mode"].unique().to_list())

# ---------- Compute RPS from batch diffs ----------
# Sort by (mode, np, rank, client_id, run_index, batch_index)
df_all = df_all.sort(["mode", "np", "rank", "client_id", "run_index", "batch_index"])

# Compute inter-batch time delta per client stream
df_all = df_all.with_columns(
    pl.col("elapsed_ns")
    .diff()
    .over(["mode", "np", "rank", "client_id", "run_index"])
    .alias("delta_ns")
)

# Compute per-batch RPS (batch_size / delta_ns * 1e9)
df_all = df_all.with_columns(
    (pl.col("batch_size").cast(pl.Float64) * 1e9 / pl.col("delta_ns")).alias("rps")
)

# Drop first batch per stream (delta_ns is null)
df_valid = df_all.filter(pl.col("delta_ns").is_not_null() & (pl.col("delta_ns") > 0))

# ---------- Aggregate ----------
# Step 1: per (mode, np, run_index, rank) — sum RPS across clients for per-rank total
per_rank = (
    df_valid.group_by(["mode", "np", "run_index", "rank"])
    .agg(pl.col("rps").mean().alias("rank_rps"))
)

# Step 2: per (mode, np, run_index) — rank 0 RPS, estimated total = rank0 * np
per_run = (
    per_rank.filter(pl.col("rank") == 0)
    .with_columns(
        (pl.col("rank_rps") * pl.col("np")).alias("total_rps"),
    )
    .select(["mode", "np", "run_index", "rank_rps", "total_rps"])
)

# Step 3: per (mode, np) — mean & std over runs
summary = (
    per_run.group_by(["mode", "np"])
    .agg(
        pl.col("total_rps").mean().alias("mean_rps"),
        pl.col("total_rps").std().alias("std_rps"),
        pl.col("total_rps").min().alias("min_rps"),
        pl.col("total_rps").max().alias("max_rps"),
    )
    .sort(["mode", "np"])
)

print("\n=== Summary (estimated total RPS = rank0 × np) ===")
print(summary)

# Error band bounds (clamp lo >= 1 for log scale)
summary = summary.with_columns(
    (pl.col("mean_rps") - pl.col("std_rps")).clip(lower_bound=1).alias("rps_lo"),
    (pl.col("mean_rps") + pl.col("std_rps")).alias("rps_hi"),
)

# Pretty mode names
MODE_LABELS = {
    "agg": "Agg (copyrpc+ipc+Flux)",
    "copyrpc_direct": "copyrpc (direct)",
    "ucx_am": "UCX Active Message",
}
summary = summary.with_columns(
    pl.col("mode").replace_strict(MODE_LABELS, default=pl.col("mode")).alias("backend"),
)

pdf = summary.to_pandas()

# Consistent colors
backend_order = ["Agg (copyrpc+ipc+Flux)", "copyrpc (direct)", "UCX Active Message"]
color_scale = alt.Scale(
    domain=backend_order,
    range=["#1f77b4", "#ff7f0e", "#d62728"],
)

# ---------- Chart: Total throughput (log-log) ----------
line = (
    alt.Chart(pdf)
    .mark_line(point=alt.OverlayMarkDef(size=60))
    .encode(
        x=alt.X("np:Q", title="Number of Nodes", scale=alt.Scale(type="log", base=2)),
        y=alt.Y("mean_rps:Q", title="Estimated Total Throughput (RPS)", scale=alt.Scale(type="log")),
        color=alt.Color("backend:N", title="Backend", scale=color_scale, sort=backend_order),
        strokeDash=alt.StrokeDash("backend:N", sort=backend_order),
    )
)

band = (
    alt.Chart(pdf)
    .mark_area(opacity=0.15)
    .encode(
        x="np:Q",
        y="rps_lo:Q",
        y2="rps_hi:Q",
        color=alt.Color("backend:N", title="Backend", scale=color_scale, sort=backend_order),
    )
)

chart_total = (
    (band + line)
    .properties(
        width=560,
        height=380,
        title="benchkv: Estimated Total Throughput (rank0 × np)",
    )
    .configure_axis(labelFontSize=12, titleFontSize=13)
    .configure_title(fontSize=15)
)

chart_total.save(str(OUT_DIR / "throughput_total.png"), scale_factor=2)
print(f"\nSaved: {OUT_DIR / 'throughput_total.png'}")

# ---------- Chart: Per-run scatter (log-log) ----------
per_run_pdf = (
    per_run.with_columns(
        pl.col("mode")
        .replace_strict(MODE_LABELS, default=pl.col("mode"))
        .alias("backend"),
    )
    .to_pandas()
)

chart_scatter = (
    alt.Chart(per_run_pdf)
    .mark_circle(size=60, opacity=0.7)
    .encode(
        x=alt.X("np:Q", title="Number of Nodes", scale=alt.Scale(type="log", base=2)),
        y=alt.Y(
            "total_rps:Q",
            title="Estimated Total Throughput (RPS)",
            scale=alt.Scale(type="log"),
        ),
        color=alt.Color("backend:N", title="Backend", scale=color_scale, sort=backend_order),
        tooltip=["backend", "np", "run_index", "total_rps"],
    )
    .properties(width=560, height=380, title="benchkv: Per-Run Total Throughput (all runs)")
    .configure_axis(labelFontSize=12, titleFontSize=13)
    .configure_title(fontSize=15)
)

chart_scatter.save(str(OUT_DIR / "throughput_scatter.png"), scale_factor=2)
print(f"Saved: {OUT_DIR / 'throughput_scatter.png'}")

print("\nDone!")
