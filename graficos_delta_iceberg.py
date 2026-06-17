import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import os

# Ajusta esta ruta al CSV que has generado con el benchmark Delta vs Iceberg
CSV_PATH = "./jobs/delta_vs_iceberg_20260521_183745.csv"
OUT_DIR = "./outputs"
os.makedirs(OUT_DIR, exist_ok=True)

df = pd.read_csv(CSV_PATH)

# ── Nombres consistentes para los gráficos ─────────────────────────────────
FORMATS = ["Delta Lake", "Iceberg"]
FORMAT_LABELS = ["Delta Lake", "Iceberg"]
COLORS_FORMAT = {"Delta Lake": "#DF2424", "Iceberg": "#00AFE9"}
ENGINE_NAMES = {"Spark SQL + Delta": "Spark SQL + Delta", "Spark SQL + Iceberg": "Spark SQL + Iceberg"}

TIERS = ["players_30k", "players_500k", "players_full"]
TIER_LABELS = ["30k", "389k", "1.66M"]

# ── Configuración visual ──────────────────────────────────────────────────
plt.rcParams.update({
    "font.family":      "serif",
    "font.size":        10,
    "axes.titlesize":   12,
    "axes.titleweight": "bold",
    "axes.labelsize":   10,
    "axes.spines.top":  False,
    "axes.spines.right":False,
    "axes.grid":        True,
    "grid.alpha":       0.3,
    "grid.linestyle":   "--",
    "legend.framealpha":0.9,
    "legend.fontsize":  9,
})

# ── Gráfico 1: Latencia media agregada por formato y tier ─────────────────
fig, ax = plt.subplots(figsize=(8, 4.5))

avg_by = df.groupby(["format", "tier"])["avg_ms"].mean().reset_index()

n_tiers   = len(TIERS)
n_formats = len(FORMATS)
width     = 0.3
x         = np.arange(n_tiers)

for i, fmt in enumerate(FORMATS):
    vals = []
    for t in TIERS:
        row = avg_by[(avg_by["format"] == fmt) & (avg_by["tier"] == t)]
        vals.append(row["avg_ms"].values[0] if len(row) else 0)
    offset = (i - 0.5) * (width + 0.05)
    bars = ax.bar(x + offset, vals, width, label=FORMAT_LABELS[i],
                  color=COLORS_FORMAT[fmt], edgecolor="white", linewidth=0.5)
    for bar, v in zip(bars, vals):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 4,
                f"{v:.0f}", ha="center", va="bottom", fontsize=7.5)

ax.set_xticks(x)
ax.set_xticklabels(TIER_LABELS)
ax.set_xlabel("Volumen de datos")
ax.set_ylabel("Latencia media (ms)")
ax.set_title("Latencia media agregada por formato y volumen")
ax.legend(title="Formato")
ax.set_ylim(0, ax.get_ylim()[1] * 1.15)
fig.tight_layout()
fig.savefig(f"{OUT_DIR}/grafico1_delta_vs_iceberg_agregado.pdf", format="pdf", bbox_inches="tight")
plt.close()
print("Gráfico 1 OK")

# ── Gráfico 2: Evolución de consultas clave ────────────────────────────────
QUERIES_SEL = {
    "Q1_count":       "Q1 — COUNT(*)",
    "Q3_aggregation": "Q3 — GROUP BY",
    "Q4_window":      "Q4 — Ventana",
    "Q5_multi_agg":   "Q5 — Agr. multicolumna",
    "Q_skip_team":    "Q_skip_team — Data skipping",
}

fig, axes = plt.subplots(2, 3, figsize=(12, 7), sharey=False)
axes = axes.flatten()
# La última subparcela (axes[5]) quedará vacía; la eliminamos para dejar 5.
fig.delaxes(axes[5])

for ax, (qid, qlabel) in zip(axes[:5], QUERIES_SEL.items()):
    sub = df[df["query_id"] == qid]
    for fmt in FORMATS:
        vals = []
        for t in TIERS:
            row = sub[(sub["format"] == fmt) & (sub["tier"] == t)]
            vals.append(row["avg_ms"].values[0] if len(row) else np.nan)
        ax.plot(TIER_LABELS, vals, marker="o", label=fmt,
                color=COLORS_FORMAT[fmt], linewidth=1.8, markersize=5)
        for xi, v in enumerate(vals):
            if not np.isnan(v):
                ax.annotate(f"{v:.0f}", (xi, v),
                            textcoords="offset points", xytext=(0, 6),
                            ha="center", fontsize=7.5)
    ax.set_title(qlabel)
    ax.set_xlabel("Volumen")
    ax.set_ylabel("ms")
    ax.legend(fontsize=8)

fig.suptitle("Evolución de latencia por consulta al escalar el volumen", fontsize=12, fontweight="bold")
fig.tight_layout()
fig.savefig(f"{OUT_DIR}/grafico2_delta_vs_iceberg_evolucion.pdf", format="pdf", bbox_inches="tight")
plt.close()
print("Gráfico 2 OK")

# ── Gráfico 3: Data skipping en detalle ────────────────────────────────────
fig, axes = plt.subplots(1, 2, figsize=(10, 4.5), sharey=False)

for ax, (qid, qlabel) in zip(axes, [
    ("Q_skip_team",   "Q_skip_team — filtro por equipo\n(Boston Celtics)"),
    ("Q_skip_player", "Q_skip_player — filtro por jugador\n(personId = 2544)"),
]):
    sub = df[df["query_id"] == qid]
    x = np.arange(len(TIERS))
    for i, fmt in enumerate(FORMATS):
        vals = []
        for t in TIERS:
            row = sub[(sub["format"] == fmt) & (sub["tier"] == t)]
            vals.append(row["avg_ms"].values[0] if len(row) else np.nan)
        offset = (i - 0.5) * (width + 0.04)
        bars = ax.bar(x + offset, vals, width + 0.04, label=fmt,
                      color=COLORS_FORMAT[fmt], edgecolor="white", linewidth=0.5)
        for bar, v in zip(bars, vals):
            if not np.isnan(v):
                ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 3,
                        f"{v:.0f}", ha="center", va="bottom", fontsize=8)
    ax.set_xticks(x)
    ax.set_xticklabels(TIER_LABELS)
    ax.set_xlabel("Volumen de datos")
    ax.set_ylabel("Latencia media (ms)")
    ax.set_title(qlabel)
    ax.legend(title="Formato")
    ax.set_ylim(0, ax.get_ylim()[1] * 1.18)

fig.suptitle("Data skipping: Delta Lake vs Iceberg", fontsize=11, fontweight="bold")
fig.tight_layout()
fig.savefig(f"{OUT_DIR}/grafico3_delta_vs_iceberg_data_skipping.pdf", format="pdf", bbox_inches="tight")
plt.close()
print("Gráfico 3 OK")

print(f"\nTodos los PDFs generados en {OUT_DIR}")