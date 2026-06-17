import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import os

OUT = "./outputs"
os.makedirs(OUT, exist_ok=True)

df = pd.read_csv("./jobs/scalability_results_20260521_172036.csv")

# ── Estilo general ──────────────────────────────────────────────────────────
COLORS = {"Trino": "#000033", "Spark SQL": "#E35A1D", "Pandas": "#F6B935"}
ENGINES = ["Trino", "Spark SQL", "Pandas"]
TIERS   = ["Tier 1 — 30k", "Tier 2 — 389k", "Tier 3 — 1.66M"]
TIER_LABELS = ["30k", "389k", "1.66M"]

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

# ── Gráfico 1: Latencia media agregada por engine y tier ───────────────────
fig, ax = plt.subplots(figsize=(8, 4.5))

avg_by = df.groupby(["engine", "tier"])["avg_ms"].mean().reset_index()

n_tiers   = len(TIERS)
n_engines = len(ENGINES)
width     = 0.22
x         = np.arange(n_tiers)

for i, eng in enumerate(ENGINES):
    vals = []
    for t in TIERS:
        row = avg_by[(avg_by["engine"] == eng) & (avg_by["tier"] == t)]
        vals.append(row["avg_ms"].values[0] if len(row) else 0)
    offset = (i - 1) * (width + 0.02)
    bars = ax.bar(x + offset, vals, width, label=eng,
                  color=COLORS[eng], edgecolor="white", linewidth=0.5)
    for bar, v in zip(bars, vals):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 4,
                f"{v:.0f}", ha="center", va="bottom", fontsize=7.5)

ax.set_xticks(x)
ax.set_xticklabels(TIER_LABELS)
ax.set_xlabel("Volumen de datos")
ax.set_ylabel("Latencia media (ms)")
ax.set_title("Latencia media agregada por motor y volumen")
ax.legend(title="Motor")
ax.set_ylim(0, ax.get_ylim()[1] * 1.12)
fig.tight_layout()
fig.savefig(f"{OUT}/grafico1_latencia_media.pdf", format="pdf", bbox_inches="tight")
plt.close()
print("Gráfico 1 OK")

# ── Gráfico 2: Evolución por consulta al escalar (líneas) ─────────────────
QUERIES_SEL = {
    "Q1_count":    "Q1 — COUNT(*)",
    "Q3_aggregation": "Q3 — GROUP BY",
    "Q4_window":   "Q4 — Ventana",
    "Q5_multi_agg":"Q5 — Agr. multicolumna",
}

fig, axes = plt.subplots(2, 2, figsize=(10, 6.5), sharey=False)
axes = axes.flatten()

for ax, (qid, qlabel) in zip(axes, QUERIES_SEL.items()):
    sub = df[df["query_id"] == qid]
    for eng in ENGINES:
        vals = []
        for t in TIERS:
            row = sub[(sub["engine"] == eng) & (sub["tier"] == t)]
            vals.append(row["avg_ms"].values[0] if len(row) else np.nan)
        ax.plot(TIER_LABELS, vals, marker="o", label=eng,
                color=COLORS[eng], linewidth=1.8, markersize=5)
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
fig.savefig(f"{OUT}/grafico2_evolucion_consultas.pdf", format="pdf", bbox_inches="tight")
plt.close()
print("Gráfico 2 OK")

# ── Gráfico 3: Q4_window detalle con barras de error ─────────────────────
fig, ax = plt.subplots(figsize=(8, 4.5))

sub = df[df["query_id"] == "Q4_window"]
x   = np.arange(n_tiers)

for i, eng in enumerate(ENGINES):
    avgs, mins, maxs = [], [], []
    for t in TIERS:
        row = sub[(sub["engine"] == eng) & (sub["tier"] == t)]
        if len(row):
            avgs.append(row["avg_ms"].values[0])
            mins.append(row["avg_ms"].values[0] - row["min_ms"].values[0])
            maxs.append(row["max_ms"].values[0] - row["avg_ms"].values[0])
        else:
            avgs.append(np.nan); mins.append(0); maxs.append(0)
    offset = (i - 1) * (width + 0.02)
    yerr = [mins, maxs]
    bars = ax.bar(x + offset, avgs, width, label=eng,
                  color=COLORS[eng], edgecolor="white", linewidth=0.5,
                  yerr=yerr, capsize=4, error_kw={"elinewidth":1.2, "ecolor":"#444"})
    for bar, v in zip(bars, avgs):
        if not np.isnan(v):
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(maxs)*0.05 + 15,
                    f"{v:.0f}", ha="center", va="bottom", fontsize=7.5)

ax.set_xticks(x)
ax.set_xticklabels(TIER_LABELS)
ax.set_xlabel("Volumen de datos")
ax.set_ylabel("Latencia (ms)")
ax.set_title("Q4 — Función de ventana: latencia y variabilidad por motor")
ax.legend(title="Motor")
ax.set_ylim(0, ax.get_ylim()[1] * 1.15)
fig.tight_layout()
fig.savefig(f"{OUT}/grafico3_q4_ventana.pdf", format="pdf", bbox_inches="tight")
plt.close()
print("Gráfico 3 OK")

# ── Gráfico 4: Data skipping ───────────────────────────────────────────────
fig, axes = plt.subplots(1, 2, figsize=(10, 4.5), sharey=False)

for ax, (qid, qlabel) in zip(axes, [
    ("Q_skip_team",   "Q_skip_team — filtro por equipo\n(Boston Celtics)"),
    ("Q_skip_player", "Q_skip_player — filtro por jugador\n(personId = 2544)"),
]):
    sub = df[df["query_id"] == qid]
    x   = np.arange(n_tiers)
    for i, eng in enumerate(["Trino", "Spark SQL"]):  # Pandas no hace data skipping real
        vals = []
        for t in TIERS:
            row = sub[(sub["engine"] == eng) & (sub["tier"] == t)]
            vals.append(row["avg_ms"].values[0] if len(row) else np.nan)
        offset = (i - 0.5) * (width + 0.04)
        bars = ax.bar(x + offset, vals, width + 0.04, label=eng,
                      color=COLORS[eng], edgecolor="white", linewidth=0.5)
        for bar, v in zip(bars, vals):
            if not np.isnan(v):
                ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 3,
                        f"{v:.0f}", ha="center", va="bottom", fontsize=8)
    ax.set_xticks(x)
    ax.set_xticklabels(TIER_LABELS)
    ax.set_xlabel("Volumen de datos")
    ax.set_ylabel("Latencia media (ms)")
    ax.set_title(qlabel)
    ax.legend(title="Motor")
    ax.set_ylim(0, ax.get_ylim()[1] * 1.18)

fig.suptitle("Efecto del data skipping de Iceberg — latencia decrece al aumentar el volumen",
             fontsize=11, fontweight="bold")
fig.tight_layout()
fig.savefig(f"{OUT}/grafico4_data_skipping.pdf", format="pdf", bbox_inches="tight")
plt.close()
print("Gráfico 4 OK")

print("\nTodos los PDFs generados en", OUT)