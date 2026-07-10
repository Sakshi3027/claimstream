"""
ClaimStream — Analytics Dashboard
Reads Gold Delta Lake tables and generates visual analytics
"""

from pyspark.sql import SparkSession
from delta import *
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import os

os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17"

builder = SparkSession.builder \
    .appName("ClaimStream-Dashboard") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:4.3.1") \
    .config("spark.driver.bindAddress", "127.0.0.1")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("✅ Loading Gold tables...")

# Load all gold tables
payer_df = spark.read.format("delta").load("data/delta/gold/denial_by_payer").toPandas()
cpt_df = spark.read.format("delta").load("data/delta/gold/denial_by_cpt").toPandas()
reason_df = spark.read.format("delta").load("data/delta/gold/denial_by_reason").toPandas()
monthly_df = spark.read.format("delta").load("data/delta/gold/monthly_trends").toPandas()

spark.stop()

# ─────────────────────────────────────────
# BUILD DASHBOARD
# ─────────────────────────────────────────
fig = plt.figure(figsize=(18, 12))
fig.suptitle("ClaimStream — Healthcare Claims Analytics Dashboard", 
             fontsize=18, fontweight='bold', y=0.98)

gs = gridspec.GridSpec(2, 2, figure=fig, hspace=0.4, wspace=0.35)

# Chart 1 — Denial Rate by Payer
ax1 = fig.add_subplot(gs[0, 0])
colors = ['#e74c3c' if r > 35 else '#f39c12' if r > 25 else '#27ae60' 
          for r in payer_df['denial_rate_pct']]
bars = ax1.barh(payer_df['payer'], payer_df['denial_rate_pct'], color=colors)
ax1.set_xlabel('Denial Rate (%)')
ax1.set_title('Denial Rate by Payer', fontweight='bold')
ax1.axvline(x=payer_df['denial_rate_pct'].mean(), color='gray', 
            linestyle='--', alpha=0.7, label=f"Avg: {payer_df['denial_rate_pct'].mean():.1f}%")
ax1.legend(fontsize=8)
for bar, val in zip(bars, payer_df['denial_rate_pct']):
    ax1.text(bar.get_width() + 0.5, bar.get_y() + bar.get_height()/2,
             f'{val:.1f}%', va='center', fontsize=9)

# Chart 2 — Top Denial Reasons
ax2 = fig.add_subplot(gs[0, 1])
reason_short = reason_df.copy()
reason_short['denial_reason'] = reason_short['denial_reason'].str[:30]
ax2.barh(reason_short['denial_reason'], reason_short['total_denials'], 
         color='#3498db')
ax2.set_xlabel('Number of Denials')
ax2.set_title('Top Denial Reasons', fontweight='bold')
ax2.invert_yaxis()

# Chart 3 — Denial Rate by CPT Code
ax3 = fig.add_subplot(gs[1, 0])
cpt_df_sorted = cpt_df.sort_values('denial_rate_pct', ascending=True)
ax3.barh(cpt_df_sorted['cpt_description'].str[:25], 
         cpt_df_sorted['denial_rate_pct'], color='#9b59b6')
ax3.set_xlabel('Denial Rate (%)')
ax3.set_title('Denial Rate by Procedure (CPT)', fontweight='bold')

# Chart 4 — Monthly Trends
ax4 = fig.add_subplot(gs[1, 1])
ax4.plot(monthly_df['claim_month'], monthly_df['total_claims'], 
         'b-o', label='Total Claims', linewidth=2)
ax4.plot(monthly_df['claim_month'], monthly_df['denied_claims'], 
         'r-o', label='Denied Claims', linewidth=2)
ax4.set_xlabel('Month')
ax4.set_ylabel('Count')
ax4.set_title('Monthly Claims Trend', fontweight='bold')
ax4.legend()
ax4.tick_params(axis='x', rotation=45)
ax4.grid(True, alpha=0.3)

os.makedirs("docs", exist_ok=True)
plt.savefig("docs/dashboard.png", dpi=150, bbox_inches='tight',
            facecolor='white', edgecolor='none')
print("✅ Dashboard saved to docs/dashboard.png")
plt.show()
