import duckdb

con = duckdb.connect(r'F:\Projects\Olist\duckdb\portfolio.duckdb', read_only=True)

print("\n=== WATERFALL BY CHANNEL ===")
print(con.execute("""
    SELECT channel,
           ROUND(AVG(price_realization_pct),1) AS realization_pct,
           ROUND(AVG(trade_disc_pct),1)        AS trade_disc_pct,
           ROUND(SUM(pocket_revenue),0)        AS pocket_revenue
    FROM refined.rfn_pocket_price_waterfall
    WHERE year = 2018
    GROUP BY channel
    ORDER BY realization_pct DESC
""").df().to_string())

print("\n=== ROW COUNTS ===")
print(con.execute("""
    SELECT year, COUNT(*) AS rows, ROUND(SUM(net_revenue),0) AS net_revenue
    FROM staging.stg_transactions
    GROUP BY year ORDER BY year
""").df().to_string())

con.close()