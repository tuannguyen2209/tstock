from FiinQuantX import FiinSession
import matplotlib.pyplot as plt
import pandas as pd

username = 'DNSE_FG_164@fiinquant.vn'
password = 'DNSE_FG_FiinQuant_@0@6'

client = FiinSession(
    username=username,
    password=password,
).login()

data = client.MarketDepth().get_stock_valuation(
    tickers=["VCB","BID","CTG"],
    from_date="2020-08-28",
    to_date="2025-09-03"
)
df_pb = data.pivot(index='timestamp', columns='ticker', values='pb')

df_pb.index = pd.to_datetime(df_pb.index)

plt.figure(figsize=(12, 6))
df_pb.plot(kind='line', linewidth=1.5, figsize=(12, 6), ax=plt.gca())

plt.title('Biến động chỉ số P/B của VCB, BID, CTG (2020 - 2025)', fontsize=14, fontweight='bold')
plt.xlabel('Ngày', fontsize=12)
plt.ylabel('Chỉ số P/B', fontsize=12)
plt.grid(True, linestyle='--', alpha=0.6)
plt.legend(title='Mã cổ phiếu', loc='best')
plt.tight_layout()

plt.show()