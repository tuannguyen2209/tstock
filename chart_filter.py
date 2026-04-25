import pandas as pd
from FiinQuantX import FiinSession

# Load từ biến môi trường hoặc file .env thay vì hardcode nhé!
username = 'DNSE_FG_164@fiinquant.vn'
password = 'DNSE_FG_FiinQuant_@0@6'

client = FiinSession(username=username, password=password).login()
pa = client.Pattern()

# Danh sách ví dụ (Bạn có thể nạp đủ 100 mã VN100 vào đây)
vn100_tickers = client.TickerList(ticker="VNINDEX")

print("Đang tải dữ liệu...")
df_all = client.Fetch_Trading_Data(
    realtime=False,
    tickers=vn100_tickers,
    fields=["high", "low", "close"], # ĐÃ SỬA: Xóa chữ "ticker" ở đây
    adjusted=True,
    by="1d",
    from_date="2024-01-01",
    to_date="2024-12-31"
).get_data()

# ---------------------------------------------------------
# BƯỚC QUAN TRỌNG: Kiểm tra cấu trúc DataFrame trả về
# ---------------------------------------------------------
# Đôi khi API giấu mã cổ phiếu trong Index (MultiIndex), ta reset nó ra thành cột cho dễ xử lý
df_all = df_all.reset_index() 

print("Các cột hiện có trong Data:", df_all.columns.tolist())
print(df_all.head())

# Giả sử sau khi in ra, bạn thấy cột chứa mã cổ phiếu tên là 'ticker' 
# (Nếu nó tên là 'symbol' hay 'sec_code' thì bạn đổi lại chữ 'ticker' bên dưới cho đúng nhé)
col_name = 'ticker' 

detected_tickers = []
results_dict = {}

print("Đang phân tích mẫu hình Vai-Đầu-Vai...")

# Group by theo đúng tên cột
if col_name in df_all.columns:
    for ticker, df_ticker in df_all.groupby(col_name):
        df_temp = df_ticker.copy()
        
        # Bỏ qua các dòng bị lỗi dữ liệu (NaN) để tránh hàm detect bị crash
        df_temp = df_temp.dropna(subset=['high', 'low', 'close'])
        
        if not df_temp.empty:
            df_temp["head_shoulder"] = pa.detect_head_shoulder(df_temp)
            
            # Lọc tín hiệu trong 5 phiên gần nhất
            if df_temp["head_shoulder"].tail(5).any():
                detected_tickers.append(ticker)
                results_dict[ticker] = df_temp[df_temp["head_shoulder"] == True]

    print("-" * 50)
    if detected_tickers:
        print(f"CẢNH BÁO: Phát hiện mẫu hình Vai-Đầu-Vai tại: {', '.join(detected_tickers)}")
    else:
        print("Không phát hiện mã nào có mẫu hình Vai-Đầu-Vai trong thời gian gần đây.")
else:
    print(f"LỖI: Không tìm thấy cột '{col_name}'. Hãy xem dữ liệu in ra ở trên để tìm đúng tên cột định danh mã cổ phiếu!")