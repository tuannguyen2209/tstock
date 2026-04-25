import pandas as pd
import numpy as np
from FiinQuantX import FiinSession
from datetime import datetime, timedelta

# 1. Khởi tạo và Đăng nhập (Code của bạn)
username = 'DNSE_FG_164@fiinquant.vn'
password = 'DNSE_FG_FiinQuant_@0@6'
client = FiinSession(username=username, password=password).login()

# Lấy danh sách VN100 (FiinQuant trả về list các tickers)
# Lưu ý: Hàm TickerList của FiinQuant có thể cần format đúng, ở đây giả định trả về list ['SSI', 'HPG',...]
vn100_tickers = client.TickerList(ticker="VN100") 

end_date = datetime.now().strftime("%Y-%m-%d")
start_date = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")

# 2. Tạo danh sách chứa kết quả
buyable_stocks = []

print(f"Đang quét {len(vn100_tickers)} mã cổ phiếu để tìm tín hiệu Wyckoff...")

# 3. Vòng lặp phân tích từng mã
for ticker in vn100_tickers:
    try:
        # Fetch dữ liệu OHLCV (Sử dụng hàm theo doc của FiinQuant)
        df = client.Fetch_Trading_Data(
            realtime=False,
            tickers=[ticker],
            fields=["open", "high", "low", "close", "volume"], # Chỉ lấy OHLCV
            adjusted=True,
            by="1d",
            from_date=start_date,
            to_date=end_date
            ).get_data()

        # Bỏ qua nếu dữ liệu lỗi hoặc quá ngắn không đủ tính MA
        if df.empty or len(df) < 50:
            continue
            
        # --- TÍNH TOÁN CÁC CHỈ SỐ LÀM NỀN TẢNG ---
        # Tính Trung bình động (MA) cho Xu hướng
        df['SMA_20'] = df['close'].rolling(window=20).mean()
        df['SMA_50'] = df['close'].rolling(window=50).mean()
        
        # Tính Trung bình Volume để đo lường dòng tiền đột biến/cạn kiệt
        df['Vol_MA_20'] = df['volume'].rolling(window=20).mean()
        
        # Xác định hộp Darvas / Trading Range ngắn hạn (20 phiên)
        df['Min_20'] = df['low'].rolling(window=20).min()
        df['Max_20'] = df['high'].rolling(window=20).max()

        # Lấy dòng dữ liệu của phiên mới nhất và phiên trước đó
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        
        # Bỏ qua các mã không có thanh khoản (VD: Volume trung bình < 100k)
        if latest['Vol_MA_20'] < 100000:
            continue

        # --- LOGIC WYCKOFF ĐỊNH LƯỢNG (QUANT LOGIC) ---
        
        # 1. Điều kiện Spring (Bear Trap / Rũ bỏ)
        # Giá nhúng thủng đáy 20 phiên trước, nhưng đóng cửa rút chân lên nửa trên của nến kèm Volume lớn
        is_spring = (latest['low'] < prev['Min_20']) and \
                    (latest['close'] > (latest['high'] + latest['low']) / 2) and \
                    (latest['volume'] > latest['Vol_MA_20'] * 1.3)
                    
        # 2. Điều kiện SOS (Sign of Strength / Bùng nổ)
        # Đóng cửa phá vỡ đỉnh 20 phiên, nến xanh đặc, Volume gấp rưỡi trung bình
        is_sos = (latest['close'] > prev['Max_20']) and \
                 (latest['close'] > latest['open']) and \
                 (latest['volume'] > latest['Vol_MA_20'] * 1.5)
                 
        # 3. Điều kiện LPS (Last Point of Support / Cạn cung)
        # Giá nằm trên MA50 (giữ được trend), đang dao động sát MA20 (+/- 2%), Volume cạn kiệt (dưới 80% TB)
        is_lps = (latest['close'] > latest['SMA_50']) and \
                 (abs(latest['close'] - latest['SMA_20']) / latest['SMA_20'] < 0.02) and \
                 (latest['volume'] < latest['Vol_MA_20'] * 0.8)

        # 4. Ghi nhận nếu thỏa mãn 1 trong 3 tín hiệu
        if is_spring or is_sos or is_lps:
            status = []
            if is_spring: status.append("Phase C: Spring (Rút chân đáy)")
            if is_sos: status.append("Phase D: SOS (Breakout đỉnh)")
            if is_lps: status.append("Phase D: LPS (Retest cạn cung)")
            
            buyable_stocks.append({
                "Mã CP": ticker,
                "Giá hiện tại": round(latest['close'], 2),
                "Tỷ lệ Volume": f"{round(latest['volume'] / latest['Vol_MA_20'], 2)}x",
                "Trạng thái Wyckoff": " | ".join(status)
            })
            
    except Exception as e:
        # Bỏ qua các lỗi API hoặc index out of bounds
        pass

# 4. In kết quả ra màn hình
if buyable_stocks:
    result_df = pd.DataFrame(buyable_stocks)
    print("\n=== DANH SÁCH CỔ PHIẾU CÓ ĐIỂM MUA THEO WYCKOFF ===")
    print(result_df.to_markdown(index=False))
else:
    print("\nKhông tìm thấy cổ phiếu nào thỏa mãn điều kiện Wyckoff trong phiên này.")