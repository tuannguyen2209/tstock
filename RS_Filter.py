import pandas as pd
from FiinQuantX import FiinSession
from datetime import datetime, timedelta

# 1. Khởi tạo và Đăng nhập
username = 'DNSE_FG_164@fiinquant.vn'
password = 'DNSE_FG_FiinQuant_@0@6'
client = FiinSession(username=username, password=password).login()

vn30_list = client.TickerList(ticker="VN100")

end_date = datetime.now().strftime("%Y-%m-%d")
start_date = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
fallback_start = "2024-10-10"
fallback_end = "2026-04-16"

print(f"1. Đang quét tín hiệu RRG rổ VN30 từ {start_date} đến {end_date}...")

try:
    # --- PHẦN 1: KÉO DỮ LIỆU VÀ TÍNH TOÁN RRG ---
    rrg = client.RRG(tickers=vn30_list, benchmark="VNINDEX", by='1d', from_date=start_date, to_date=end_date)
    df_rrg = rrg.get()
    
    if df_rrg is None or df_rrg.empty:
        print(f"Dữ liệu hôm nay chưa có, chuyển về mốc dự phòng...")
        rrg = client.RRG(tickers=vn30_list, benchmark="VNINDEX", by='1d', from_date=fallback_start, to_date=fallback_end)
        df_rrg = rrg.get()

    if df_rrg.index.name is not None or 'index' not in df_rrg.columns:
        df_rrg = df_rrg.reset_index()

    df_rrg.columns = df_rrg.columns.str.lower()
    df_rrg['timestamp'] = pd.to_datetime(df_rrg['timestamp'] if 'timestamp' in df_rrg.columns else df_rrg['date'])
    df_rrg = df_rrg.sort_values(by=['ticker', 'timestamp']).reset_index(drop=True)

    # Tính Động lượng thay đổi (Xem mũi tên RRG đang hướng lên hay cắm xuống)
    df_rrg['rm_change'] = df_rrg.groupby('ticker')['rm'].diff()

    # Lấy ngày hiện tại
    latest_rrg = df_rrg.groupby('ticker').last().reset_index()

    # Phân loại Quadrant
    latest_rrg['Quadrant'] = 'None'
    latest_rrg.loc[(latest_rrg['rs'] >= 100) & (latest_rrg['rm'] >= 100), 'Quadrant'] = 'Leading'
    latest_rrg.loc[(latest_rrg['rs'] >= 100) & (latest_rrg['rm'] < 100), 'Quadrant'] = 'Weakening'
    latest_rrg.loc[(latest_rrg['rs'] < 100) & (latest_rrg['rm'] < 100), 'Quadrant'] = 'Lagging'
    latest_rrg.loc[(latest_rrg['rs'] < 100) & (latest_rrg['rm'] >= 100), 'Quadrant'] = 'Improving'

    # --- PHẦN 2: TÁCH 2 NHÓM CỔ PHIẾU MỤC TIÊU ---
    # Nhóm 1: Đang ở Improving (Sắp nổ)
    improving_stocks = latest_rrg[latest_rrg['Quadrant'] == 'Improving'].copy()
    
    # Nhóm 2: Đang Dần Hồi Phục (Nằm ở Lagging nhưng Động lượng RM đang tăng)
    recovering_stocks = latest_rrg[(latest_rrg['Quadrant'] == 'Lagging') & (latest_rrg['rm_change'] > 0)].copy()

    # Gộp danh sách mã để check Hành động giá
    target_tickers = list(set(improving_stocks['ticker'].tolist() + recovering_stocks['ticker'].tolist()))

    if len(target_tickers) == 0:
        print("\nHiện tại không có mã nào thoả mãn tiêu chí Hồi phục hoặc Cải thiện.")
    else:
        print(f"-> Tìm thấy {len(improving_stocks)} mã Improving và {len(recovering_stocks)} mã Dần hồi phục.")
        print("\n2. Đang kiểm tra Hành động giá (MA20 & Volume) cho các mã này...\n")
        
        # --- PHẦN 3: KIỂM TRA MA20 VÀ VOLUME ---
        event = client.Fetch_Trading_Data(
            realtime=False, tickers=target_tickers, fields=['close', 'volume'], adjusted=True, period=40, by='1d'
        )
        df_price = event.get_data()
        
        if df_price.index.name is not None or 'index' not in df_price.columns:
            df_price = df_price.reset_index()
        df_price.columns = df_price.columns.str.lower()
        
        time_col = 'timestamp' if 'timestamp' in df_price.columns else 'date' if 'date' in df_price.columns else df_price.columns[1]
        df_price[time_col] = pd.to_datetime(df_price[time_col])
        df_price = df_price.sort_values(by=['ticker', time_col]).reset_index(drop=True)
        
        df_price['ma20'] = df_price.groupby('ticker')['close'].transform(lambda x: x.rolling(20).mean())
        df_price['avg_vol20'] = df_price.groupby('ticker')['volume'].transform(lambda x: x.rolling(20).mean())
        
        latest_price = df_price.groupby('ticker').last().reset_index()
        
        # Hàm in bảng hiển thị
        def print_stock_table(df_group, title):
            if df_group.empty:
                print(f"{title}: Không có mã nào.\n")
                return
            
            # Ghép dữ liệu Giá vào RRG
            merged = pd.merge(df_group[['ticker', 'rs', 'rm']], latest_price[['ticker', 'close', 'ma20', 'volume', 'avg_vol20']], on='ticker', how='left')
            merged['Trên MA20?'] = merged['close'] > merged['ma20']
            merged['Nổ Vol?'] = merged['volume'] > merged['avg_vol20']
            merged['Điểm TA'] = merged['Trên MA20?'].astype(int) + merged['Nổ Vol?'].astype(int)
            merged = merged.sort_values(by=['Điểm TA', 'rm'], ascending=[False, False])
            
            print("="*95)
            print(title)
            print("="*95)
            headers = f"{'MÃ':<5} | {'GIÁ':<8} | {'MA20':<8} | {'TRẠNG THÁI TREND':<18} | {'TRẠNG THÁI VOLUME':<20} | {'RM (ĐỘNG LƯỢNG)':<10}"
            print(headers)
            print("-" * len(headers))
            
            for idx, row in merged.iterrows():
                price = f"{row['close']:,.0f}"
                ma20 = f"{row['ma20']:,.0f}" if not pd.isna(row['ma20']) else "N/A"
                rm = f"{row['rm']:.2f}"
                trend = "✅ Vượt MA20" if row['Trên MA20?'] else "❌ Dưới MA20"
                vol_ratio = row['volume'] / row['avg_vol20'] if row['avg_vol20'] > 0 else 0
                vol = f"🔥 Nổ ({vol_ratio:.1f}x TB)" if row['Nổ Vol?'] else "💧 Cạn kiệt"
                star = "⭐" if row['Điểm TA'] == 2 else "  "
                
                print(f"{star} {row['ticker']:<3} | {price:<8} | {ma20:<8} | {trend:<18} | {vol:<20} | {rm:<10}")
            print("\n")

        # In 2 bảng độc lập
        print_stock_table(improving_stocks, "🌟 NHÓM 1: ĐANG Ở PHA IMPROVING (Sắp bứt phá sang Leading)")
        print_stock_table(recovering_stocks, "🛠️ NHÓM 2: ĐANG DẦN HỒI PHỤC TỪ ĐÁY (Pha Lagging nhưng Động lượng đang tăng)")

        print("📝 CHIẾN LƯỢC GIẢI NGÂN:")
        print("- Nhóm 1 (Improving): Độ an toàn cao. Đánh mạnh tay vào các mã có dấu ⭐ (Vượt MA20 + Nổ Vol).")
        print("- Nhóm 2 (Dần hồi phục): Đang dò đáy. CHỈ MUA THĂM DÒ tỷ trọng nhỏ nếu mã có dấu ⭐. Các mã chưa vượt MA20 tuyệt đối không được bắt dao rơi!")

except Exception as e:
    print(f"\n[LỖI HỆ THỐNG]: {e}")