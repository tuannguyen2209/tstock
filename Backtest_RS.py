import pandas as pd
from FiinQuantX import FiinSession

# 1. Khởi tạo và Đăng nhập
username = 'DNSE_FG_164@fiinquant.vn'
password = 'DNSE_FG_FiinQuant_@0@6'
client = FiinSession(username=username, password=password).login()

vn30_list = [
    "ACB", "BCM", "BID", "BVH", "CTG", "FPT", "GAS", "GVR", "HDB", "HPG", 
    "MBB", "MSN", "MWG", "PLX", "POW", "SAB", "SHB", "SSB", "SSI", "STB", 
    "TCB", "TPB", "VCB", "VHM", "VIB", "VIC", "VJC", "VNM", "VPB", "VRE"
]

# SỬ DỤNG MỐC THỜI GIAN ĐÃ KIỂM CHỨNG LÀ API TRẢ VỀ DỮ LIỆU CHUẨN
start_date = "2024-10-10" 
end_date = "2026-04-16"

print(f"Đang tải dữ liệu và Backtest chiến lược Bắt đáy rổ VN30 từ {start_date} đến {end_date}...\n")

try:
    # --- BƯỚC 1: KÉO VÀ CHUẨN HÓA DỮ LIỆU ---
    rrg = client.RRG(tickers=vn30_list, benchmark="VNINDEX", by='1d', from_date=start_date, to_date=end_date)
    df_rrg = rrg.get()
    
    if df_rrg is None or df_rrg.empty:
        raise ValueError("API trả về dữ liệu rỗng.")

    if df_rrg.index.name is not None or 'index' not in df_rrg.columns:
        df_rrg = df_rrg.reset_index()

    df_rrg.columns = df_rrg.columns.str.lower()
    time_col = 'timestamp' if 'timestamp' in df_rrg.columns else 'date'
    df_rrg[time_col] = pd.to_datetime(df_rrg[time_col])
    
    # Sắp xếp và phân loại Quadrant
    df_rrg = df_rrg.sort_values(by=['ticker', time_col]).reset_index(drop=True)
    df_rrg['Quadrant'] = 'None'
    df_rrg.loc[(df_rrg['rs'] >= 100) & (df_rrg['rm'] >= 100), 'Quadrant'] = 'Leading'
    df_rrg.loc[(df_rrg['rs'] >= 100) & (df_rrg['rm'] < 100), 'Quadrant'] = 'Weakening'
    df_rrg.loc[(df_rrg['rs'] < 100) & (df_rrg['rm'] < 100), 'Quadrant'] = 'Lagging'
    df_rrg.loc[(df_rrg['rs'] < 100) & (df_rrg['rm'] >= 100), 'Quadrant'] = 'Improving'

    # --- BƯỚC 2: TIẾN HÀNH BACKTEST TỪNG MÃ ---
    all_trades = []

    for ticker in vn30_list:
        df_ticker = df_rrg[df_rrg['ticker'] == ticker].copy()
        if df_ticker.empty:
            continue
            
        # Tính toán MA20 trực tiếp
        df_ticker['ma20'] = df_ticker['close'].rolling(window=20).mean()
        df_ticker['prev_close'] = df_ticker['close'].shift(1)
        df_ticker['prev_ma20'] = df_ticker['ma20'].shift(1)
        
        position = 0 
        entry_price = 0
        entry_date = None

        for index, row in df_ticker.iterrows():
            if pd.isna(row['ma20']) or pd.isna(row['prev_ma20']):
                continue 

            # ĐIỀU KIỆN MUA: Ở pha Lagging VÀ Cắt lên MA20 
            is_cross_up_ma20 = (row['close'] > row['ma20']) and (row['prev_close'] <= row['prev_ma20'])
            
            if position == 0 and row['Quadrant'] == 'Lagging' and is_cross_up_ma20:
                position = 1
                entry_date = row[time_col]
                entry_price = row['close']
                
            # ĐIỀU KIỆN BÁN: Gãy MA20 HOẶC Chuyển sang Weakening 
            elif position == 1:
                is_cross_down_ma20 = row['close'] < row['ma20']
                is_weakening = row['Quadrant'] == 'Weakening'
                
                if is_cross_down_ma20 or is_weakening:
                    position = 0
                    exit_date = row[time_col]
                    exit_price = row['close']
                    profit_pct = ((exit_price - entry_price) / entry_price) * 100
                    
                    reason = "Gãy MA20" if is_cross_down_ma20 else "Chốt lời (Weakening)"
                    
                    all_trades.append({
                        'Mã CP': ticker,
                        'Ngày Mua': entry_date.strftime('%Y-%m-%d'),
                        'Giá Mua': entry_price,
                        'Ngày Bán': exit_date.strftime('%Y-%m-%d'),
                        'Giá Bán': exit_price,
                        'Lợi Nhuận (%)': round(profit_pct, 2),
                        'Lý do Bán': reason
                    })

    # --- BƯỚC 3: TỔNG HỢP VÀ XUẤT BÁO CÁO ---
    df_trades = pd.DataFrame(all_trades)

    print("="*85)
    print("🎯 KẾT QUẢ BACKTEST: CHIẾN LƯỢC BẮT ĐÁY (LAGGING + CROSS MA20)")
    print("="*85)
    
    if not df_trades.empty:
        df_trades = df_trades.sort_values('Ngày Mua').reset_index(drop=True)
        print(df_trades.tail(15).to_string(index=False))
        print("\n(Bảng trên hiển thị 15 lệnh giao dịch gần nhất...)")
        
        total_trades = len(df_trades)
        win_trades = len(df_trades[df_trades['Lợi Nhuận (%)'] > 0])
        loss_trades = total_trades - win_trades
        win_rate = (win_trades / total_trades) * 100
        
        avg_profit = df_trades[df_trades['Lợi Nhuận (%)'] > 0]['Lợi Nhuận (%)'].mean() if win_trades > 0 else 0
        avg_loss = df_trades[df_trades['Lợi Nhuận (%)'] <= 0]['Lợi Nhuận (%)'].mean() if loss_trades > 0 else 0
        max_profit = df_trades['Lợi Nhuận (%)'].max()
        max_loss = df_trades['Lợi Nhuận (%)'].min()
        
        print("\n" + "-"*60)
        print("📊 TỔNG QUAN HIỆU SUẤT HỆ THỐNG:")
        print(f"Tổng số lệnh đã đánh: {total_trades}")
        print(f"Tỷ lệ thắng (Win Rate): {win_rate:.2f}%")
        print(f"Lợi nhuận trung bình lệnh Thắng: {avg_profit:.2f}%")
        print(f"Mức lỗ trung bình lệnh Thua: {avg_loss:.2f}%")
        print(f"Lệnh thắng lớn nhất: +{max_profit:.2f}%")
        print(f"Lệnh thua nặng nhất: {max_loss:.2f}%")
        
        if abs(avg_loss) > 0:
            reward_risk = avg_profit / abs(avg_loss)
            print(f"Tỷ lệ Risk/Reward: 1 / {reward_risk:.2f}")
    else:
        print("Không có tín hiệu giao dịch nào thỏa mãn bộ lọc nghiệm ngặt này.")

except Exception as e:
    print(f"\n[LỖI HỆ THỐNG]: {e}")