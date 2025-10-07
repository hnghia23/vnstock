from vnstock import Quote
from tenacity import RetryError
from datetime import date, datetime, timedelta
import pandas as pd
import ta
import os


# === Extract: lấy dữ liệu mới kể từ ngày cuối cùng trong file local ===
def extract_data(symbol, base_path="data/lake", start_date=None):
    today = date.today()
    if start_date == None:
        start_date = "2020-01-01"

    # Nếu đã có file, đọc ngày cuối cùng để chỉ lấy incremental
    file_path = os.path.join(base_path, f"{symbol}.csv")
    if os.path.exists(file_path):
        try:
            existing_df = pd.read_csv(file_path)
            if not existing_df.empty:
                last_date = pd.to_datetime(existing_df["time"]).max()
                start_date = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
        except Exception as e:
            print(f"Lỗi khi đọc file {symbol}.csv: {e}")

    print(f"{symbol}: trích dữ liệu từ {start_date} đến {today}")

    quote = Quote(symbol=symbol, source="VCI")
    try:
        df = quote.history(start=start_date, end=today.strftime("%Y-%m-%d"))
        print(f"Trích xuất {len(df)} dòng mới cho {symbol}")
    except (ValueError, RetryError):
        print(f"Không tìm thấy dữ liệu cho {symbol}")
        df = pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume"])

    return df


# === Transform ===
def transform_data(df):
    if not df.empty:
        df["SMA_20"] = ta.trend.sma_indicator(df["close"], window=20).round(4)
        df["EMA_20"] = ta.trend.ema_indicator(df["close"], window=20).round(4)
    return df


# === Load/append vào file có sẵn ===
def load_data(df, symbol, output_dir, mode="append"):
    if df.empty:
        print(f"Không có dữ liệu mới cho {symbol}")
        return

    path = os.path.join(output_dir, f"{symbol}.csv")
    if mode == "append" and os.path.exists(path):
        df.to_csv(path, mode="a", header=False, index=False)
        print(f"Append {len(df)} dòng mới vào {path}")

    else:
        df.to_csv(path, index=False)
        print(f"Tạo mới file {path}")
