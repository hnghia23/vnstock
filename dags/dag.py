from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from datetime import timedelta
import pandas as pd
import os, sys, time
from collections import deque

# === Cấu hình đường dẫn ===
sys.path.append("/opt/airflow/src")
from batch.etl_vnstock import extract_data, load_data


# === Cấu hình chung ===
DATA_DIR = "/opt/airflow/data"
SYMBOL_FILE = os.path.join(DATA_DIR, "symbol.csv")
OUTPUT_DIR = os.path.join(DATA_DIR, "lake")


# ===Rate Limiter==
class SimpleRateLimiter:
    """Giới hạn số lượng request trong mỗi khoảng thời gian."""
    def __init__(self, max_calls: int, period: int):
        self.max_calls = max_calls
        self.period = period
        self.calls = deque()

    def acquire(self):
        now = time.time()
        while self.calls and now - self.calls[0] > self.period:
            self.calls.popleft()

        if len(self.calls) >= self.max_calls:
            sleep_time = self.period - (now - self.calls[0])
            print(f"Giới hạn API đạt mức tối đa, chờ {sleep_time:.1f}s...")
            time.sleep(sleep_time)

        self.calls.append(time.time())


# Giới hạn 5 request/phút
limiter = SimpleRateLimiter(max_calls=5, period=60)

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

def validate_data(df: pd.DataFrame) -> bool:
    """Kiểm tra schema, null, duplicate, kiểu dữ liệu"""
    required_columns = ["time", "open", "high", "low", "close", "volume"]
    for col in required_columns:
        if col not in df.columns:
            print(f"❌ Thiếu cột {col}")
            return False

    # Check null
    if df[required_columns].isnull().any().any():
        print("❌ Phát hiện giá trị null trong dữ liệu.")
        return False

    # Check duplicate
    # if df.duplicated(subset=["time", "symbol"]).any():
    #     print("❌ Phát hiện dòng trùng lặp.")
    #     return False

    # Check data types
    try:
        df["time"] = pd.to_datetime(df["time"], errors="raise")
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="raise")
    except Exception as e:
        print(f"❌ Lỗi kiểu dữ liệu: {e}")
        return False

    print("✅ Data Validation OK.")
    return True


def quality_check(df: pd.DataFrame) -> bool:
    """Kiểm tra logic nghiệp vụ (giá trị hợp lý, volume dương, v.v.)"""
    invalid_prices = df[(df["open"] <= 0) | (df["close"] < 0)]
    if not invalid_prices.empty:
        print(f"❌ Phát hiện giá <= 0 ở {len(invalid_prices)} dòng.")
        return False

    invalid_vol = df[df["volume"] < 0]
    if not invalid_vol.empty:
        print(f"❌ Phát hiện volume âm ở {len(invalid_vol)} dòng.")
        return False

    print("✅ Quality Check OK.")
    return True


# === DAG ===
with DAG(
    dag_id="vnstock_weekly_etl_incremental",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="0 7 * * MON",  
    catchup=False,
    tags=["vnstock", "etl", "incremental"],
    max_active_tasks=18,  # cho phép chạy song song 18 batch
) as dag:

    # --- Task 1: Đọc danh sách mã cổ phiếu ---
    @task()
    def load_symbols():
        if not os.path.exists(SYMBOL_FILE):
            raise FileNotFoundError(f"Không tìm thấy file {SYMBOL_FILE}")

        df = pd.read_csv(SYMBOL_FILE)
        symbols = df["symbol"].dropna().unique().tolist()
        print(f"Đã tải {len(symbols)} mã cổ phiếu từ symbol.csv.")
        return symbols


    # --- Task 2: Chia symbol thành nhiều batch ---
    @task()
    def split_batches(symbols: list, batch_size: int = 50):
        batches = [symbols[i:i + batch_size] for i in range(0, len(symbols), batch_size)]
        print(f"Chia thành {len(batches)} batch, mỗi batch {batch_size} mã.")
        return batches

    # --- Task 3: Xử lý từng batch ---
    @task(retries=2, retry_delay=timedelta(minutes=2))
    def process_batch(symbol_batch: list):
        for symbol in symbol_batch:
            try:
                limiter.acquire()
                print(f"Bắt đầu xử lý symbol = {symbol}")

                # --- Bước 1: Đọc dữ liệu cũ (nếu có) ---
                symbol_path = os.path.join(OUTPUT_DIR, f"{symbol}.csv")
                last_date = None
                if os.path.exists(symbol_path):
                    try:
                        df_old = pd.read_csv(symbol_path)
                        if not df_old.empty:
                            last_date = pd.to_datetime(df_old["time"]).max().strftime("%Y-%m-%d")
                            print(f"{symbol}: Dữ liệu cũ đến {last_date}")
                    except Exception as e:
                        print(f"Không đọc được file cũ {symbol}: {e}")

                # --- Bước 2: Extract dữ liệu mới ---
                df_new = extract_data(symbol, OUTPUT_DIR, start_date=last_date)
                if df_new is None or df_new.empty:
                    print(f"{symbol}: Không có dữ liệu mới.")
                    continue
                
                # --- Bước 3: Validation ---
                if not validate_data(df_new):
                    print(f"{symbol}: ❌ Data Validation thất bại, bỏ qua.")
                    continue

                # --- Bước 4: Quality Check ---
                if not quality_check(df_new):
                    print(f"{symbol}: ❌ Quality Check thất bại, bỏ qua.")
                    continue

                # --- Bước 5: Append vào file hiện có ---
                load_data(df_new, symbol, OUTPUT_DIR, mode="append")

                print(f"Hoàn tất {symbol}, thêm {len(df_new)} dòng mới.")
                time.sleep(2)  

            except Exception as e:
                print(f"Lỗi khi xử lý {symbol}: {e}")
                continue


    # === Flow ===
    symbols = load_symbols()

    batches = split_batches(symbols)

    process_batch.expand(symbol_batch=batches)
