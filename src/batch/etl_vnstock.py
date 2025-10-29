from vnstock import Quote
from tenacity import RetryError
from datetime import date, timedelta
import pandas as pd
import os
import io
from minio import Minio
from minio.error import S3Error



# === Extract: lấy dữ liệu mới kể từ ngày cuối cùng trong file local ===
def extract_data(symbol, base_path="data/lake", start_date=None):
    today = date.today()

    # Nếu không có tham số truyền vào, mặc định ngày bắt đầu là 01/01/2020
    if start_date == None:
        start_date = "2020-01-01"

    # Nếu đã có file, đọc ngày cuối cùng để chỉ lấy incremental
    file_path = os.path.join(base_path, f"{symbol}.csv")
    if os.path.exists(file_path):
        try:
            # Get start_date from existed file
            existing_df = pd.read_csv(file_path)
            if not existing_df.empty:
                last_date = pd.to_datetime(existing_df["time"]).max()
                start_date = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
        except Exception as e:
            print(f"Lỗi khi đọc file {symbol}.csv: {e}")

    print(f"{symbol}: trích dữ liệu từ {start_date} đến {today}")

    # Call the API
    quote = Quote(symbol=symbol, source="VCI")
    try:
        df = quote.history(start=start_date, end=today.strftime("%Y-%m-%d"))
        print(f"Trích xuất {len(df)} dòng mới cho {symbol}")

    # Nếu không có dữ liệu mới, trả về DataFrame rỗng
    except (ValueError, RetryError):
        print(f"Không tìm thấy dữ liệu cho {symbol}")
        df = pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume"])

    return df


# === Load/append vào file có sẵn ===
def load_data(df, symbol, output_dir, mode="append"):
    # Kiểm tra DataFrame có dữ liệu mới hay không

    # Nếu không có dữ liệu mới
    if df.empty:
        print(f"Không có dữ liệu mới cho {symbol}")
        return

    # Nếu có dữ liệu mới, kiểm tra file đã tồn tại chưa

    # Nếu đã tồn tại, thêm dữ liệu mới vào cuối file
    path = os.path.join(output_dir, f"{symbol}.csv")
    if mode == "append" and os.path.exists(path):
        df.to_csv(path, mode="a", header=False, index=False)
        print(f"Append {len(df)} dòng mới vào {path}")

    # Nếu chưa có file, tạo mới, lưu df.
    else:
        df.to_csv(path, index=False)
        print(f"Tạo mới file {path}.csv")


def load_data_to_minio(df, symbol, minio_client, bucket_name, mode="append"):
    if df.empty:
        print(f"Không có dữ liệu mới cho {symbol}")
        return

    parquet_path = f"{symbol}.parquet"

    # Check bucket
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)

    # Nếu file đã tồn tại và cần append
    try:
        response = minio_client.get_object(bucket_name, parquet_path)
        old_df = pd.read_parquet(io.BytesIO(response.read()))
        response.close()
        response.release_conn()

        if mode == "append":
            combined_df = pd.concat([old_df, df], ignore_index=True)
            combined_df.drop_duplicates(inplace=True)
        else:
            combined_df = df

        print(f"Append {len(df)} dòng mới vào {parquet_path}")

    except S3Error:
        # File chưa tồn tại → tạo mới
        combined_df = df
        print(f"Tạo mới {parquet_path} trên MinIO")

    # Upload lại dưới dạng Parquet
    buffer = io.BytesIO()
    combined_df.to_parquet(buffer, index=False)
    buffer.seek(0)

    minio_client.put_object(
        bucket_name=bucket_name,
        object_name=parquet_path,
        data=buffer,
        length=buffer.getbuffer().nbytes,
        content_type="application/octet-stream"
    )

    print(f"Upload thành công {parquet_path} lên MinIO ✅")

