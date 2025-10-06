from vnstock import Quote
import pandas as pd
from datetime import date
from tenacity import RetryError
import os
import ta


# Funtion to extract today stock price data from API, start from date...(default: from 01/01/2020)
def extract_data(symbol, start_date=None):
    today = date.today()

    if start_date is None:
        start_date = "2020-01-01"

    quote = Quote(symbol=symbol, source="VCI")

    try:
        df = quote.history(
            start=start_date,
            end=today.strftime("%Y-%m-%d"),
        )
        print("Trích xuất dữ liệu thành công")

    except (ValueError, RetryError):
        print(f"Không tìm thấy dữ liệu cho {symbol}.")
        # tạo DataFrame rỗng có cấu trúc chuẩn
        df = pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume"])

    return df


# Function to transform data: create technical indicators (SMA, EMA)
def transform_data(df):
    # Add technical indicators: simple moving average (SMA) and exponential moving average (EMA) indicators
    df["SMA_20"] = ta.trend.sma_indicator(df["close"], window=20).round(4)
    df["EMA_20"] = ta.trend.ema_indicator(df["close"], window=20).round(4)

    print("Chuyển đổi thành công")

    return df


# Function to load data to data lake
def load_data(df, symbol, path):
    # create folder if not exist
    os.makedirs(path, exist_ok=True)

    # save CSV file
    df.to_csv(f"{path}/{symbol}.csv", index=False)

    if df.empty:
        print("Không có dữ liệu")
    else:
        print(f"Lưu thành công data tại {path}/{symbol}.csv")


# Function to update if there are new data
def append_data(df, symbol, path):
    # create folder if not exist
    os.makedirs(path, exist_ok=True)

    # save CSV file
    df.to_csv(f"{path}/{symbol}.csv", mode="a", index=False, header=False)

    if df.empty:
        print("Chưa có dữ liệu mới")
    else:
        print("Lưu thành công data mới tại {path}/{symbol}.csv")
