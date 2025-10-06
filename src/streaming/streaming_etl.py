from vnstock import Quote

import pandas as pd 
from datetime import date
from dateutil.relativedelta import relativedelta
from tenacity import RetryError
import ta
import os


# Funtion to extract today stock price data from API 
def extract_data(symbol):
    today = date.today()
    tomorrow = today + relativedelta(days=1)

    try:
        quote = Quote(symbol=symbol, source='VCI')

        df = quote.history(start=today.strftime('%Y-%m-%d'),
                            end=tomorrow.strftime('%Y-%m-%d'),
                            interval='1m')
        
        print("Trích xuất dữ liệu thành công.")

    except (ValueError, RetryError):
        print(f"Không tìm thấy dữ liệu cho {symbol}.")
        # create clone DataFrame
        df = pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume"])

    return df



# Function to transform data: get hh:mm time, create technical indicators (SMA, EMA)

def transform_data(df):

    # transform "time" column
    df = df.copy()
    df["time"] = df["time"].dt.strftime("%H:%M")
    
    # Add technical indicators: simple moving average (SMA) and exponential moving average (EMA) indicators
    df['SMA_20'] = ta.trend.sma_indicator(df['close'], window=20).round(4)
    df['EMA_20'] = ta.trend.ema_indicator(df['close'], window=20).round(4)

    print("Chuyển đổi thành công")
    return df


# Function to load data to temporary storage
def load_data(df, symbol, path):
    os.makedirs(path, exist_ok=True)
    # Save CSV file
    df.to_csv(f"{path}/{symbol}.csv", index=False)


# Function to update if there is new data 
def update_data(symbol, path):
    df = extract_data(symbol)
    
    old_data = pd.read_csv(f'{path}/{symbol}.csv')

    # Check if there is new data
    if (len(df) > len(old_data)):
        new_data = transform_data(df)
        load_data(new_data, symbol, path)
        print("Đã cập nhật dữ liệu mới")
    else:
        print("Chưa có dữ liệu mới")







