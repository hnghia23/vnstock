import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

from streaming.streaming_etl import *
from datetime import datetime
from streamlit_autorefresh import st_autorefresh
from pathlib import Path



# root path
PROJECT_ROOT = Path(__file__).resolve().parent


# Check the symbol for stock code if exist
def check_legit_symbol(symbol):
    symbols = (
        pd.read_csv(PROJECT_ROOT / "data" / "symbol.csv")[["symbol"]]
        .iloc[:, 0]
        .tolist()
    )

    return symbol in symbols


# Calculate basic metrics from the stock data
def calculate_metrics(data):
    last_close = data["close"].iloc[-1]
    prev_close = data["close"].iloc[-2]
    change = last_close - prev_close
    pct_change = (change / prev_close) * 100
    high = data["high"].max()
    low = data["low"].min()
    volume = data["volume"].sum()

    return last_close, change, pct_change, high, low, volume



## PART 2: Creating the Dashboard App layout ##
# Set up Streamlit page layout
st.set_page_config(layout="wide")
st.title("Real Time Stock Dashboard")

# 2A: SIDEBAR PARAMETERS ############

# Sidebar for user input parameters
st.sidebar.header("Chart Parameters")
ticker = st.sidebar.text_input("Ticker")
chart_type = st.sidebar.selectbox("Chart Type", ["Candlestick", "Line"])
indicators = st.sidebar.multiselect("Technical Indicators", ["SMA 20", "EMA 20"])


# 2B: MAIN CONTENT AREA ############

# autorefresher setting - for streaming dashboard
st_autorefresh(interval=5 * 1000, key="check_minutes")


# Get stock information, data from API 
if st.sidebar.button("Update"):
    st.session_state["selected_ticker"] = ticker
    st.session_state["previous_minute"] = datetime.now().minute
    st.session_state["auto_update"] = True

    df = extract_data(ticker)
    df = transform_data(df)
    load_data(df, ticker, PROJECT_ROOT / "data" / "temp")

    st.success(f"Đang theo dõi {ticker} (tự động cập nhật mỗi phút)")



# create streaming dashboard from data
if st.session_state.get("auto_update", False):
    ticker = st.session_state["selected_ticker"]
    previous_minute = st.session_state.get("previous_minute", None)
    current_minute = datetime.now().minute
    current_hour = datetime.now().hour

    st.subheader(f"Biến động giá hôm nay - {ticker}")

    # check close time
    if current_hour >= 15:
        st.warning("Thị trường đã đóng cửa — dừng cập nhật.")
        st.session_state["auto_update"] = False

    # update data each minute
    elif current_minute != previous_minute:
        st.session_state["previous_minute"] = current_minute
        print(f"Cập nhật dữ liệu mới lúc {datetime.now().strftime('%H:%M:%S')}")

        update_data(ticker, PROJECT_ROOT / "data" / "temp")

    # Read new data, create dashboard
    try:
        data = pd.read_csv(PROJECT_ROOT / "data" / "temp" / f"{ticker}.csv")

        last_close, change, pct_change, high, low, volume = calculate_metrics(data)

        # Display main metrics
        st.metric(
            label=f"{ticker} Last Price",
            value=f"{last_close:.2f} USD",
            delta=f"{change:.2f} ({pct_change:.2f}%)",
        )

        col1, col2, col3 = st.columns(3)
        col1.metric("High", f"{high:.2f} USD")
        col2.metric("Low", f"{low:.2f} USD")
        col3.metric("Volume", f"{volume:,}")

        # Plot the stock price chart
        fig = go.Figure()
        if chart_type == "Candlestick":
            fig.add_trace(
                go.Candlestick(
                    x=data["time"],
                    open=data["open"],
                    high=data["high"],
                    low=data["low"],
                    close=data["close"],
                )
            )
        else:
            fig = px.line(data, x="time", y="close")

        # Add selected technical indicators to the chart
        for indicator in indicators:
            if indicator == "SMA 20":
                fig.add_trace(
                    go.Scatter(x=data["time"], y=data["SMA_20"], name="SMA 20")
                )
            elif indicator == "EMA 20":
                fig.add_trace(
                    go.Scatter(x=data["time"], y=data["EMA_20"], name="EMA 20")
                )

        # Format graph
        fig.update_layout(
            title=f"{ticker} real-time Chart",
            xaxis_title="Time",
            yaxis_title="Price (USD)",
            height=600,
        )
        st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.info("Chưa có dữ liệu hiện tại")

else:
    st.info("Nhập mã cổ phiếu và bấm **Update** để bắt đầu theo dõi.")


# Batch dashboard
st.subheader("Biến động giá từ 01-01-2020")

try:
    data_batch = pd.read_csv(PROJECT_ROOT / "data" / "lake" / f"{ticker}.csv")

    if data_batch.empty:
        st.info("Không có dữ liệu")
    else:
        last_close2, change2, pct_change2, high2, low2, volume2 = calculate_metrics(
            data_batch
        )
        # Display main metrics
        st.metric(
            label=f"{ticker} Last Price",
            value=f"{last_close2:.2f} USD",
            delta=f"{change2:.2f} ({pct_change2:.2f}%)",
        )

        column1, column2, column3 = st.columns(3)
        column1.metric("High", f"{high2:.2f} USD")
        column2.metric("Low", f"{low2:.2f} USD")
        column3.metric("Volume", f"{volume2:,}")

        # Plot the stock price chart
        fig2 = go.Figure()
        if chart_type == "Candlestick":
            fig2.add_trace(
                go.Candlestick(
                    x=data_batch["time"],
                    open=data_batch["open"],
                    high=data_batch["high"],
                    low=data_batch["low"],
                    close=data_batch["close"],
                )
            )
        else:
            fig2 = px.line(data_batch, x="time", y="close")

        # Add selected technical indicators to the chart
        for indicator in indicators:
            if indicator == "SMA 20":
                fig2.add_trace(
                    go.Scatter(
                        x=data_batch["time"], y=data_batch["SMA_20"], name="SMA 20"
                    )
                )
            elif indicator == "EMA 20":
                fig2.add_trace(
                    go.Scatter(
                        x=data_batch["time"], y=data_batch["EMA_20"], name="EMA 20"
                    )
                )

        # Format graph
        fig2.update_layout(
            title=f"{ticker} batch Chart",
            xaxis_title="Time",
            yaxis_title="Price (USD)",
            height=600,
        )
        st.plotly_chart(fig2, use_container_width=True)
except:
    print("Chưa có thông tin mã")
    st.info("Nhập mã cổ phiếu và bấm **Update** để xem thông tin.")




# 2C: SIDEBAR PRICES ############

# Sidebar section for real-time stock prices of selected symbols
st.sidebar.header("Real-Time Stock Prices")

# Sidebar information section
st.sidebar.subheader("About")
st.sidebar.info(
    "This dashboard provides stock data and technical indicators for various time periods. Use the sidebar to customize your view."
)
