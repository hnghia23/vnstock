#  Stock Data Pipeline — VnStock ETL & Streaming

## Giới thiệu

Dự án này xây dựng **pipeline thu thập và trực quan dữ liệu chứng khoán Việt Nam** từ API [vnstock](https://pypi.org/project/vnstock/), nhằm **đánh giá cổ phiếu có đáng mua không** dựa trên:

-  **Biến động giá thị trường trong 5 năm gần nhất**  
-  **Dữ liệu giao dịch real-time trong ngày**

Hệ thống được chia thành hai phần:
- **Batch Layer:** Thu thập dữ liệu lịch sử, lưu trữ, thu thập định kỳ.
- **Streaming Layer:** Thu thập dữ liệu thời gian thực trong ngày và trực quan hóa liên tục.

---

## Kiến trúc hệ thống

```text
                 ┌────────────────────┐
                 │     API VnStock    │
                 └────────┬───────────┘
                          │
             ┌────────────┴─────────────┐
             │                          │
     ┌───────▼────────┐         ┌───────▼────────┐
     │  Batch ETL     │         │   Streaming    │
     │ (Airflow DAG)  │         │ (Real-time API)│
     └───────┬────────┘         └────────┬───────┘
             │                           │
     ┌───────▼─────────┐          ┌──────▼───────────┐
     │  Dữ liệu lịch sử│          │ Dữ liệu theo phút│
     │ (CSV / Parquet) │          │   (/data/temp)   │
     └───────┬─────────┘          └──────┬───────────┘
             │                           │
             └────────────┬──────────────┘
                          ▼
                ┌─────────────────────┐
                │   Visualization     │
                │ (Streamlit / Plotly)│
                └─────────────────────┘
```

## Cấu trúc thư mục 
```
  vnstock/
  │
  ├── config
  │
  ├── dags/                       # Airflow DAGs cho batch ETL
  │   └── etl_vnstock_dag.py
  │
  ├── src/
  │   ├── batch/
  │   │   └── etl_vnstock.py      # Hàm extract, transform, load
  │   │    
  │   ├── streaming/
  │   │   └── streaming_etl.py    # Thu thập dữ liệu real-time
  │   │   
  │   └── build.py                # Trực quan dữ liệu
  │                
  │
  ├── data/
  │   ├── lake/                   # Dữ liệu gốc (từ API)
  │   ├── temp/                   # Dữ liệu real-time trong ngày
  │   └── symbol.csv              # Mã chứng khoán 
  │
  ├── logs/                             
  │
  ├── plugins/ 
  │
  ├── requirements.txt
  ├── docker-compose.yml
  ├── Dockerfile
  └── README.md

```


##  Luồng hoạt động

### Batch pipeline (ETL + Airflow)

- **Extract:**  
  Gọi API `vnstock` để lấy dữ liệu giá cổ phiếu trong **5 năm gần nhất**.

- **Transform:**  
  Làm sạch dữ liệu, xử lý thời gian, đổi tên cột, và tính toán thêm **chỉ số kỹ thuật (SMA, RSI, v.v.)**.

- **Load:**  
  Lưu dữ liệu định dạng `.csv` vào thư mục  
  ```bash
  /data/lake/

- **Airflow DAG:**  
    Lên lịch tự động chạy hàng tuần để cập nhật thêm dữ liệu mới.


### Streaming pipeline (Real-time)
- Gọi API Quote của vnstock để lấy dữ liệu giao dịch theo phút.
- Ghi dữ liệu tạm thời vào: ```bash /data/temp/
- Dashboard hiển thị biểu đồ real-time về giá và khối lượng cổ phiếu, cập nhật liên tục.


## Công nghệ sử dụng

| Thành phần           | Công nghệ                        |
| -------------------- | -------------------------------- |
| **Ngôn ngữ**         | Python 3.10+                     |
| **ETL**              | pandas, vnstock, requests        |
| **Orchestration**    | Apache Airflow                   |
| **Streaming**        | vnstock API, asyncio / threading |
| **Visualization**    | Streamlit / Plotly               |
| **Lưu trữ**          | CSV                              |
| **Containerization** | Docker, docker-compose           |



## Cài đặt
1. Clone dự án
   ```bash
    git clone https://github.com/hnghia23/vnstock.git
    cd vnstock

3. Cài đặt thư viện cần thiết
   ```bash
    pip install -r requirements.txt

5. Chạy Airflow
   ```bash
   docker build . --tag extending_airflow:latest
   docker-compose up -d

7. Chạy pipeline streaming
   ```bash
    streamlit run src/build.py


## Giao diện Dashboard 

- Nhập mã cổ phiếu cần xem >>> Update
- Dashboard hiển thị, cập nhật liên tục


## Hướng phát triển

-  **Kết nối với Data Lake triển khai hạ tầng lưu trữ (MinIO, HDFS, ...)**  

-  **Áp dụng Machine Learning để dự báo giá cổ phiếu**  
  (Random Forest, LSTM, ...)

-  **Tích hợp thêm API cho phân tích tài chính nâng cao**  
  (lợi nhuận, P/E, EPS, vốn hóa, doanh thu,...)

---

##  Nguồn tham khảo

- [ **vnstock Library**](https://vnstocks.com/)
- [ **Apache Airflow**](https://airflow.apache.org/)
- [ **Streamlit**](https://streamlit.io/)
- [ **TA-Lib Indicators**](https://ta-lib.github.io/ta-lib-python/)


