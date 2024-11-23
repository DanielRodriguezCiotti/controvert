import yfinance as yf
# Define the ticker symbol for Orpea
ticker_symbol = "EMEIS.PA"  # Yahoo Finance symbol for Orpea (adjust if necessary)

# Define the date range
start_date = "2021-12-17"
end_date = "2022-02-04"

# Fetch historical data
try:
    orpea_data = yf.download(ticker_symbol, start=start_date, end=end_date)
    assert orpea_data is not None, "No data retrieved"
    # Optionally save to a CSV file
    orpea_data.to_csv("orpea_stock_data.csv")
    print("Data saved to orpea_stock_data.csv")

    # print("Data saved to orpea_stock_data.csv")
except Exception as e:
    print(f"An error occurred: {e}")
