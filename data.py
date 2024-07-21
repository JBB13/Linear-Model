import pandas as pd
import yfinance as yf
import time

# Fetch the data
url = 'https://es.wikipedia.org/wiki/Anexo:Compa%C3%B1%C3%ADas_del_S%26P_500'
data_table = pd.read_html(url)
tickers = data_table[0]['Símbolo'].to_list()[:100]

# Initialize empty DataFrames to store the data
open_prices, close_prices, high_prices, low_prices, volume = pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

# Download the data from yfinance for each ticker individually
for ticker in tickers:
    print(f"Processing {ticker}")
    try:
        ticker_data = yf.download(ticker, start='2020-01-01', end='2020-12-31')
        if ticker_data.empty:
            print(f"No data returned for {ticker}")
        else:
            open_prices[ticker] = yf.download(ticker, start='2020-01-01', end='2020-12-31')['Open']
            close_prices[ticker] = yf.download(ticker, start='2020-01-01', end='2020-12-31')['Close']
            high_prices[ticker] = yf.download(ticker, start='2020-01-01', end='2020-12-31')['High']
            low_prices[ticker] = yf.download(ticker, start='2020-01-01', end='2020-12-31')['Low']
            volume[ticker] = yf.download(ticker, start='2020-01-01', end='2020-12-31')['Volume']
        time.sleep(1)  # Delay between requests to avoid hitting rate limits
    except Exception as e:
        print(f"Failed to download data for {ticker}: {e}")

# Check if any DataFrame is empty
if open_prices.empty or close_prices.empty or high_prices.empty or low_prices.empty or volume.empty:
    print("One or more DataFrames are empty. Please check the data download process.")
else:
    # Create MultiIndex columns
    open_prices.columns = pd.MultiIndex.from_product([['Open'], open_prices.columns])
    close_prices.columns = pd.MultiIndex.from_product([['Close'], close_prices.columns])
    high_prices.columns = pd.MultiIndex.from_product([['High'], high_prices.columns])
    low_prices.columns = pd.MultiIndex.from_product([['Low'], low_prices.columns])
    volume.columns = pd.MultiIndex.from_product([['Volume'], volume.columns])

    # Concatenate all data into one DataFrame
    data = pd.concat([open_prices, close_prices, high_prices, low_prices, volume], axis=1)

    # Drop rows with any missing values
    data.dropna(inplace=True)

    # Save to CSV
    data.to_csv('data1.csv')

    # Display the resulting DataFrame
    print(data)
