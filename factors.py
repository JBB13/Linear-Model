import yfinance as yf
import time
import pandas as pd
import pandas_ta as ta

def fetch_data(tickers, start_date, end_date):
    open_prices = pd.DataFrame()
    close_prices = pd.DataFrame()
    high_prices = pd.DataFrame()
    low_prices = pd.DataFrame()
    volume = pd.DataFrame()

    for ticker in tickers:
        print(f"Processing {ticker}")
        try:
            ticker_data = yf.download(ticker, start=start_date, end=end_date)
            if ticker_data.empty:
                print(f"No data returned for {ticker}")
            else:
                open_prices[ticker] = ticker_data['Open']
                close_prices[ticker] = ticker_data['Close']
                high_prices[ticker] = ticker_data['High']
                low_prices[ticker] = ticker_data['Low']
                volume[ticker] = ticker_data['Volume']

        except Exception as e:
            print(f"Failed to download data for {ticker}: {e}")

    return open_prices, close_prices, high_prices, low_prices, volume

def prepare_data(open_prices, close_prices, high_prices, low_prices, volume):
    open_prices.columns = pd.MultiIndex.from_product([['Open'], open_prices.columns])
    close_prices.columns = pd.MultiIndex.from_product([['Close'], close_prices.columns])
    high_prices.columns = pd.MultiIndex.from_product([['High'], high_prices.columns])
    low_prices.columns = pd.MultiIndex.from_product([['Low'], low_prices.columns])
    volume.columns = pd.MultiIndex.from_product([['Volume'], volume.columns])

    data = pd.concat([open_prices, close_prices, high_prices, low_prices, volume], axis=1)
    data.dropna(inplace=True)
    data_flat = data.copy()
    data_flat.columns = ['_'.join(col).strip() for col in data_flat.columns.values]
    data_flat.to_csv('data1_flat.csv')
    return data_flat

def calculate_indicators(data):
    for col in data.columns:
        ticker = col.split('_')[1]
        if col.split('_')[0] == 'Open':
            data['Dollar_Vol_' + ticker] = data['Close_' + ticker] * data['Volume_' + ticker]
            data['Dollar_Vol_1m_' + ticker] = data['Dollar_Vol_' + ticker].rolling(window=21).mean()
            data['RSI_' + ticker] = ta.rsi(close=data['Close_' + ticker], length=14)

            bbands = ta.bbands(close=data['Close_' + ticker], length=20, std=2)
            data = data.join(bbands.add_suffix('_' + ticker))

            atr = ta.atr(close=data['Close_' + ticker], high=data['High_' + ticker], low=data['Low_' + ticker], length=14)
            data[f'ATR_{ticker}'] = atr

            MACD = ta.macd(close=data['Close_'+ticker])
            data = data.join(MACD.add_suffix('_' + ticker))

            lags = [1, 5, 10, 21, 42, 63]
            q = 0.0001
            
            for lag in lags:
                data[f'Returns_{ticker}_{lag}d'] = (
                    data['Close_' + ticker]
                        .pct_change(lag)
                        .pipe(lambda x: x.clip(lower=x.quantile(q), upper=x.quantile(1 - q)))
                        .add(1)
                        .pow(1 / lag)
                        .sub(1))

            for t in [1, 2, 3, 4, 5]:
                for lag in [1, 5, 10, 21]:
                    data[f'Return_{ticker}_{lag}d_lag{t}'] = data[f'Returns_{ticker}_{lag}d'].shift(t * lag)

    return data

def calculate_forward_returns(data):
    for col in data.columns:
        ticker = col.split('_')[1]
        if col.split('_')[0] == 'Close':
            for t in [1, 5, 10, 21]:
                data[f'target_{ticker}_{t}d'] = data[f'Returns_{ticker}_{t}d'].shift(-t)
    return data

def main():
    start_time = time.time()
    url = 'https://es.wikipedia.org/wiki/Anexo:Compa%C3%B1%C3%ADas_del_S%26P_500'
    data_table = pd.read_html(url)
    tickers = data_table[0]['Símbolo'].to_list()[:10]

    open_prices, close_prices, high_prices, low_prices, volume = fetch_data(tickers, '2020-01-01', '2020-12-31')

    if open_prices.empty or close_prices.empty or high_prices.empty or low_prices.empty or volume.empty:
        print("One or more DataFrames are empty. Please check the data download process.")
    else:
        data_flat = prepare_data(open_prices, close_prices, high_prices, low_prices, volume)
        data = pd.read_csv('data1_flat.csv')
        data.index = data['Date']
        data = data.drop(columns=['Date'])

        data = calculate_indicators(data)
        data = calculate_forward_returns(data)
        print(data)

if __name__ == "__main__":
    main()
