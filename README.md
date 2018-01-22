# Crypto Candle Collector
A script to collect OHLCV data from a crypto-currency exchange and store it in a database.

Makes use of a YAML-based settings file (see included sample) to specify the following user-specific parameters:
* exchange_options: 
    * exchange_id: The ccxt ID of the exchange to query for market data. See the [CCXT wiki](https://github.com/ccxt/ccxt/wiki/Exchange-Markets) for a list of supported exchanges and their IDs.
    * limit: The number of candlesticks to collect from the exchange API.
    * timeframe: The size of the candles (i.e. 1 minute, 5 minutes, 1 hour, etc.). The timeframes attribute of each ccxt.exchange lists the supported timeframes for a given exchange. See the [CCXT](https://github.com/ccxt/ccxt/wiki/Manual#exchange-structure) for additional information.
    * mysql_connection:
        * dbname: The name of the MySQL database where Tweet data will be stored.
        * host: The MySQL database host name.
        * password: The MySQL database password.
        * results_table: The name of the table where market data for each cashtag will be stored.
        * tweets_table: The name of the table where the Tweet data will be stored.
        * user: The MySQL database user name.