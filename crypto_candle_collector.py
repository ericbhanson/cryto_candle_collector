import ccxt
import datetime
import math
import sqlalchemy
import sys
import time
import yaml


def connect_to_db(db_settings):
    protocol = db_settings['protocol']
    user = db_settings['user']
    password = db_settings['password']
    host = db_settings['host']
    dbname = db_settings['dbname']
    engine = sqlalchemy.create_engine(protocol + '://' + user + ':' + password + '@' + host + '/' + dbname,
                                      pool_pre_ping=True, pool_recycle=30)
    db_connection = engine.connect()

    return db_connection


def create_since_ts(db_connection, granularity, settings):
    table = get_table(db_connection, 'testing_data')
    limit = 1
    order_by = sqlalchemy.desc('utc_timestamp')
    select = [table.c.utc_timestamp]
    where = table.c.timeframe == granularity
    query = sqlalchemy.select(select).where(where).order_by(order_by).limit(limit)
    results = db_connection.execute(query).fetchall()

    if not results:
        since = datetime.datetime.strptime(settings, '%Y-%m-%d %H:%M:%S')

    else:
        since = results[0][0] + datetime.timedelta(seconds=granularity)

    return since


def create_testing_data_list(exchange, granularity, limit, request_loops, since, timeframe):
    testing_data_list = []
    timezone = datetime.timezone.utc

    print('Running through {} loops to obtain the requested amount of data...'.format(request_loops))

    for i in range(request_loops):
        print('Starting loop number {}...'.format(i))

        start_ts = since + datetime.timedelta(seconds=granularity * limit * i)
        start_ts = start_ts.replace(tzinfo=timezone).timestamp() * 1000

        for j in range(1, 6):

            try:
                uohlcv_list = exchange.fetch_ohlcv(symbol, limit=limit, since=start_ts, timeframe=timeframe)

            except ccxt.RequestTimeout:
                error_message = 'Request to {} failed on a time out. Waiting 60 seconds and trying again. This is ' \
                                'attempt number {} of 5.'.format(exchange.name, j)
                print(error_message)

                time.sleep(60)

            else:
                for uohlcv in uohlcv_list:
                    uohlcv_dict = create_uohlcv_dict(granularity, uohlcv)
                    testing_data_list.append(uohlcv_dict)

                break

        time.sleep(exchange.rateLimit / 1000)

    return testing_data_list


def create_uohlcv_dict(granularity, uohlcv):
    close_price = float(uohlcv[4])
    high_price = float(uohlcv[2])
    low_price = float(uohlcv[3])
    open_price = float(uohlcv[1])
    utc_timestamp = datetime.datetime.utcfromtimestamp(uohlcv[0] // 1000)
    volume = float(uohlcv[5])
    uohlcv_dict = {'close': close_price, 'high': high_price, 'low': low_price, 'open': open_price,
                   'symbol': symbol, 'timeframe': granularity, 'utc_timestamp': utc_timestamp,
                   'volume': volume}

    return uohlcv_dict


def get_table(db_connection, table_name):
    metadata = sqlalchemy.MetaData(bind=db_connection)
    table = sqlalchemy.Table(table_name, metadata, autoload=True, autoload_with=db_connection)

    return table


def load_settings(file_location):
    with open(file_location + 'settings.yaml', 'rb') as settings_file:
        yaml_settings = settings_file.read()
        settings = yaml.load(yaml_settings)

    return settings


def return_rounded_time(ts, granularity):
    date_delta = datetime.timedelta(seconds=granularity)
    round_to = date_delta.total_seconds()
    seconds = (ts - ts.min).seconds
    rounding = (seconds + round_to / 2) // round_to * round_to
    rounded_time = ts + datetime.timedelta(0, rounding - seconds, -ts.microsecond)

    return rounded_time


# Load the settings file to obtain script specifications.
settings = load_settings(sys.argv[1])

# Assign those specifications to individual variables.
exchange_name = settings['ccxt']['exchange_id']
limit = settings['ccxt']['limit']
timeframes = settings['ccxt']['timeframes']

# Load the exchange information and create a list of traded symbols.
exchange_method = getattr(ccxt, exchange_name)
exchange = exchange_method()

markets = exchange.load_markets()
symbol_list = [symbol for symbol in markets if 'USD' in symbol]

# Connect to the database.
db_connection = connect_to_db(settings['mysql_connection'])

for timeframe in timeframes:
    granularity = exchange.timeframes[timeframe]

    # Determine the start date for collecting data.
    since = create_since_ts(db_connection, granularity, settings['ccxt']['since'])

    # Every exchange has a limit to the amount of data it will return from an API call for candlesticks. To request
    # data beyond that limit, a variable number of loops is required. This code calculates that variable number,
    # based on the requested start date, the granularity of the candlestick, and the data request limit.
    adj_now = return_rounded_time(datetime.datetime.utcnow(), granularity)
    request_loops = int(math.ceil((adj_now - since).total_seconds() / (granularity * limit)))

    # Request the data for each symbol in the symbol list from the exchange API and store the results in a database.
    for symbol in symbol_list:
        print('Getting data for the symbol {} at the {} seconds granularity...'.format(symbol, granularity))

        testing_data_list = create_testing_data_list(exchange, granularity, limit, request_loops, since, timeframe)
        table = get_table(db_connection, 'testing_data')
        insert_query = table.insert(testing_data_list)
        result = db_connection.execute(insert_query)

        print('Inserted with key {}.'.format(result.inserted_primary_key[0]))
        print('\n')