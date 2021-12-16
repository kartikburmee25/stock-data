import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import yfinance as yf


# Connecting to database

#db_password = os.environ.get('DB_PASSWORD')
db_password = '123456'
engine = create_engine('postgresql://postgres:{}@localhost/stock-data'.format(db_password))

# Inserting pandas dataframe for a stock into DB as a new table
def create_prices_table(symbol, period):

    df = yf.download(symbol,period=period)
    df.reset_index(inplace=True)
    df = df.rename(columns = {'index':'Date'})

    # writing the data into database
    df.to_sql('daily_prices', con=engine, schema='public', if_exists='replace', index=False)

    # Create a primary key on the table
    query = """ALTER TABLE daily_prices 
              ADD PRIMARY KEY (Date);"""
    engine.execute(query)
    
    return 'Daily prices table created'


# function to insert data into existing prices table
def import_stock_data_file(symbol):
    path = bars_path + '/{}.csv'.format(symbol)
    df = pd.read_csv(path, index_col=[0], parse_dates=[0])
    
    # Some clean up for missing data
    if 'dividend' not in df.columns:
        df['dividend'] = 0
    df = df.fillna(0.0)
    
    # First part of the insert statement
    insert_init = """INSERT INTO daily_prices
                    (symbol, date, volume, open, close, high, low, dividend, ratio_adj,
                    volume_adj, open_adj, close_adj, high_adj, low_adj, dollar_volume)
                    VALUES
                """
                
    # Add values for all days to the insert statement
    vals = ",".join(["""('{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', 
                     '{}', '{}', '{}')""".format(
                     symbol,
                     date,
                     row.volume,
                     row.open,
                     row.close,
                     row.high,
                     row.low,
                     row.dividend,
                     row.ratio_adj,
                     row.volume_adj,
                     row.open_adj,
                     row.close_adj,
                     row.high_adj,
                     row.low_adj,
                     row.dollar_volume
                     ) for date, row in df.iterrows()])
    
    # Handle duplicate values - Avoiding errors if you've already got some data in your table
    insert_end = """ ON CONFLICT (symbol, date) DO UPDATE 
                SET
                volume = EXCLUDED.volume,
                open = EXCLUDED.open,
                close = EXCLUDED.close,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                dividend = EXCLUDED.dividend,
                ratio_adj = EXCLUDED.ratio_adj,
                volume_adj = EXCLUDED.volume_adj,
                open_adj = EXCLUDED.open_adj,
                close_adj = EXCLUDED.close_adj,
                high_adj = EXCLUDED.high_adj,
                low_adj = EXCLUDED.low_adj,
                dollar_volume =  EXCLUDED.dollar_volume;
                """

    # Put together the query string
    query = insert_init + vals + insert_end
    
    # Fire insert statement
    engine.execute(query)

#create_prices_table('AAPL','1y')
