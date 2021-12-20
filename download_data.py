import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import yfinance as yf


# Connecting to database

db_password = os.environ.get('DB_PASSWORD')

engine = create_engine('postgresql://postgres:{}@localhost/stock-data'.format(db_password))

# Inserting pandas dataframe for a stock into DB as a new table
def store_prices_data(symbol, period, table_name):

    df = yf.download(symbol,period=period)
    df.insert(0, 'Symbol', symbol)
    df.reset_index(inplace=True)
    df = df.rename(columns = {'index':'Date'})

    print(df)

    # writing the data into database
    df.to_sql(table_name, con=engine, schema='public', if_exists='append', index=False)
 
    return


# function to insert data into existing prices table from csv file
def import_stock_data_file(file_path, table_name):
    
    df = pd.read_csv(file_path)
    
    # writing the data into database
    df.to_sql(table_name, con=engine, schema='public', if_exists='append', index=False)

    return


def parse_index_symbols(index_name):

	df = pd.read_csv(index_name + '.csv')
	symbol_list = df['Symbol'].tolist()

	return symbol_list


if __name__ == '__main__':

	# download and store nasdaq 100 data for past 5 years

	nasdaq_100 = parse_index_symbols('nasdaq_100')
	for symbol in nasdaq_100:
		store_prices_data(symbol, '5y')

