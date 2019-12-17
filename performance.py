"""
This module calculates different performance metrics for the 
backtests and saves them in a summary folder
"""

import pandas as pd
import numpy as np
import pyfolio as pf
from fastbt.rapid import metrics

def get_benchmark(filename='indices.csv', parse_dates=['Date']):
	"""
	Get the benchmark returns
	filename
		filename with date and ohlc columns
	parse_dates
		columns to parse dates as list
	returns calculated on the close column
	"""
	df = pd.read_csv(filename, parse_dates=parse_dates)
	df.rename(lambda x: x.lower(), axis='columns', inplace=True)
	df = df.set_index('date').sort_index()
	df['chg'] = df.close.pct_change(1)
	return df

def main():
	benchmark = get_benchmark()

if __name__ == "__main__":
	main()