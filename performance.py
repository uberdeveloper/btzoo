"""
This module calculates different performance metrics for the 
backtests and saves them in a summary folder
"""
import os
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
	df = df.loc['2012':]
	return df

def main():
	benchmark = get_benchmark()
	DIR = '/media/machine/4E1EA2D152455460/temp/btzoo_results/results'
	counter = 0
	for root,directory,files in os.walk(DIR):
		for file in files:
			if file.endswith('h5'):
				filename = os.path.join(root, file)
				dct = {}
				results = pd.read_hdf(filename)
				byday = results.groupby('timestamp').net_profit.sum()
				stats = pf.timeseries.perf_stats(byday/100000,
					factor_returns=benchmark.chg)
				dct.update(stats.to_dict())
				simple = metrics(results)
				dct.update(simple)
				dct['open==high'] = results.query('open==high').net_profit.sum()
				dct['open==low'] = results.query('open==low').net_profit.sum()
				by_year = byday.groupby(lambda x: x.year).sum()
				dct.update(by_year.to_dict())

if __name__ == "__main__":
	main()