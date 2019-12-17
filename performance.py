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

def all_metrics(result, benchmark):
	"""
	Calculate all the metrics for the given backtest results
	result
		backtest results in the expected format - results generated
		by rapid
	"""
	dct = {}
	byday = result.groupby('timestamp').net_profit.sum()
	stats = pf.timeseries.perf_stats(byday/100000,
		factor_returns=benchmark.chg)
	dct.update(stats.to_dict())
	simple = metrics(result)
	dct.update(simple)
	dct['open==high'] = result.query('open==high').net_profit.sum()
	dct['open==low'] = result.query('open==low').net_profit.sum()
	by_year = byday.groupby(lambda x: x.year).sum()
	dct.update(by_year.to_dict())
	return dct

def main():
	benchmark = get_benchmark()
	DIR = '/media/machine/4E1EA2D152455460/temp/btzoo_results/results'
	counter = 0
	for root,directory,files in os.walk(DIR):
		for file in files:
			if file.endswith('h5'):
				filename = os.path.join(root, file)
				results = pd.read_hdf(filename)
				perf_stats = all_metrics(results, benchmark)

if __name__ == "__main__":
	main()