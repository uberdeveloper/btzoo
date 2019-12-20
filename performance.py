"""
This module calculates different performance metrics for the 
backtests and saves them in a summary folder
"""
import os
import json
import concurrent.futures
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
	dct['open_high'] = result.query('open==high').net_profit.sum()
	dct['open_low'] = result.query('open==low').net_profit.sum()
	by_year = byday.groupby(lambda x: x.year).sum()
	dct.update(by_year.to_dict())
	return dct

def runner(filename, output_file, benchmark, counter):
	"""
	Runner for concurrent execution
	"""
	results = pd.read_hdf(filename)
	perf_stats = all_metrics(results, benchmark)
	print(counter, output_file)
	with open(output_file, 'w') as f:
		json.dump(perf_stats, f)



def main():
	benchmark = get_benchmark()
	counter = 0
	for root,directory,files in os.walk(DIR):
		for file in files:
			if file.endswith('h5'):
				filename = os.path.join(root, file)
				output_filename = os.path.join(OUTPUT_DIR, file.split('.')[0])+'.json'
				counter+=1
				# Using thread pool since this is an I/O bound task
				with concurrent.futures.ThreadPoolExecutor() as executor:
					executor.submit(runner, filename, output_filename, benchmark, counter)

if __name__ == "__main__":
	try:
		from google.cloud import storage
		GOOGLE_CLOUD = True
	except ImportError:
		print('Google Cloud library not installed')
		GOOGLE_CLOUD = False

	DIR = '/media/machine/4E1EA2D152455460/temp/btzoo_results/results'
	OUTPUT_DIR = '/media/machine/4E1EA2D152455460/temp/btzoo_results/summary'
	main()