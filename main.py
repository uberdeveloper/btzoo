import pandas as pd
import os
import yaml
from fastbt.datasource import DataSource

def transform(data):
    """
    Apply the necessary transformation to the given data
    """
    ds = DataSource(data, timestamp='date')
    for i in range(2,8):
        ds.add_rolling(on='high', window=i, col_name='rmax'+str(i),
            function='max', lag=1)
        ds.add_rolling(on='low', window=i, col_name='rmin'+str(i),
            function='min', lag=1)
    ds.add_formula('(open/prevclose)-1', col_name='pret')
    ds.add_formula('(close/open)-1', col_name='idret')
    ds.add_formula('(tottrdval/totaltrades)', col_name='qtrd')
    ds.add_pct_change(on='close', col_name='ret')
    for col in ['ret', 'tottrdval', 'perdel', 'qtrd']:
        ds.add_lag(on=col, period=1, col_name='prev'+col) 
    return ds.data


def create_files(index_file, data_file, output_dir):
    """
    Create the files necessary for running the backtest
    index_file
        a HDF5 file with keys as indexes with date and symbol
    data_file
        a HDF5 file with all the available data with a 
        single key named as data
    output_dir
        output directory to save the files
    Note
    ----
    This function reads each key of the index file and does
    a merge on the data_file and then saves them as 
    individual files in the output directory
    """
    store = pd.HDFStore(index_file)
    data = pd.read_hdf(data_file).rename(columns={'timestamp': 'date'})
    for key in store.keys():
        index_data = store.get(key)
        df = index_data.merge(data, on=['date', 'symbol'])
        # No slashes since its already included in key
        filename = '{o}{fn}.h5'.format(o=output_dir, fn=key)
        print(filename)
        df.to_hdf(filename, key='data', format='fixed')
    store.close()

def main():
    # Expect a config.yaml in the present working directory
    with open('config.yaml') as f:
        config = yaml.safe_load(f)
    print(config)
    INDEX_FILE = config['index_file']
    DATA_FILE = config['data_file']
    OUTPUT_DIR = config['output_dir']
    create_files(INDEX_FILE, DATA_FILE, OUTPUT_DIR)

if __name__ == "__main__":
    main()