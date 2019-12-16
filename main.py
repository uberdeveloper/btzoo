import pandas as pd
import os
import yaml
import json
import hashlib
import concurrent.futures
from itertools import product
from fastbt.datasource import DataSource
from fastbt.rapid import backtest

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
    for i in [1,2,3]:
        ds.add_pct_change(on='close', period=i, col_name='ret'+str(i))
    for i in [2,3]:
        ds.add_rolling(on='tottrdval', window=i, col_name='vol'+str(i),
            function='sum', lag=1)
    for col in ['tottrdval', 'perdel', 'qtrd']:
        ds.add_lag(on=col, period=1, col_name='prev_'+col) 
    return ds.data


def create_files(index_file, data_file, output_dir, is_transform=False):
    """
    Create the files necessary for running the backtest
    index_file
        a HDF5 file with keys as indexes with date and symbol
    data_file
        a HDF5 file with all the available data with a 
        single key named as data
    output_dir
        output directory to save the files
    is_transform
        Boolean - True/False
        Whether to transform the data. If True, data transformations
        are applied with the default function and then saved

    Note
    ----
    This function reads data from each key of the index file and does
    a merge on the data_file with all the data and then saves them as 
    individual files in the output directory
    """
    store = pd.HDFStore(index_file)
    data = pd.read_hdf(data_file).rename(columns={'timestamp': 'date'})
    for key in store.keys():
        index_data = store.get(key)
        df = index_data.merge(data, on=['date', 'symbol'])
        if is_transform:
            df = transform(df)
        # No slashes since its already included in key
        filename = '{o}{fn}.h5'.format(o=output_dir, fn=key)
        print(filename)
        df.to_hdf(filename, key='data', format='fixed')
    store.close()


def unpack_parameters(dict_of_parameters, key1=None):
    """
    Generate a list of parameters for backtest function
    dict_of_parameters
        dictionary of parameters in the given format
        see the README files for details
    Given a list of parameters, unpack them into list of
    dictionareis for further function
    Note
    -----
    1) This is not a generalized function
    """
    lst = []
    d = dict_of_parameters.copy()
    for k,v in d.items():
        if isinstance(v, (str, int, float)):
            lst.append([{k:v}])
        elif isinstance(v, list):
            L = [{k:l} for l in v]
            lst.append(L)
        elif isinstance(v, dict):
            if key1:
                v[key1] = k
            L = unpack_parameters(v, key1=k)
            lst.append(L)
    return lst

def generate_parameters(lsts):
    """
    Given a list of dictionaries, generate a combined list of
    all possible parameters
    lsts
        list of dictionaries from unpack_parameters function
    returns a list of dictionaries that could be passed on to
    the backtest function as kwargs
    Note
    -----
    1) Expects each list to have dictionaries. Flatten in case
    of multiple lists
    2) This doesn't yield a generator and all the possible options
    are loaded into memory.
    3) In case of lists with repeated arguments, the last argument
    is taken as valid
    """
    all_dcts = list(product(*lsts))  
    def inner(X):
        """
        an inner function that takes the list of dictionaries,
        unpacks them and creates one single dictionary
        """
        empty_dict = {}
        for m in X:
            empty_dict.update(m)
        return empty_dict
    return [inner(x) for x in all_dcts]

def create_parameters(filename='params.yaml'):
    """
    Creates a list of parameters for running the backtest 
    function in batch
    filename
        full path to the params.yaml file. If not specified,
        the file in the present working directory is taken
    returns a list of parameters as dictionary
    Note
    ----
    This function does the following
    1) Load the yaml file
    2) Unpack the parameters
    3) Generate the parameters for the backtest function
    """
    with open(filename, 'r') as f:
        params = yaml.safe_load(f)
    list_of_params = unpack_parameters(params)
    # Generate parameters for keys without nesting
    singular = generate_parameters(list_of_params[:-1])
    # Generate parameters for keys with nesting
    # This is assumed to be the last list by default
    all_parameters = []
    for nested_params in list_of_params[-1]:
        N = generate_parameters(nested_params)
        merged_params = generate_parameters([singular, N])
        all_parameters.extend(merged_params)
    # Sort all by keys for hashing purpose
    all_parameters = [{k:v for k,v in sorted(p.items(), key=lambda x:x[0])}
    for p in all_parameters]
    return all_parameters

def get_hash(params_dict):
    """
    Get a unique has for the given dictionary
    params_dict
        a python dictionary
    TO DO:
    Add sort for hash
    """    
    txt = str(params_dict).encode()
    return hashlib.sha1(txt).hexdigest()

def load_data(datapath):
    """
    Load all HDF5 files with extension h5 in the given directory
    datapath
        directory path for the files; usually the output directory
    returns a dictionary with all the HDF5 files loaded
    """
    data_dict = {}
    ext = 'h5'
    for root,directory,files in os.walk(datapath):
        for file in files:
            if file.endswith(ext):
                key = file.split('.')[0]
                filename = os.path.join(root, file)
                data_dict[key] = pd.read_hdf(filename)
    return data_dict

def runner(data, universe, params, counter):
    p = params.copy()
    p['universe'] = universe
    params_path = os.path.join(os.getenv('HOME'), 'output', 'parameters')
    results_path = os.path.join(os.getenv('HOME'), 'output', 'results')
    identifier = get_hash(p)
    results = backtest(data=data, **params)
    print(counter, universe, params)
    with open('{}/{}.json'.format(params_path, identifier), 'w') as f:
        json.dump(p, f)
    results.to_hdf('{}/{}.h5'.format(results_path, identifier),
        key='data', format='fixed')

def check_paths():
    """
    Check whether output path exists for saving files
    exist and if not, create the respective directories
    """
    home = os.getenv('HOME')
    paths = [
        os.path.join(home, 'output'),
        os.path.join(home, 'output', 'parameters'),
        os.path.join(home, 'output', 'results')
    ]
    for pth in paths:
        if not(os.path.exists(pth)):
            os.mkdir(pth)


def main():    
    if not(IS_DATA):
        # create data if already not created
        create_files(INDEX_FILE, DATA_FILE, OUTPUT_DIR, is_transform=True)    
    datas = load_data(OUTPUT_DIR)
    all_parameters = create_parameters()
    check_paths()
    counter = 0
    for k,v in datas.items():
        with concurrent.futures.ProcessPoolExecutor() as executor:
            for params in all_parameters:
                counter+=1
                executor.submit(runner, v, k, params, counter)


if __name__ == "__main__":
    
    # Expect a config.yaml in the present working directory
    with open('config.yaml') as f:
        config = yaml.safe_load(f)
    # Set all GLOBAL CONSTANTS here
    INDEX_FILE = config['index_file']
    DATA_FILE = config['data_file']
    OUTPUT_DIR = config['output_dir']
    IS_DATA = config['is_data']
    main()