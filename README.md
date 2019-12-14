# btzoo

## Introduction

A repository to create automated strategy and backtests.

Specify a simple yaml file and setup a directory structure for
testing different variations of backtest parameters

## Configuration

**config.yaml** file loads the configuration. The following options are
provided

### index_file
a HDF5 file with index name as keys. Each key must contain date and
index constituents on that date as date and symbol columns

### data_file
a HDF5 file with all data. The file must only have a single key 
(data source)

### output_dir
output directory to save files.
Separate files are generated for each of the indices

## parameters.yaml file

Parameters.yaml file contains the set of parameters to iterate.
Define each parameter as a key and the possible set of values as
values in the format `parameter: [value1, value2]`.
In case of a single value, you can specify it as simple key-value
pair `parameter:value`.
In case of conditional parameters, where the a set of parameters are 
dependent upon the value of otherparameters, use a nested dictionary 
in the form
```
parameter1:
	value1:
		parameter2: [value1, value2]
	value2:
		parameter3: [value1, value2]
```
* The nested parameter should be the last declaration in the file
* Only one nested parameter is supported
* The nested parameter is only tested with the order argument


