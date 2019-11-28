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


