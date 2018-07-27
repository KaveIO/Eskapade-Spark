"""Project: Eskapade - A python-based package for data analysis.

Macro: esk607_spark_with_column

Created: 2017/06/14

Description:
    Tutorial macro for adding a new column to a Spark dataframe by applying
    a Spark built-in or user-defined function to a selection of columns
    in a Spark dataframe.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""


from pyspark.sql import types, functions

from eskapade import process_manager, ConfigObject, Chain
from eskapade.logger import Logger
from eskapadespark import SparkManager, SparkDfReader, SparkWithColumn, resources

logger = Logger()

logger.debug('Now parsing configuration file esk607_spark_with_column')

##########################################################################
# Minimal analysis information

settings = process_manager.service(ConfigObject)
settings['analysisName'] = 'esk607_spark_with_column'
settings['version'] = 0

##########################################################################
# Start Spark session

spark = process_manager.service(SparkManager).create_session(eskapade_settings=settings)

##########################################################################
# CSV and dataframe settings

# NB: local file may not be accessible to worker node in cluster mode
file_path = ['file:' + resources.fixture('dummy1.csv')]

##########################################################################
# Now set up the chains and links based on configuration flags

read = Chain('Read')

# create read link for each data file
read_link = SparkDfReader(name='ReadFile',
                          store_key='spark_df',
                          read_methods=['csv'])

# set CSV read arguments
read_link.read_meth_args['csv'] = (file_path,)
read_link.read_meth_kwargs['csv'] = dict(sep='|', header=True, inferSchema=True)

# add link to chain
read.add(read_link)

# create link to create new column
col_link = SparkWithColumn(name='UdfPower', read_key=read_link.store_key, store_key='new_spark_df')

# Power of two columns
col_link.new_col = functions.pow(functions.col('x'), functions.col('y'))
col_link.new_col_name = 'pow_xy1'

# add link to chain
add_col = Chain('AddColumn')
add_col.add(col_link)

##########################################################################

logger.debug('Done parsing configuration file esk607_spark_with_column')
