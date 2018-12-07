"""Project: Eskapade - A python-based package for data analysis.

Macro: esk609_map_df_groups

Created: 2017/06/20

Description:
    This macro demonstrates techniques for flattening a time-series in Spark.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import os
from pyspark.sql import functions as f, Window

from eskapade import ConfigObject, process_manager, Chain
from escore.core import persistence
from escore.core_ops import DsToDs
from eskapadespark import SparkDfWriter, SparkDfReader, SparkManager, FindDaysUntilEvent, DailySummary, \
    SparkWithColumn, SparkConfigurator, resources
from eskapade.logger import Logger


logger = Logger()
logger.debug('Now parsing configuration file esk611_flatten_time_series')

msg = """
This macro demonstrates techniques for flattening a time-series in Spark.

By flattening a time series, we mean turning sensor or other time series data in
various frequencies into a regularly spaced dataset suitable for training
machine learning algorithms and making predictions at regular intervals (in this
case daily).

There have been two links created to aid in this process: `FindDaysUntilEvent`
and `DailySummary`. This macro will perform a simplified analysis to demonstrate
the core concepts of time series flattening.

The basic steps to flatten the series are as follows:

1. Load the input (dummy) dataset
2. Resample, going from data of arbitrary frequency to daily data
    * To retain as much information as possible, we create daily summaries of
      each feature using various statistics
3. For every day, we calculate the number of days until the next failure on that
   machine (for regression analysis/time to failure prediction)
4. Apply sliding windows to daily data, so that each row also contains
   information about the previous days
    * Again various statistics are used to capture the characteristics of the
      data
5. Data is output to a new, flattened csv with one row per day

The input dataset, `dummy_time_series.csv`, is simulated sensor data. It
represents two machines that go through cycles which take some amount of time.
Its columns are:

* `ts`: timestamp, starts on 01-01-2017
* `machine`: 1 or 2 (there are 2 machines in the set)
* `cycle_time_sec`: the time for that cycle to complete, in seconds
* `failure`: 0 or 1, whether the machine failed on that cycle

The flattened dataset is output to `../data/dummy_time_series_flattened.csv.

To play with the intermediary results in the datastore after the macro has
finished, give `eskapade_run` the `-i` flag, to start an interactive IPython
session with the variables saved. You can view the first few rows of Spark
dataframes in the datastore using e.g. `df['df_input'].show()`.
"""

logger.info(msg)

################################################################################
# --- minimal analysis information
settings = process_manager.service(ConfigObject)
settings['analysisName'] = 'esk611_flatten_time_series'
settings['version'] = 0

################################################################################
# --- Analysis values, settings, helper functions, configuration flags.

#     E.g. define flags turn on or off certain chains with links.
#     by default all set to false, unless already configured in
#     configobject or vars()

# IO settings

settings['input_file'] = 'file:' + resources.fixture('dummy_time_series.csv')
settings['output_file'] = os.path.join(persistence.io_dir('results_data'), 'dummy_time_series_flat.csv')

# --- Dataset settings
# Feature to make rolling summaries of. In a more realistic example with many
# features this could be a list to loop over
settings['feature'] = 'cycle_time_sec'

# column where failures are recorded
settings['failure_col'] = 'failure'

# --- Feature settings
# how far back our sliding windows should go
settings['lookback_windows'] = [1]

################################################################################
# --- now set up the chains and links based on configuration flags

# STEP 1: Read the data

sm = process_manager.service(SparkManager)
spark = sm.create_session(eskapade_settings=settings)

conf_link = SparkConfigurator(name='SparkConfigurator', log_level='WARN')
conf_link.spark_settings = [('spark.executor.instances', '1'),
                            ('spark.executor.cores', '2'),
                            ('spark.executor.memory', '4g'),
                            ]
config = Chain('Config')
config.add(conf_link)

# Load the data from the csv into a spark dataframe
read_chain = Chain('load_data')

read_link = SparkDfReader(name='load_data', store_key='df_input', read_methods=['csv'])
read_link.read_meth_args['csv'] = (settings['input_file'],)
read_link.read_meth_kwargs['csv'] = {'header': True, 'inferSchema': True}

read_chain.add(read_link)


# STEP 2: Resample the data to make it daily
daily_chain = Chain('daily_aggregation')

# Option 1: Use the default aggregations, giving the columns to aggregate as a
# list. The default aggregations are min, mean, max, stddev, count, and sum.
# We also give `machine` as the column to partition by, since we want to get the
# summaries of individual machines separately.

summary_link1 = DailySummary(name='daily_summary1', read_key='df_input',
                             store_key='df_daily',
                             feature_cols=[settings['feature']],
                             new_date_col='date',  # won't have timestamps, but dates
                             datetime_col='ts',  # column with timestamp
                             partitionby_cols=['machine'])  # column to partition by

# Option 2: Choose our own aggregations for resampling, given with a dictionary
# of the format `{'col_name':[list of aggregation functions]}`. We can still use
# the default aggregations for some columns just by passing an empty list.

aggregation_dict = {settings['feature']: [],  # use all the default aggregations
                    'failure': [f.sum]}  # only care about number of failures

summary_link2 = DailySummary(name='daily_summary2', read_key='df_input',
                             store_key='df_daily',
                             feature_cols=aggregation_dict,
                             new_date_col='date',
                             datetime_col='ts',
                             partitionby_cols=['machine'])

# We will use option 2. The new dataframe in the datastore will have columns
# named like `cycle_time_sec_stddev_0d`. The `_0d` tells us that this is an
# aggregation over only today, so no previous days are included.

daily_chain.add(summary_link2)


# STEP 3: Find the number of days until a failure. Here we want to make a column
# that counts down the number of days until the next failure on that machine.
# This is useful if we want to do a regression analysis, predicting how long
# we have until the next failure.
#
# We count down towards the newly created column `failure_sum_0d`, since this
# column will be >0 whenever there was a failure and 0 otherwise.
#
# The newly created column `days_until_failure` will be `null` after the most
# recent failure. You can check the behaviors of the column using the following
# commands after running the macro in an interactive session:
# >>> cols = ['date','machine','failure_sum_0d','days_until_failure']
# >>> ds['df_agg'].select(cols).orderBy(['machine','date']).show(100)
countdown_link = FindDaysUntilEvent(name='days_until_failure',
                                    read_key='df_daily', store_key='df_countdown',
                                    datetime_col='date',
                                    # column we're counting down towards:
                                    event_col='failure_sum_0d',
                                    # new col name:
                                    countdown_col_name='days_until_failure',
                                    partitionby_cols=['machine'])

daily_chain.add(countdown_link)


# STEP 4: Look back in time using sliding windows. Here we loop through the
# windows sizes in `settings['lookback_windows']` and make the aggregations for
# windows of each of the chosen sizes. This adds new columns per window per
# chosen aggregation, so we get new columns like `cycle_time_sec_sum_3d` for the
# total cycle time in a window up to 3 days back.
lookback_chain = Chain('lookback_windows')

# First we move the daily dataframe to a new location in the datastore,
# `'df_agg'`, where we will add columns in a loop
copy_link = DsToDs(read_key='df_countdown', store_key='df_agg')
lookback_chain.add(copy_link)

# Now we do a selection of aggregations over each window, and add these new
# columns to our dataframe. If we had many features, we could add a second
# loop to do this operation per feature.
for days in settings['lookback_windows']:
    # Create unix timestamp col (seconds since epoch). This allows us to use
    # the `rangeBetween` window specification to include rows in the last `days`
    # days. We use this instead of `rowsBetween` in case there are any days
    # without events, since those days do not show up as rows in our aggregated
    # daily dataset.
    unix_ts_col = f.unix_timestamp(f.col('date'))

    # Create the sliding `window` object, including the `rangeBetween` between
    # `days` days ago (`-60*60*24*days`) and today (`0`).
    # Don't forget to partition by `machine`!
    window = Window.partitionBy(['machine'])\
                   .orderBy(unix_ts_col)\
                   .rangeBetween(-60*60*24*days, 0)

    # Make the summary of each feature for the last `days` days. We'll store
    # the new column objects in a `dict` of the form {'new_col_name':
    # new_col_object}, and add all the columns to `df_agg` in a loop at the end.
    #
    # We name the new columns with a suffix like `_2d` indicating that the
    # column is an aggregation of days up to 2 days back in time
    #
    # These summary columns are generally some aggregation function of the
    # corresponding `_0d` summary, over the new sliding window:
    # `f.some_func(f.col(`feat_some_func_0d`)).over(window).
    # We show a few simple examples, feel free to add your own!
    feat = settings['feature']+'_'  # convenience
    dd = '_{}d'.format(days)  # column name suffix indicating window size
    new_cols = {
        # min = min of daily mins
        feat + 'min' + dd:
            f.min(f.col(feat + 'min_0d')).over(window),

        # max = max of daily maxes
        feat + 'max' + dd:
            f.max(f.col(feat + 'max_0d')).over(window),

        # sum = sum of daily sums
        feat + 'sum' + dd:
            f.sum(f.col(feat + 'sum_0d')).over(window),

        # count = sum of daily counts
        feat + 'count' + dd:
            f.sum(f.col(feat + 'count_0d')).over(window),

        # A few more complicated examples:

        # mean = weighted mean of daily means
        feat + 'count' + dd:
            f.sum(f.col(feat + 'mean_0d') * f.col(feat + 'count_0d')).over(window)
            / f.sum(f.col(feat + 'count_0d')).over(window),

        # stddev = sqrt(weighted mean of daily variances)
        feat + 'stddev' + dd:
            f.sqrt(f.mean(f.col(feat + 'count_0d')
                          * f.col(feat + 'stddev_0d') ** 2).over(window)
                   / f.sum(f.col(feat + 'count_0d')).over(window)),
    }

    # Loop through the dictionary of new columns and add them to the aggregated
    # dataframe
    for col_name, col_obj in new_cols.items():
        add = SparkWithColumn(name='add_'+col_name,
                              read_key='df_agg', store_key='df_agg',
                              new_col_name=col_name,
                              new_col=col_obj)

        lookback_chain.add(add)


# STEP 5: Save the results
ch = Chain('save_results')

write_link = SparkDfWriter(name='write_data', read_key='df_agg', write_methods=['csv'])
write_link.write_meth_args['csv'] = (settings['output_file'],)
write_link.write_meth_kwargs['csv'] = {'header': True, 'mode': 'overwrite'}

ch.add(write_link)

################################################################################

logger.debug('Done parsing configuration file esk611_flatten_time_series')

