"""Project: Eskapade - A python-based package for data analysis.

Class: ExampleLink

Created: 2018-03-08

Description:
    Each feature given from the input df will by default correspond to 6 columns
    in the output: min, mean, max, stddev, count, and sum. The columns are named
    like 'feature_stddev_0d' (0d since we look 0 days back into the past).

    The new dataframe will also contain the column `new_date_col` with the date,
    and all the identifying columns given in `partitionby_cols`.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from pyspark.sql import functions as f

from eskapade import process_manager, DataStore, Link, StatusCode


class DailySummary(Link):
    """Creates daily summary information from a timeseries dataframe.

    Each feature given from the input df will by default correspond to 6 columns
    in the output: min, mean, max, stddev, count, and sum. The columns are named
    like 'feature_stddev_0d' (0d since we look 0 days back into the past).

    The new dataframe will also contain the column `new_date_col` with the date,
    and all the identifying columns given in `partitionby_cols`."""

    def __init__(self, **kwargs):
        """Initialize an instance.

        :param str name: name of link
        :param str read_key: key of input data to read from data store
        :param str store_key: key of output data to store in data store
        :param list/dict feature_cols: columns to take daily aggregates of. If list, all columns in the
            list are aggregated with the min, mean, max, stddev, count, and sum. If dict, the keys are
            column names to aggregate, and the values are lists of aggregation functions to apply. These
            must be built in spark aggregation functions.
        :param str new_date_col: name of the 'date' column which will be created (default 'date')
        :param str datetime_col: name of column with datetime information in the dataframe
        :param list partitionby_cols: identifying columns to partition by before aggregating
        """
        # initialize Link, pass name from kwargs
        Link.__init__(self, kwargs.pop('name', 'DailySummary'))

        # Process and register keyword arguments. If the arguments are not given, all arguments are popped from
        # kwargs and added as attributes of the link. Otherwise, only the provided arguments are processed.
        self._process_kwargs(kwargs, read_key=None, store_key=None, feature_cols=[], new_date_col='date',
                             datetime_col=None, partitionby_cols=[])

        # check residual kwargs; exit if any present
        self.check_extra_kwargs(kwargs)
        # Turn off the line above, and on the line below if you wish to keep these extra kwargs.
        # self._process_kwargs(kwargs)

    def initialize(self):
        """Initialize the link.

        :returns: status code of initialization
        :rtype: StatusCode
        """
        return StatusCode.Success

    def execute(self):
        """Execute the link.

        :returns: status code of execution
        :rtype: StatusCode
        """
        ds = process_manager.service(DataStore)

        # --- your algorithm code goes here
        self.logger.debug('Now executing link: {link}.', link=self.name)

        def col_name(feat, func):
            return '_'.join([feat, func.__name__, '0d'])

        df = ds[self.read_key]

        default_agg_funcs = [f.min, f.mean, f.max, f.stddev, f.count, f.sum]

        # aggregation functions to do daily
        if type(self.feature_cols) is list:
            agg_funcs = default_agg_funcs

            # resulting columns named like 'feat1_mean_0d'
            agged_cols = [func(df[feat]).alias(col_name(feat, func))
                          for func in agg_funcs for feat in self.feature_cols]
        else:
            agged_cols = []
            for feat, agg_funcs in self.feature_cols.items():
                # resulting columns named like 'feat1_func_0d'
                if not agg_funcs:
                    agg_funcs = default_agg_funcs

                agged_cols += [func(df[feat]).alias(col_name(feat, func))
                               for func in agg_funcs]

        # columns to group by
        gb_cols = self.partitionby_cols + [self.new_date_col]

        df_agged = df.withColumn(self.new_date_col, f.to_date(self.datetime_col)) \
            .groupBy(gb_cols) \
            .agg(*agged_cols)

        ds[self.store_key] = df_agged

        return StatusCode.Success

    def finalize(self):
        """Finalize the link.

        :returns: status code of finalization
        :rtype: StatusCode
        """
        # --- any code to finalize the link follows here

        return StatusCode.Success
