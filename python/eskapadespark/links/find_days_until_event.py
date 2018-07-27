"""Project: Eskapade - A python-based package for data analysis.

Class: ExampleLink

Created: 2018-03-08

Description:
    Will create a new column (name given by `countdown_col_name`) containing
    the number of days between the current row and the next date on which
    `event_col` is greater than 0. The dataframe must include a column that has
    a date or datetime.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from pyspark.sql import functions as f
from pyspark.sql import Window

from eskapade import process_manager, DataStore, Link, StatusCode


class FindDaysUntilEvent(Link):
    """Find the number of days until an event in a spark dataframe.

    Will create a new column (name given by `countdown_col_name`) containing
    the number of days between the current row and the next date on which
    `event_col` is greater than 0. The dataframe must include a column that has
    a date or datetime.
    """

    def __init__(self, **kwargs):
        """Find the number of days until a particular event in an ordered dataframe.

        :param str name: name of link
        :param str read_key: key of input data to read from data store
        :param str store_key: key of output data to store in data store
        :param str datetime_col: column with datetime information
        :param str event_col: the column containing the events (0 for rows with
            no events, >0 otherwise)
        :param str countdown_col_name: column where the number of days until the
            next event will be stored
        :param str partitionby_cols: columns to partition the countdown by
        """

        # initialize Link, pass name from kwargs
        Link.__init__(self, kwargs.pop('name', 'FindDaysUntilEvent'))

        # Process and register keyword arguments. If the arguments are not given, all arguments are popped from
        # kwargs and added as attributes of the link. Otherwise, only the provided arguments are processed.
        self._process_kwargs(kwargs, read_key=None, store_key=None,
                             datetime_col=None, event_col=None,
                             countdown_col_name='days_until_event',
                             partitionby_cols=None)

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

        df = ds[self.read_key]

        window = Window.orderBy(self.datetime_col) \
            .rowsBetween(0, Window.unboundedFollowing)

        if self.partitionby_cols:
            window = window.partitionBy(self.partitionby_cols)

        date_col = f.to_date(self.datetime_col)  # only dates per row
        reset_col = f.when(f.col(self.event_col) > 0, date_col)  # dates of events
        bfilled_reset_col = f.first(reset_col, ignorenulls=True).over(window)  # backfilled dates of events
        countdown_col = f.datediff(bfilled_reset_col, date_col)  # difference between next event date and today

        ds[self.store_key] = df.withColumn(self.countdown_col_name, countdown_col)

        return StatusCode.Success

    def finalize(self):
        """Finalize the link.

        :returns: status code of finalization
        :rtype: StatusCode
        """
        # --- any code to finalize the link follows here

        return StatusCode.Success
