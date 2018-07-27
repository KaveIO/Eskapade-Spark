"""Project: Eskapade - A python-based package for data analysis.

Class: SparkWithColumn

Created: 2018-03-08

Description:
    SparkWithColumn adds the output of a column expression (column operation,
    sql.functions function, or udf) to a dataframe.

Authors:
    KPMG Advanced Analytics & Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from pyspark.sql.column import Column

from eskapade import process_manager, DataStore, Link, StatusCode


class SparkWithColumn(Link):
    """Create a new column from columns in a Spark dataframe

    SparkWithColumn adds the output of a column expression (column operation,
    sql.functions function, or udf) to a dataframe.
    """

    def __init__(self, **kwargs):
        """Initialize SparkWithColumn instance

        :param str name: name of link
        :param str read_key: key of data to read from data store
        :param str store_key: key of data to store in data store
        :param str new_col_name: name of newly created column
        :param Column new_col: the column object to be included in the dataframe, resulting from a column expression
        """

        # initialize Link, pass name from kwargs
        Link.__init__(self, kwargs.pop('name', 'SparkWithColumn'))

        # Process and register keyword arguments. If the arguments are not given, all arguments are popped from
        # kwargs and added as attributes of the link. Otherwise, only the provided arguments are processed.
        self._process_kwargs(kwargs, read_key='', store_key='', new_col_name='', new_col=None)

        # check residual kwargs; exit if any present
        self.check_extra_kwargs(kwargs)
        # Turn off the line above, and on the line below if you wish to keep these extra kwargs.
        # self._process_kwargs(kwargs)

    def initialize(self):
        """Initialize the link.

        :returns: status code of initialization
        :rtype: StatusCode
        """

        # check input arguments
        self.check_arg_types(read_key=str, store_key=str, new_col=Column)
        self.check_arg_vals('read_key', 'new_col_name')
        if not self.store_key:
            self.store_key = self.read_key

        return StatusCode.Success

    def execute(self):
        """Execute the link.

        :returns: status code of execution
        :rtype: StatusCode
        """
        ds = process_manager.service(DataStore)

        # --- your algorithm code goes here
        self.logger.debug('Now executing link: {link}.', link=self.name)

        spark_df = ds[self.read_key]

        # apply function
        new_spark_df = spark_df.withColumn(self.new_col_name, self.new_col)

        # store updated data frame
        ds[self.store_key] = new_spark_df

        return StatusCode.Success

    def finalize(self):
        """Finalize the link.

        :returns: status code of finalization
        :rtype: StatusCode
        """
        # --- any code to finalize the link follows here

        return StatusCode.Success
