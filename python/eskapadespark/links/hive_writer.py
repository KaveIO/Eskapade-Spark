"""Project: Eskapade - A python-based package for data analysis

Class: HiveWriter

Created: 2016/11/08

Description:
     Algorithm to write a dataframe in the datastore into a Hive table.

Authors:
     KPMG Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import pyspark

from eskapade import process_manager, DataStore, Link, StatusCode
from eskapadespark import SparkManager, data_conversion


class HiveWriter(Link):
    """Link to write a dataframe in the datastore into a Hive table."""

    def __init__(self, **kwargs):
        """Initialize link instance.

        :param str name: name of link
        :param str read_key: key of data to read from data store
        :param str db: hive database name
        :param str table: hive table name
        :param dict schemSpec: if writing spark rdd, schema of hive types
        :param str prefix: prefix for hive column names
        :param list column_names_not_to_change: column names not to give the prefix
        :param list columns: columns to store in hive. If empty all columns will be stored
        :param list not_columns: columns to store not in hive
        :param list change_column_names: columns only to add prefix to
        """
        # initialize Link
        Link.__init__(self, kwargs.pop('name', 'HiveWriter'))

        # process keyword arguments
        self._process_kwargs(kwargs, read_key='', db='', table='', schema_spec=None, prefix='',
                             column_names_not_to_change=[], columns=[], not_columns=[], change_column_names=[])
        self.check_extra_kwargs(kwargs)

    def initialize(self):
        """Initialize the link."""

        # check input arguments
        self.check_arg_types(read_key=str, db=str, table=str)
        self.check_arg_types(allow_none=True, schema_spec=dict)
        self.check_arg_vals('read_key', 'db', 'table')

        return StatusCode.Success

    def execute(self):
        """Execute the link."""
        spark = process_manager.service(SparkManager).get_session()
        data = process_manager.service(DataStore)[self.read_key]

        if not isinstance(data, pyspark.sql.DataFrame):
            data = data_conversion.create_spark_df(spark, data, schema=self.schema_spec)
        if self.columns:
            data = data.select(self.columns)
        if self.not_columns:
            data = data.select(*[c for c in data.columns if c not in self.not_columns])
        if self.change_column_names:
            for c, c_to_be in self.change_column_names:
                data = data.withColumnRenamed(c, c_to_be)
        if self.column_names_not_to_change:
            for c in data.columns:
                if c not in self.column_names_not_to_change:
                    data = data.withColumnRenamed(c, self.prefix + c)
        data_conversion.hive_table_from_df(spark, data, self.db, self.table)

        return StatusCode.Success
