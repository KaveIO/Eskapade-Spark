"""Project: Eskapade - A python-based package for data analysis

Class: HiveReader

Created: 2016/11/08

Description:
     Algorithm to read a Hive table into a Spark dataframe.

Authors:
     KPMG Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from collections import OrderedDict as odict

from eskapade import process_manager, DataStore, Link, StatusCode
from eskapadespark import SparkManager


class HiveReader(Link):
    """Link to read a Hive table into a Spark dataframe."""

    def __init__(self, **kwargs):
        """Initialize link instance.

        :param str databaseName: name of the hive database
        :param str tableName: name of the hive table
        :param str store_key: key of data to store in data store
        :param list columns: hive columns to read. If empty all columns will be queried.
        :param str selection: where clause of the hive query
        :param str limit: limit clause of the hive query
        :param dict processFuncs: process spark functions after query
        :param str full_query: if not empty execute only this querystring
        :param str hive_sql_file: path to an hive.sql file. If not empty the query in this file will be executed
        """

        # initialize Link
        Link.__init__(self, kwargs.pop('name', 'HiveReader'))

        # process keyword arguments
        self._process_kwargs(kwargs, databaseName='', tableName='', store_key='', columns=[], selection='',
                             limit='', processFuncs={}, full_query='', hive_sql_file='')
        self.check_extra_kwargs(kwargs)

    def initialize(self):
        """Initialize the link."""

        # check settings
        self.check_arg_types(processFuncs=dict, **dict((k, str)
                             for k in ['databaseName', 'tableName', 'store_key', 'selection']))
        self.check_arg_types(True, columns=str)
        if not (self.full_query or self.hive_sql_file):
            self.check_arg_vals('tableName', 'store_key')

        # set post-processing functions
        procFuncs = odict()
        if self.processFuncs.pop('cache', None) is not None:
            if 'persist' not in self.processFuncs:
                procFuncs['cache'] = {}
        if 'persist' in self.processFuncs:
            procFuncs['persist'] = self.processFuncs.pop('persist')
        for func, kwargs in self.processFuncs.items():
            if func == 'rdd':
                procFuncs[lambda df: df.rdd] = {}
            else:
                procFuncs[func] = kwargs
        self.processFuncs = procFuncs

        return StatusCode.Success

    def execute(self):
        """Execute the link."""

        # construct Hive query
        if self.full_query:
            readQuery = self.full_query
            self.logger.debug('Executing query "{query}" into dataframe "{key}".',
                              query=self.full_query, key=self.store_key)
        elif self.hive_sql_file:
            with open(self.hive_sql_file, 'r') as f:
                readQuery = f.read()
            self.logger.debug('Executing query "{query}" into dataframe "{key}".',
                              query=readQuery, key=self.store_key)
        else:
            tableSpec = self.tableName if not self.databaseName else '%s.%s' % (self.databaseName, self.tableName)
            cols = '*' if not self.columns else self.columns if isinstance(self.columns, str)\
                   else ','.join(c for c in self.columns)
            readQuery = 'select %s from %s' % (cols, tableSpec)
            if self.selection:
                readQuery += ' where %s' % self.selection
            if self.limit:
                readQuery += ' limit %s' % self.limit

            # output debug info
            self.logger.debug('Reading Hive table "{table}" into dataframe "{key}."\n'
                              'Reading columns: {columns}.\n'
                              'Applying selection: {selection}.\n', table=tableSpec, key=self.store_key,
                              columns=cols, selection=self.selection if self.selection else 'none')

        # create Spark dataframe from Hive table
        df = process_manager.service(SparkManager).get_session().sql(readQuery)
        df = eskapadespark.helpers.apply_transform_funcs(df, [(f, (), a) for f, a in self.processFuncs.items()])
        process_manager.service(DataStore)[self.store_key] = df

        return StatusCode.Success

    def add_proc_func(self, func, **kwargs):
        self.processFuncs[func] = kwargs
