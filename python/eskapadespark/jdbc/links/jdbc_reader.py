"""Project: Eskapade - A python-based package for data analysis

Class: JdbcReader

Created: 2016/11/08

Description:
     Link to read a table from a database using JDBC

Authors:
     KPMG Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

from collections import OrderedDict as odict

import pyspark

from escore import process_manager, ConfigObject, DataStore, Link, StatusCode
from eskapadespark import SparkManager, data_conversion
from eskapadespark.jdbc.jdbcconnection import JdbcConnection


# helper functions to (un)quote names
_unquote_name = lambda s: s.replace('"', '').replace('\'', '')
_quote_name = lambda s: '"{}"'.format(_unquote_name(s))


# lookup table for Spark column types
spark_col_types = dict(TINYINT=pyspark.sql.types.ByteType,
                       SMALLINT=pyspark.sql.types.ShortType,
                       INTEGER=pyspark.sql.types.IntegerType,
                       BIGINT=pyspark.sql.types.LongType,
                       REAL=pyspark.sql.types.FloatType,
                       FLOAT=pyspark.sql.types.DoubleType,
                       DOUBLE=pyspark.sql.types.DoubleType,
                       DECIMAL=pyspark.sql.types.DoubleType,
                       VARCHAR=pyspark.sql.types.StringType,
                       NVARCHAR=pyspark.sql.types.StringType,
                       DATE=pyspark.sql.types.StringType)


class JdbcReader(Link):
    """Link to read a table from a database using JDBC"""

    def __init__(self, **kwargs):
        """Initialize link instance

        :param str name: name of link
        :param str store_key: key to spark dataframe to store in data store
        :param str schema: schema?
        :param str table: table?
        :param list queries: list of queries to perform
        :param list sel_cols: list of columns to select
        :param list col_ranges: list of column ranges to apply
        :param bool group_sel: group selection
        :param bool max_recs: maximum number of retries
        """

        # initialize Link
        Link.__init__(self, kwargs.pop('name', 'JdbcReader'))

        # process keyword arguments
        self._process_kwargs(kwargs, store_key='')
        self._process_kwargs(kwargs, schema='')
        self._process_kwargs(kwargs, table='')
        self._process_kwargs(kwargs, queries=[])
        self._process_kwargs(kwargs, sel_cols=[])
        self._process_kwargs(kwargs, group_sel=False)
        self._process_kwargs(kwargs, col_ranges=[])
        self._process_kwargs(kwargs, max_recs=-1)
        self.check_extra_kwargs(kwargs)

    def initialize(self):
        """Initialize JdbcReader"""

        # get path and section of JDBC configuration settings
        settings = process_manager.service(ConfigObject)
        cfg_path = settings.get('jdbc_path')
        cfg_sec = settings.get('jdbc_section')
        if not cfg_path or not isinstance(cfg_path, str):
            self.logger.fatal('No JDBC configuration file path specified in settings ("jdbc_path").')
            raise RuntimeError('No JDBC configuration file path set.')
        if not cfg_sec or not isinstance(cfg_path, str):
            self.logger.fatal('No section in JDBC configuration file specified in settings ("jdbc_section").')
            raise RuntimeError('No JDBC configuration section set.')

        # create JDBC connection instance
        jdbc_conn = process_manager.service(JdbcConnection)
        jdbc_conn.config_path = cfg_path
        jdbc_conn.config_section = cfg_sec

        # get JDBC meta data
        jdbc_meta = jdbc_conn.connection.jconn.getMetaData()

        # check arguments
        self.check_arg_types(**dict((k, str) for k in ['schema', 'table', 'store_key']))
        self.check_arg_types(recurse=True, queries=str, sel_cols=str)
        self.check_arg_vals('table')

        # create queue of column ranges
        self._col_ranges_queue = None if not self.col_ranges \
            else [self.col_ranges] if isinstance(self.col_ranges, dict) else list(reversed(self.col_ranges))

        # construct table specification
        self._constr_table_spec(jdbc_meta)

        # construct column schema
        self._constr_col_schema(jdbc_meta)

        # construct where clauses
        self._constr_where(self.col_ranges)

        # construct queries
        self._qry_queue = [] if not self.queries else [self.queries] if isinstance(self.queries, str) \
            else list(self.queries)
        if not self._qry_queue:
            for args in reversed(self._where_args):
                self._qry_queue.append('SELECT {0:s} FROM {1:s}'.format(self._select_cols, self._table_spec))
                if args:
                    self._qry_queue[-1] += ' WHERE {}'.format(args)
                if self.group_sel:
                    self._qry_queue[-1] += ' GROUP BY {}'.format(self._select_cols)

        # set default for current iteration
        self._current_it = -1

        return StatusCode.Success

    def execute(self):
        """Execute JdbcReader"""

        # remove old dataframe from data store
        ds = process_manager.service(DataStore)
        if self.store_key in ds:
            ds[self.store_key].unpersist()
            del ds[self.store_key]

        if self._current_it < 0:
            self._current_it = 0

            # get next query
            query = self._qry_queue.pop()

            # execute select query
            self._curs = process_manager.service(JdbcConnection).createJdbcCursor()
            self.logger.debug('executing JDBC query "{}"'.format(query))
            self._curs.execute(query)

            # check column names
            self.logger.debug('Checking column names for table {0:s}'.format(self._table_spec))
            cols = [c[0] for c in self._curs.description]
            if cols != list(self._spark_schema.names):
                self.logger.fatal('Fetched column names from query are inconsistent with schema names:\n'
                                  '    fetched: [{columns}]\n',
                                  '    schema:  [{schema}]\n',
                                  columns=', '.join(cols), schema=', '.join(self._spark_schema.names))
                raise RuntimeError('Inconsistent column names from JDBC query.')
        else:
            self._current_it += 1

        # fetch data from table
        self.logger.debug('iteration {0:d}: fetching data from table {1:s}'.format(self._current_it, self._table_spec))
        select_data = self._curs.fetchmany(self.max_recs)if self.max_recs > 0 else self._curs.fetchall()

        # determine if there will be a next read iteration
        settings = process_manager.service(ConfigObject)
        read_more = 0 < self.max_recs == len(select_data)
        settings['chainRepeatRequestBy_{}'.format(self.name)] = read_more or len(self._qry_queue) > 0

        # create a dataframe if data were fetched
        if select_data:
            self.logger.debug('iteration {0:d}: read {1:d} records'.format(self._current_it, len(select_data)))
            self.logger.debug('iteration {0:d}: creating dataframe from table {1:s} with columns [{2:s}]'
                              .format(self._current_it, self._table_spec,
                                      ', '.join(c for c in self._spark_schema.names)))
            df = data_conversion.create_spark_df(process_manager.service(SparkManager).get_session(), select_data,
                                                 schema=self._spark_schema)
        else:
            df = None
        del select_data

        # store dataframe in data store
        ds[self.store_key] = df

        # reset iterator and finish execution
        if not read_more:
            self._current_it = -1
        return StatusCode.Success

    def _constr_table_spec(self, jdbc_meta):
        """Construct table specification string"""

        # get table info from JDBC meta data
        tables = jdbc_meta.getTables(None, self.schema, self.table, None)
        n_tables = 0
        while True:
            valid_res = next(tables)
            if not valid_res:
                break
            n_tables += 1

        # check number of tables found
        if n_tables != 1:
            self.logger.fatal('{count:d} tables found with schema "{schema}" and name "{name}".',
                              count=n_tables, schema=self.schema, name=self.table)
            raise RuntimeError('Unexpected number of tables found in JDBC meta data.')

        # construct table specification
        self._table_spec = '.'.join((_quote_name(n) for n in (self.schema, self.table) if n))
        if not self.store_key:
            self.store_key = _unquote_name(self._table_spec)

    def _constr_where(self, col_ranges):
        """Construct where clauses of select queries"""

        # no where clause if no column ranges are specified
        if not col_ranges:
            self._where_args = ['']
            return

        # parse specified column ranges into list column-range dictionaries
        if isinstance(col_ranges, dict):
            col_ranges = [col_ranges]

        # construct where clauses
        self._where_args = []
        for ranges in col_ranges:
            self._where_args.append(' AND '.join('{0:s} {1:s} {2:s}'.format(
                    _quote_name(c), '>=' if i == 0 else '<', str(b))
                    for c, r in ranges.items() for i, b in enumerate(r) if b is not None))

    def _constr_col_schema(self, jdbc_meta):
        """Construct schema of table columns"""

        # get column info from JDBC meta data
        columns = jdbc_meta.getColumns(None, self.schema, self.table, None)
        col_info = odict()
        while True:
            valid_res = next(columns)
            if not valid_res:
                break
            col_info[columns.getString('COLUMN_NAME')] = columns.getString('TYPE_NAME')
        self.logger.debug('Columns in table with schema "{schema}" and name "{name}":\n'
                          '    [{columns}]', schema=self.schema, name=self.table,
                          columns=', '.join('{0:s} {1:s}'.format(*c) for c in col_info.items()))

        # check number of columns found
        if not col_info:
            self.logger.fatal('No columns found for table with schema "{schema}" and name "{name}".',
                              schema=self.schema, name=self.table)
            raise RuntimeError('No columns found for specified table in JDBC meta data.')

        # give warning if not all specified columns are found
        no_sel_cols = set(self.sel_cols) - set(col_info)
        if no_sel_cols:
            self.logger.warning('Columns [{columns}] not found in JDBC meta data.', columns=', '.join(no_sel_cols))

        # determine columns to select
        if self.sel_cols:
            col_info_sel = odict()
            for col in self.sel_cols:
                if col not in col_info:
                    continue
                col_info_sel[col] = col_info[col]
        else:
            col_info_sel = col_info
        if not col_info_sel:
            self.logger.fatal('No columns to select (table with schema "{schema}" and name "{name}").',
                              schema=self.schema, name=self.table)
            raise RuntimeError('No columns to select.')
        for cname, ctype in col_info_sel.items():
            if ctype not in spark_col_types:
                self.logger.fatal('Unknown column type: {type} (table with schema "{schema}" and name "{name}")',
                                  type=ctype, schema=self.schema, name=self.table)
                raise RuntimeError('Unknown column type(s) in select columns.')

        # construct SQL schema for Spark dataframe
        struct_t = pyspark.sql.types.StructType
        struct_f = pyspark.sql.types.StructField
        self._spark_schema = struct_t([struct_f(name=c, dataType=spark_col_types[t](),
                                                nullable=True, metadata=None) for c, t in col_info_sel.items()])

        # construct list of selection columns
        self._select_cols = ', '.join(_quote_name(c) for c in self._spark_schema.names)
