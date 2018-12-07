import unittest
import pandas as pd
import pyspark.sql.functions as f

from eskapade import process_manager, DataStore, ConfigObject
from eskapadespark import SparkManager, DailySummary
from escore.core import execution


class DailySummaryTest(unittest.TestCase):

    def setUp(self):

        test_df = pd.DataFrame({'dt': ['2017-01-01 12:00:00',
                                       '2017-01-01 13:00:00',
                                       '2017-01-03 12:45:23'],
                                'a': [1, 2, 5],
                                'b': [1, 2, 1]})
        settings = process_manager.service(ConfigObject)
        spark = process_manager.service(SparkManager).create_session(eskapade_settings=settings)
        ds = process_manager.service(DataStore)
        ds['test_input'] = spark.createDataFrame(test_df)

    def tearDown(self):
        execution.reset_eskapade()

    def test_store_key_used(self):
        summary_link = DailySummary(read_key='test_input',
                                    store_key='test_output',
                                    feature_cols=['a'],
                                    datetime_col='dt')

        summary_link.initialize()
        summary_link.execute()

        ds = process_manager.service(DataStore)
        self.assertIn('test_output', ds)

    def test_new_date_col_used(self):
        summary_link = DailySummary(read_key='test_input',
                                    store_key='test_output',
                                    feature_cols=['a'],
                                    datetime_col='dt',
                                    new_date_col='date_test')

        summary_link.initialize()
        summary_link.execute()

        ds = process_manager.service(DataStore)
        self.assertIn('date_test', ds['test_output'].columns)

    def test_default_summary_columns(self):
        summary_link = DailySummary(read_key='test_input',
                                    store_key='test_output',
                                    feature_cols=['a'],
                                    datetime_col='dt')

        summary_link.initialize()
        summary_link.execute()

        ds = process_manager.service(DataStore)
        self.assertIn('a_min_0d', ds['test_output'].columns)
        self.assertIn('a_mean_0d', ds['test_output'].columns)
        self.assertIn('a_max_0d', ds['test_output'].columns)
        self.assertIn('a_stddev_0d', ds['test_output'].columns)
        self.assertIn('a_count_0d', ds['test_output'].columns)
        self.assertIn('a_sum_0d', ds['test_output'].columns)

    def test_specific_summary_column_dict(self):
        summary_link = DailySummary(read_key='test_input',
                                    store_key='test_output',
                                    feature_cols={'a': [f.sum]},
                                    datetime_col='dt')

        summary_link.initialize()
        summary_link.execute()

        ds = process_manager.service(DataStore)
        self.assertNotIn('a_min_0d', ds['test_output'].columns)
        self.assertNotIn('a_mean_0d', ds['test_output'].columns)
        self.assertNotIn('a_max_0d', ds['test_output'].columns)
        self.assertNotIn('a_stddev_0d', ds['test_output'].columns)
        self.assertNotIn('a_count_0d', ds['test_output'].columns)
        self.assertIn('a_sum_0d', ds['test_output'].columns)

    def test_function_execution(self):
        summary_link = DailySummary(read_key='test_input',
                                    store_key='test_output',
                                    feature_cols={'a': [f.sum, f.count]},
                                    datetime_col='dt')

        summary_link.initialize()
        summary_link.execute()

        ds = process_manager.service(DataStore)
        pdf = ds['test_output'].toPandas()

        self.assertEqual(list(pdf['a_sum_0d']), [3, 5])
        self.assertEqual(list(pdf['a_count_0d']), [2, 1])

    def test_partitionby_cols_kept(self):
        summary_link = DailySummary(read_key='test_input',
                                    store_key='test_output',
                                    feature_cols=['a'],
                                    datetime_col='dt',
                                    partitionby_cols=['b'])

        summary_link.initialize()
        summary_link.execute()

        ds = process_manager.service(DataStore)
        self.assertIn('b', ds['test_output'].columns)

    def test_partitionby_partitions(self):
        summary_link = DailySummary(read_key='test_input',
                                    store_key='test_output',
                                    feature_cols=['a'],
                                    datetime_col='dt',
                                    partitionby_cols=['b'])

        summary_link.initialize()
        summary_link.execute()

        ds = process_manager.service(DataStore)
        pdf = ds['test_output'].toPandas()

        self.assertIn(1, pdf['b'])
        self.assertIn(2, pdf['b'])
        self.assertEqual(len(pdf[pdf['b'] == 1]), 2)
        self.assertEqual(len(pdf[pdf['b'] == 2]), 1)
