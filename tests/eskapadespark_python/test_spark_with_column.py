import unittest
import pandas as pd
import pyspark.sql.functions as f

from eskapade import process_manager, DataStore, ConfigObject
from eskapadespark import SparkManager, SparkWithColumn
from escore.core import execution


class SparkWithColumnTest(unittest.TestCase):

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
        with_col_link = SparkWithColumn(read_key='test_input',
                                        store_key='test_output',
                                        new_col_name='new_col',
                                        new_col=f.lit(0))

        with_col_link.initialize()
        with_col_link.execute()

        ds = process_manager.service(DataStore)
        self.assertIn('test_output', ds)

    def test_new_col_made(self):
        with_col_link = SparkWithColumn(read_key='test_input',
                                        store_key='test_output',
                                        new_col_name='new_col',
                                        new_col=f.lit(0))

        with_col_link.initialize()
        with_col_link.execute()

        ds = process_manager.service(DataStore)
        self.assertIn('new_col', ds['test_output'].columns)
        self.assertEqual(ds['test_input'].columns + ['new_col'],
                         ds['test_output'].columns)

    def test_new_col_right(self):
        with_col_link = SparkWithColumn(read_key='test_input',
                                        store_key='test_output',
                                        new_col_name='new_col',
                                        new_col=f.col('a')+1)

        with_col_link.initialize()
        with_col_link.execute()

        ds = process_manager.service(DataStore)
        pdf = ds['test_output'].toPandas()

        self.assertEqual(list(pdf['new_col']), list(pdf['a']+1))
