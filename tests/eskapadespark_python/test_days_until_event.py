import unittest
import pandas as pd
import numpy as np

from eskapade import process_manager, DataStore, ConfigObject
from eskapadespark import SparkManager, FindDaysUntilEvent
from eskapade.core import execution


class FindDaysUntilEventTest(unittest.TestCase):

    def setUp(self):
        test_df_single_part = pd.DataFrame({'dt': ['2017-01-01',
                                                   '2017-01-02',
                                                   '2017-01-03',
                                                   '2017-01-04'],
                                            'a': [0, 0, 1, 0]})

        test_df_two_parts = pd.DataFrame({'dt': ['2017-01-01',
                                                 '2017-01-02',
                                                 '2017-01-03',
                                                 '2017-01-04',

                                                 '2017-01-01',
                                                 '2017-01-02',
                                                 '2017-01-03',
                                                 '2017-01-04'],
                                          'b': [1, 1, 1, 1,
                                                2, 2, 2, 2],
                                          'a': [0, 0, 1, 0,
                                                0, 0, 0, 0]})

        settings = process_manager.service(ConfigObject)
        spark = process_manager.service(SparkManager).create_session(eskapade_settings=settings)
        ds = process_manager.service(DataStore)
        ds['test_input_single_part'] = spark.createDataFrame(test_df_single_part)
        ds['test_input_two_parts'] = spark.createDataFrame(test_df_two_parts)

    def tearDown(self):
        execution.reset_eskapade()

    def test_store_key_used(self):
        link = FindDaysUntilEvent(read_key='test_input_single_part',
                                  store_key='test_output',
                                  datetime_col='dt',
                                  event_col='a')

        link.initialize()
        link.execute()

        ds = process_manager.service(DataStore)
        self.assertIn('test_output', ds)

    def test_default_new_col_name(self):
        link = FindDaysUntilEvent(read_key='test_input_single_part',
                                  store_key='test_output',
                                  datetime_col='dt',
                                  event_col='a')

        link.initialize()
        link.execute()

        ds = process_manager.service(DataStore)
        self.assertIn('days_until_event', ds['test_output'].columns)

    def test_custom_new_col_name(self):
        link = FindDaysUntilEvent(read_key='test_input_single_part',
                                  store_key='test_output',
                                  datetime_col='dt',
                                  event_col='a',
                                  countdown_col_name='test_col')

        link.initialize()
        link.execute()

        ds = process_manager.service(DataStore)
        self.assertIn('test_col', ds['test_output'].columns)

    def test_countdown(self):
        link = FindDaysUntilEvent(read_key='test_input_single_part',
                                  store_key='test_output',
                                  datetime_col='dt',
                                  event_col='a')

        link.initialize()
        link.execute()

        ds = process_manager.service(DataStore)
        pdf = ds['test_output'].toPandas().set_index('dt')

        self.assertEqual(pdf.loc['2017-01-01', 'days_until_event'], 2)
        self.assertEqual(pdf.loc['2017-01-02', 'days_until_event'], 1)
        self.assertEqual(pdf.loc['2017-01-03', 'days_until_event'], 0)

    def test_null_after_last_event(self):
        link = FindDaysUntilEvent(read_key='test_input_single_part',
                                  store_key='test_output',
                                  datetime_col='dt',
                                  event_col='a')

        link.initialize()
        link.execute()

        ds = process_manager.service(DataStore)
        pdf = ds['test_output'].toPandas().set_index('dt')

        self.assertTrue(np.isnan(pdf.loc['2017-01-04', 'days_until_event']))

    def test_partitionby_cols(self):
        link = FindDaysUntilEvent(read_key='test_input_two_parts',
                                  store_key='test_output',
                                  datetime_col='dt',
                                  event_col='a',
                                  partitionby_cols=['b'])

        link.initialize()
        link.execute()

        ds = process_manager.service(DataStore)
        pdf = ds['test_output'].toPandas().set_index('dt')

        self.assertIn('b', ds['test_output'])
        self.assertIn(1, list(pdf['b']))
        self.assertIn(2, list(pdf['b']))
        self.assertFalse(any(list(pdf['b'] > 2)))
        self.assertEqual(8, len(pdf))

    def test_all_null_if_no_events(self):
        link = FindDaysUntilEvent(read_key='test_input_two_parts',
                                  store_key='test_output',
                                  datetime_col='dt',
                                  event_col='a',
                                  partitionby_cols=['b'])

        link.initialize()
        link.execute()

        ds = process_manager.service(DataStore)
        pdf = ds['test_output'].toPandas().set_index('dt')

        self.assertTrue(all(list(np.isnan(pdf[pdf['b'] == 2]['days_until_event']))))

