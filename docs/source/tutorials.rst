=========
Tutorials
=========

This section contains materials on how to use Eskapade-Spark.
All command examples can be run from any directory with write access.
For more in depth explanations on the functionality of the code-base,
try the `API docs <code.html>`_.



All Spark Examples in Eskapade
------------------------------

All Eskapade-Spark example macros can be found in the tutorials directory.
For ease of use, let's make a shortcut to the directory containing the tutorials:

.. code-block:: bash

  $ export TUTDIR=`pip show Eskapade-Spark | grep Location | awk '{ print $2"/eskapadespark/tutorials" }'`
  $ ls -l $TUTDIR/

The numbering of the example macros follows the package structure:

* ``esk600+``: macros for processing Spark datasets and performing analysis with Spark.

These macros are briefly described below.
You are encouraged to run all examples to see what they can do for you!



Example esk601: setting the spark configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Tutorial macro for configuring Spark in multiple ways.

.. code-block:: bash

  $ eskapade_run $TUTDIR/esk601_spark_configuration.py


Example esk602: reading csv to a spark dataframe
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Tutorial macro for reading CSV files into a Spark data frame.

.. code-block:: bash

  $ eskapade_run $TUTDIR/esk602_read_csv_to_spark_df.py


Example esk603: writing spark data to csv
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Tutorial macro for writing Spark data to a CSV file.

.. code-block:: bash

  $ eskapade_run $TUTDIR/esk603_write_spark_data_to_csv.py


Example esk604: executing queries
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Tutorial macro for applying a SQL-query to one more objects in the
DataStore. Such SQL-queries can for instance be used to filter data.

.. code-block:: bash

  $ eskapade_run $TUTDIR/esk604_spark_execute_query.py


Example esk605: creating Spark data frames from various input data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Tutorial macro for creating Spark data frames from different types of input data.

.. code-block:: bash

  $ eskapade_run $TUTDIR/esk605_create_spark_df.py


Example esk606: converting Spark data frames into different data types
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Tutorial macro for converting Spark data frames into a different
data type and apply transformation functions on the resulting data.

.. code-block:: bash

  $ eskapade_run $TUTDIR/esk606_convert_spark_df.py


Example esk607: adding a new column to a Spark dataframe
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Tutorial macro for adding a new column to a Spark dataframe by applying
a Spark built-in or user-defined function to a selection of columns
in a Spark dataframe.

.. code-block:: bash

  $ eskapade_run $TUTDIR/esk607_spark_with_column.py


Example esk608: making histograms of a Spark dataframe
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Tutorial macro for making histograms of a Spark dataframe using the Histogrammar package.

.. code-block:: bash

  $ eskapade_run $TUTDIR/esk608_spark_histogrammar.py


Example esk609: applying map functions on groups of rows
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Tutorial macro for applying map functions on groups of rows in Spark data frames.

.. code-block:: bash

  $ eskapade_run $TUTDIR/esk609_map_df_groups.py


Example esk610: running Spark Streaming word count example
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Tutorial macro running Spark Streaming word count example in Eskapade, derived from:

https://spark.apache.org/docs/latest/streaming-programming-guide.html

Counts words in UTF8 encoded, '\n' delimited text received from a stream every second.
The stream can be from either files or network.

.. code-block:: bash

  $ eskapade_run $TUTDIR/esk610_spark_streaming_wordcount.py



Example esk611: techniques for flattening a time-series in Spark
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This macro demonstrates techniques for flattening a time-series in Spark.

.. code-block:: bash

  $ eskapade_run $TUTDIR/esk611_flatten_time_series.py



.. include:: tutorial_spark.rst
