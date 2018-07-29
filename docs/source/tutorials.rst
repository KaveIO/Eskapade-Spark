=========
Tutorials
=========

This section contains materials on how to use Eskapade-Spark.
All command examples are run from the root of the repository if not otherwise stated.
For more in depth explanations on the functionality of the code-base,
try the `API docs <eskapade_index.html>`_.



All Spark Examples in Eskapade
------------------------------

All Eskapade-Spark example macros can be found in the ``python/eskapadespark/tutorials`` directory.
The numbering of the example macros follows the package structure:

* ``esk600+``: macros for processing Spark datasets and performing analysis with Spark.

These macros are briefly described below.
You are encouraged to run all examples to see what they can do for you!



Example esk601: setting the spark configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Tutorial macro for configuring Spark in multiple ways.

Example esk602: reading csv to a spark dataframe
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Tutorial macro for reading CSV files into a Spark data frame.

Example esk603: writing spark data to csv
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Tutorial macro for writing Spark data to a CSV file.

Example esk604: executing queries
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Tutorial macro for applying a SQL-query to one more objects in the
DataStore. Such SQL-queries can for instance be used to filter data.

Example esk605: creating Spark data frames from various input data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Tutorial macro for creating Spark data frames from different types of input data.

Example esk606: converting Spark data frames into different data types
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Tutorial macro for converting Spark data frames into a different
data type and apply transformation functions on the resulting data.

Example esk607: adding a new column to a Spark dataframe
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Tutorial macro for adding a new column to a Spark dataframe by applying
a Spark built-in or user-defined function to a selection of columns
in a Spark dataframe.

Example esk608: making histograms of a Spark dataframe
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Tutorial macro for making histograms of a Spark dataframe using the Histogrammar package.

Example esk609: applying map functions on groups of rows
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Tutorial macro for applying map functions on groups of rows in Spark data frames.

Example esk610: running Spark Streaming word count example
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Tutorial macro running Spark Streaming word count example in Eskapade, derived from:

https://spark.apache.org/docs/latest/streaming-programming-guide.html

Counts words in UTF8 encoded, '\n' delimited text received from a stream every second.
The stream can be from either files or network.


Example esk611: techniques for flattening a time-series in Spark
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This macro demonstrates techniques for flattening a time-series in Spark.


.. include:: tutorial_spark.rst
