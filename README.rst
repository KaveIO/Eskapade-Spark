==============
Eskapade-Spark
==============

* Version: 0.8
* Released: Aug 2018

Eskapade is a light-weight, python-based data analysis framework, meant for modularizing all sorts of data analysis problems
into reusable analysis components. For documentation on Eskapade, please go to this `link <http://eskapade.readthedocs.io>`_.

Eskapade-Spark is the Spark-based extension of Eskapade.
For documentation on Eskapade-Spark, please go `here <http://eskapade-spark.readthedocs.io>`_.


Release notes
=============

Version 0.8
-----------

Version 0.8 of Eskapade-Spark (August 2018) is a split off of the ``spark-analysis`` module of Eskapade v0.7
into a separate package. This way, Eskapade v0.8 no longer depends on Spark. This new package Eskapade-Spark does require Spark to install, clearly.

In addition, we have included new analysis code for processing ("flattening") time-series data, so it can be easily used as input for machine learning models.
See tutorial example esk611 for details.


Installation
============

requirements
------------

Eskapade-Spark requires ``Python 3.5+``, ``Eskapade v0.8+`` and ``Spark v2.1.2``.
These are pre-installed in the Eskapade `docker <http://eskapade.readthedocs.io/en/latest/installation.html#eskapade-with-docker>`_.


pypi
----

To install the package from pypi, do:

.. code-block:: bash

  $ pip install Eskapade-Spark

github
------

Alternatively, you can check out the repository from github and install it yourself:

.. code-block:: bash

  $ git clone git@github.com:KaveIO/Eskapade-Spark.git eskapade-spark

To (re)install the python code from your local directory, type from the top directory:

.. code-block:: bash

  $ pip install -e eskapade-spark

python
------

After installation, you can now do in Python:

.. code-block:: python

  import eskapadespark

**Congratulations, you are now ready to use Eskapade-Spark!**


Quick run
=========

To see the available Eskapade-Spark examples, do:

.. code-block:: bash

  $ export TUTDIR=`pip show Eskapade-Spark | grep Location | awk '{ print $2"/eskapadespark/tutorials" }'`
  $ ls -l $TUTDIR/

E.g. you can now run:

.. code-block:: bash

  $ eskapade_run $TUTDIR/esk601_spark_configuration.py


For all available examples, please see the `tutorials <http://eskapade-spark.readthedocs.io/en/latest/tutorials.html>`_.


Contact and support
===================

Contact us at: kave [at] kpmg [dot] com

Please note that the KPMG Eskapade group provides support only on a best-effort basis.
