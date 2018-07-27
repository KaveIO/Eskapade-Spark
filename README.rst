==============
Eskapade-Spark
==============

Version: 0.8

Released: Aug 2018

Eskapade is a light-weight, python-based data analysis framework, meant for modularizing all sorts of data analysis problems.
Eskapade-Spark is the Spark-based extension of Eskapade.


Documentation
=============

For documentation on the Spark extension of Eskapade, please see the original Eskapade documentation `here <http://eskapade.readthedocs.io>`_.
In particular, check out the Spark section of the Tutorials.



Release notes
=============

Version 0.8
-----------

Version 0.8 of Eskapade-Spark (August 2018) is a split off of the ``spark-analysis`` module of Eskapade 
into a separate package. 

This way, Eskapade v0.8 no longer depends on Spark. This new package does require Spark to install, clearly.



Installation
============

Requirements
------------

Eskapade-ROOT requires Python 3, Eskapade v0.8 and Spark v2.1.2 or greater.

Eskapade on your own machine
----------------------------

To install the package from pypi, do:

.. code-block:: bash
  $ pip install Eskapade-Spark

Alternatively, you can check out the repository from github and install it yourself:

.. code-block:: bash
  $ git clone 

To (re)install the python code from your local directory, type from the top directory:

.. code-block:: bash
  $ pip install -e .

Python
------

After installation, you can now do in Python:

.. code-block:: python
  $ import eskapadespark


Contact and support
===================

Contact us at: kave [at] kpmg [dot] com

Please note that the KPMG Eskapade group provides support only on a best-effort basis.

