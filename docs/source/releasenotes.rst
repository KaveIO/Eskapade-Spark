=============
Release notes
=============

Version 0.9
-----------

Eskapade-Spark v0.9 (December 2018) contains only one update compared with v0.8:

* All code has been updated to Eskapade v0.9, where the core functionality has been split off into the Eskapade-Core package. As such the code is backwards-incompatible with v0.8.

Version 0.8
-----------

Version 0.8 of Eskapade-Spark (August 2018) is a split off of the ``spark-analysis`` module of Eskapade v0.7
into a separate package. This way, Eskapade v0.8 no longer depends on Spark. This new package Eskapade-Spark does require Spark to install, clearly.

In addition, we have included new analysis code for processing ("flattening") time-series data, so it can be easily used as input for machine learning models.
See tutorial example esk611 for details.

