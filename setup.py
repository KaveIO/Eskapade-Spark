"""Project: Eskapade - A python-based package for data analysis.

Created: 2017/08/08

Description:
    setup script.

Authors:
    KPMG Big Data team, Amstelveen, The Netherlands

Redistribution and use in source and binary forms, with or without
modification, are permitted according to the terms listed in the file
LICENSE.
"""

import logging

from setuptools import find_packages
from setuptools import setup

NAME = 'Eskapade-Spark'

MAJOR = 0
REVISION = 8
PATCH = 3
DEV = False

# Note: also update version at: README.rst and docs/source/conf.py 

VERSION = '{major}.{revision}.{patch}'.format(major=MAJOR, revision=REVISION, patch=PATCH)
FULL_VERSION = VERSION
if DEV:
    FULL_VERSION += '.dev'

TEST_REQUIREMENTS = ['pytest>=3.5.0',
                     'pytest-pylint>=0.9.0',
                     ]

REQUIREMENTS = [
    'eskapade>=0.8.2',
    'pyspark>=2.1.2',
]

REQUIREMENTS += TEST_REQUIREMENTS

CMD_CLASS = dict()
COMMAND_OPTIONS = dict()

EXCLUDE_PACKAGES = []
EXTERNAL_MODULES = []

logging.basicConfig()
logger = logging.getLogger(__file__)


def write_version_py(filename: str = 'python/eskapadespark/version.py') -> None:
    """Write package version to version.py.

    This will ensure that the version in version.py is in sync with us.

    :param filename: The version.py to write too.
    :type filename: str
    :return:
    :rtype: None
    """
    # Do not modify the indentation of version_str!
    version_str = """\"\"\"THIS FILE IS AUTO-GENERATED BY ESKAPADE SETUP.PY.\"\"\"

name = '{name!s}'
version = '{version!s}'
full_version = '{full_version!s}'
release = {is_release!s}
"""

    version_file = open(filename, 'w')
    try:
        version_file.write(version_str.format(name=NAME.lower(),
                                              version=VERSION,
                                              full_version=FULL_VERSION,
                                              is_release=not DEV))
    finally:
        version_file.close()


def setup_package() -> None:
    """The main setup method.

    It is responsible for setting up and installing the package.

    :return:
    :rtype: None
    """
    write_version_py()

    setup(name=NAME,
          version=FULL_VERSION,
          url='http://eskapade.kave.io',
          license='',
          author='KPMG N.V. The Netherlands',
          author_email='kave@kpmg.com',
          description='Spark for Eskapade',
          python_requires='>=3.5',
          package_dir={'': 'python'},
          packages=find_packages(where='python', exclude=EXCLUDE_PACKAGES),
          # Setuptools requires that package data are located inside the package.
          # This is a feature and not a bug, see
          # http://setuptools.readthedocs.io/en/latest/setuptools.html#non-package-data-files
          package_data={
              'eskapadespark': ['config/spark/*.cfg', 'templates/*', 'data/*', 'tutorials/*.sh']
          },
          install_requires=REQUIREMENTS,
          tests_require=TEST_REQUIREMENTS,
          ext_modules=EXTERNAL_MODULES,
          cmdclass=CMD_CLASS,
          command_options=COMMAND_OPTIONS,
          )


if __name__ == '__main__':
    setup_package()
