#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()


try:
    import rstcheck
    found_errors = False

    readme_errors = list(rstcheck.check(readme))
    if len(readme_errors) > 0:
        sys.stderr.write('\nErrors in README.rst [(line #, error)]\n' +
                         str(readme_errors) + '\n')
        found_errors = True

    history_errors = list(rstcheck.check(history))
    if len(history_errors) > 0:
        sys.stderr.write('\nErrors in HISTORY.rst [(line #, error)]\n' +
                         str(history_errors) + '\n')

        found_errors = True

    if 'sdist' in sys.argv or 'bdist_wheel' in sys.argv:
        if found_errors is True:
            sys.stderr.write('\n\nEXITING due to errors encountered in'
                             ' History.rst or Readme.rst.\n\nSee errors above\n\n')
            sys.exit(1)

except Exception as e:
    sys.stderr.write('WARNING: rstcheck library found, '
                     'unable to validate README.rst or HISTORY.rst\n')


requirements = [
    "argparse",
    "configparser",
    "Pillow"
]

test_requirements = [
    "argparse",
    "configparser",
    "Pillow",
    "mock"
]

setup(
    name='chmutil',
    version='0.8.3',
    description="Utility package to run CHM jobs on clusters",
    long_description=readme + '\n\n' + history,
    author="Christopher Churas",
    author_email='churas@ncmir.ucsd.edu',
    url='https://github.com/CRBS/chmutil',
    packages=[
        'chmutil',
    ],
    package_dir={'chmutil':
                 'chmutil'},
    include_package_data=True,
    install_requires=requirements,
    zip_safe=False,
    keywords='chmutil',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6'
    ],
    scripts=['chmutil/createchmjob.py',
             'chmutil/checkchmjob.py',
             'chmutil/mergetiles.py',
             'chmutil/createchmimage.py',
             'chmutil/createprobmapoverlay.py',
             'chmutil/createtrainingmrcstack.py',
             'chmutil/createchmtrainjob.py',
             'chmutil/mergetilerunner.py',
             'chmutil/chmrunner.py'],
    test_suite='tests',
    tests_require=test_requirements
)
