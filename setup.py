#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    "argparse",
    "configparser",
    "Pillow"
]

test_requirements = [
    "argparse",
    "configparser",
    "Pillow"
]

setup(
    name='chmutil',
    version='0.1.0',
    description="Utility package to run CHM jobs on clusters",
    long_description=readme + '\n\n' + history,
    author="Christopher Churas",
    author_email='churas@ncmir.ucsd.edu',
    url='https://github.com/coleslaw481/chmutil',
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
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    scripts=['chmutil/createchmjob.py', 'chmutil/chmrunner.py'],
    test_suite='tests',
    tests_require=test_requirements
)
