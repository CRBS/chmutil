#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_batchedjoblistgenerator.py
----------------------------------

Tests for `BatchedJobsListGenerator` class
"""

import os
import tempfile
import shutil
import unittest
import configparser
from mock import Mock

from chmutil.cluster import CHMJobCreator
from chmutil.cluster import BatchedJobsListGenerator
from chmutil.cluster import InvalidConfigFileError


class TestBatchedJobsListGenerator(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_write_batched_job_config_no_preexisting_config(self):
        temp_dir = tempfile.mkdtemp()
        try:
            checker = Mock()
            gen = BatchedJobsListGenerator(checker, 1)
            cfile = os.path.join(temp_dir, 'foo.config')
            bconfig = configparser.ConfigParser()
            bconfig.set('', 'somekey', 'val')

            gen._write_batched_job_config(bconfig, cfile)
            self.assertTrue(os.path.isfile(cfile))
        finally:
            shutil.rmtree(temp_dir)

    def test_write_batched_job_config_with_preexisting_config(self):
        temp_dir = tempfile.mkdtemp()
        try:
            checker = Mock()
            gen = BatchedJobsListGenerator(checker, 1)
            cfile = os.path.join(temp_dir, 'foo.config')
            bconfig = configparser.ConfigParser()
            bconfig.set('', 'somekey', 'val')

            gen._write_batched_job_config(bconfig, cfile)
            self.assertTrue(os.path.isfile(cfile))
            bconfig.set('', 'somekey', 'anotherval')
            gen._write_batched_job_config(bconfig, cfile)
            ocfile = cfile + BatchedJobsListGenerator.OLD_SUFFIX
            self.assertTrue(os.path.isfile(ocfile))
            bconfig.read(ocfile)
            self.assertEqual(bconfig.get('DEFAULT', 'somekey'), 'val')

            bconfig.read(cfile)
            self.assertEqual(bconfig.get('DEFAULT', 'somekey'), 'anotherval')
        finally:
            shutil.rmtree(temp_dir)

    def test_generate_batched_jobs_list_none_for_configfile(self):
            checker = Mock()
            gen = BatchedJobsListGenerator(checker, 1)
            try:
                gen.generate_batched_jobs_list(None)
                self.fail('Expected InvalidConfigFileError')
            except InvalidConfigFileError as e:
                self.assertEqual(str(e), 'configfile passed in cannot be null')

    def test_generate_batched_jobs_list_no_jobs(self):
        temp_dir = tempfile.mkdtemp()
        try:
            checker = Mock()
            checker.get_incomplete_jobs_list = Mock(return_value=[])
            gen = BatchedJobsListGenerator(checker, 1)
            cfile = os.path.join(temp_dir, 'foo.config')
            self.assertEqual(gen.generate_batched_jobs_list(cfile),
                             0)

        finally:
            shutil.rmtree(temp_dir)

    def test_generate_batched_jobs_list_one_job_one_job_per_node(self):
        temp_dir = tempfile.mkdtemp()
        try:
            checker = Mock()
            checker.get_incomplete_jobs_list = Mock(return_value=['1'])
            gen = BatchedJobsListGenerator(checker, 1)
            cfile = os.path.join(temp_dir, 'foo.config')
            self.assertEqual(gen.generate_batched_jobs_list(cfile), 1)
            self.assertTrue(os.path.isfile(cfile))
            bconfig = configparser.ConfigParser()
            bconfig.read(cfile)
            self.assertEqual(bconfig.get('1',
                                         CHMJobCreator.BCONFIG_TASK_ID), '1')
            self.assertEqual(bconfig.sections(), ['1'])
        finally:
            shutil.rmtree(temp_dir)

    def test_generate_batched_jobs_list_one_job_five_job_per_node(self):
        temp_dir = tempfile.mkdtemp()
        try:
            checker = Mock()
            checker.get_incomplete_jobs_list = Mock(return_value=['1'])
            gen = BatchedJobsListGenerator(checker, 5)
            cfile = os.path.join(temp_dir, 'foo.config')
            self.assertEqual(gen.generate_batched_jobs_list(cfile), 1)
            self.assertTrue(os.path.isfile(cfile))
            bconfig = configparser.ConfigParser()
            bconfig.read(cfile)
            self.assertEqual(bconfig.get('1',
                                         CHMJobCreator.BCONFIG_TASK_ID), '1')
            self.assertEqual(bconfig.sections(), ['1'])
        finally:
            shutil.rmtree(temp_dir)

    def test_generate_batched_jobs_list_two_jobs_one_job_per_node(self):
        temp_dir = tempfile.mkdtemp()
        try:
            checker = Mock()
            checker.get_incomplete_jobs_list = Mock(return_value=['1', '2'])
            gen = BatchedJobsListGenerator(checker, 1)
            cfile = os.path.join(temp_dir, 'foo.config')
            self.assertEqual(gen.generate_batched_jobs_list(cfile), 2)
            self.assertTrue(os.path.isfile(cfile))
            bconfig = configparser.ConfigParser()
            bconfig.read(cfile)
            self.assertEqual(bconfig.get('1',
                                         CHMJobCreator.BCONFIG_TASK_ID), '1')
            self.assertEqual(bconfig.get('2',
                                         CHMJobCreator.BCONFIG_TASK_ID), '2')

            self.assertEqual(bconfig.sections(), ['1', '2'])

        finally:
            shutil.rmtree(temp_dir)

    def test_generate_batched_jobs_list_two_jobs_two_job_per_node(self):
        temp_dir = tempfile.mkdtemp()
        try:
            checker = Mock()
            checker.get_incomplete_jobs_list = Mock(return_value=['1', '2'])
            gen = BatchedJobsListGenerator(checker, 2)
            cfile = os.path.join(temp_dir, 'foo.config')
            self.assertEqual(gen.generate_batched_jobs_list(cfile), 1)
            self.assertTrue(os.path.isfile(cfile))
            bconfig = configparser.ConfigParser()
            bconfig.read(cfile)
            self.assertEqual(bconfig.get('1',
                                         CHMJobCreator.BCONFIG_TASK_ID), '1,2')
            self.assertEqual(bconfig.sections(), ['1'])

        finally:
            shutil.rmtree(temp_dir)
