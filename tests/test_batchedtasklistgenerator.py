#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_batchedtasklistgenerator.py
----------------------------------

Tests for `BatchedTasksListGenerator` class
"""

import os
import tempfile
import shutil
import unittest
import configparser

from chmutil.cluster import CHMJobCreator
from chmutil.cluster import BatchedTasksListGenerator
from chmutil.cluster import InvalidConfigFileError
from chmutil.cluster import InvalidTaskListError


class TestBatchedTasksListGenerator(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_write_batched_task_config_no_preexisting_config(self):
        temp_dir = tempfile.mkdtemp()
        try:
            gen = BatchedTasksListGenerator(1)
            cfile = os.path.join(temp_dir, 'foo.config')
            bconfig = configparser.ConfigParser()
            bconfig.set('', 'somekey', 'val')

            gen._write_batched_task_config(bconfig, cfile)
            self.assertTrue(os.path.isfile(cfile))
        finally:
            shutil.rmtree(temp_dir)

    def test_write_batched_task_config_with_preexisting_config(self):
        temp_dir = tempfile.mkdtemp()
        try:
            gen = BatchedTasksListGenerator(1)
            cfile = os.path.join(temp_dir, 'foo.config')
            bconfig = configparser.ConfigParser()
            bconfig.set('', 'somekey', 'val')

            gen._write_batched_task_config(bconfig, cfile)
            self.assertTrue(os.path.isfile(cfile))
            bconfig.set('', 'somekey', 'anotherval')
            gen._write_batched_task_config(bconfig, cfile)
            ocfile = cfile + BatchedTasksListGenerator.OLD_SUFFIX
            self.assertTrue(os.path.isfile(ocfile))
            bconfig.read(ocfile)
            self.assertEqual(bconfig.get('DEFAULT', 'somekey'), 'val')

            bconfig.read(cfile)
            self.assertEqual(bconfig.get('DEFAULT', 'somekey'), 'anotherval')
        finally:
            shutil.rmtree(temp_dir)

    def test_generate_batched_tasks_list_none_for_configfile(self):
            gen = BatchedTasksListGenerator(1)
            try:
                gen.write_batched_config(None, None)
                self.fail('Expected InvalidConfigFileError')
            except InvalidConfigFileError as e:
                self.assertEqual(str(e), 'configfile passed in cannot be null')

    def test_generate_batched_tasks_list_no_tasks(self):
        temp_dir = tempfile.mkdtemp()
        try:
            gen = BatchedTasksListGenerator(1)
            cfile = os.path.join(temp_dir, 'foo.config')
            self.assertEqual(gen.write_batched_config(cfile, []),
                             0)
        finally:
            shutil.rmtree(temp_dir)

    def test_generate_batched_tasks_list_none_for_task_lists(self):
        temp_dir = tempfile.mkdtemp()
        try:
            gen = BatchedTasksListGenerator(1)
            cfile = os.path.join(temp_dir, 'foo.config')
            try:
                gen.write_batched_config(cfile, None)
                self.fail("Expected InvalidTaskListError")
            except InvalidTaskListError as e:
                self.assertEqual(str(e), 'task list cannot be None')
        finally:
            shutil.rmtree(temp_dir)

    def test_generate_batched_tasks_list_one_task_one_task_per_node(self):
        temp_dir = tempfile.mkdtemp()
        try:
            gen = BatchedTasksListGenerator(1)
            cfile = os.path.join(temp_dir, 'foo.config')
            self.assertEqual(gen.write_batched_config(cfile, ['1']), 1)
            self.assertTrue(os.path.isfile(cfile))
            bconfig = configparser.ConfigParser()
            bconfig.read(cfile)
            self.assertEqual(bconfig.get('1',
                                         CHMJobCreator.BCONFIG_TASK_ID), '1')
            self.assertEqual(bconfig.sections(), ['1'])
        finally:
            shutil.rmtree(temp_dir)

    def test_generate_batched_tasks_list_one_task_five_task_per_node(self):
        temp_dir = tempfile.mkdtemp()
        try:
            gen = BatchedTasksListGenerator(5)
            cfile = os.path.join(temp_dir, 'foo.config')
            self.assertEqual(gen.write_batched_config(cfile, ['1']), 1)
            self.assertTrue(os.path.isfile(cfile))
            bconfig = configparser.ConfigParser()
            bconfig.read(cfile)
            self.assertEqual(bconfig.get('1',
                                         CHMJobCreator.BCONFIG_TASK_ID), '1')
            self.assertEqual(bconfig.sections(), ['1'])
        finally:
            shutil.rmtree(temp_dir)

    def test_generate_batched_tasks_list_two_tasks_one_task_per_node(self):
        temp_dir = tempfile.mkdtemp()
        try:
            gen = BatchedTasksListGenerator(1)
            cfile = os.path.join(temp_dir, 'foo.config')
            self.assertEqual(gen.write_batched_config(cfile,
                                                      ['1', '2']), 2)
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

    def test_generate_batched_tasks_list_two_tasks_two_task_per_node(self):
        temp_dir = tempfile.mkdtemp()
        try:
            gen = BatchedTasksListGenerator(2)
            cfile = os.path.join(temp_dir, 'foo.config')
            self.assertEqual(gen.write_batched_config(cfile,
                                                      ['1', '2']), 1)
            self.assertTrue(os.path.isfile(cfile))
            bconfig = configparser.ConfigParser()
            bconfig.read(cfile)
            self.assertEqual(bconfig.get('1',
                                         CHMJobCreator.BCONFIG_TASK_ID), '1,2')
            self.assertEqual(bconfig.sections(), ['1'])

        finally:
            shutil.rmtree(temp_dir)


if __name__ == '__main__':
    unittest.main()
