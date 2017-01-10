#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_chmjobchecker
----------------------------------

Tests for `CHMJobChecker in cluster`
"""

import os
import tempfile
import unittest
import configparser
import shutil

from chmutil.cluster import MergeTaskChecker
from chmutil.core import CHMJobCreator


class TestMergeJobChecker(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_get_incomplete_jobs_list_full_paths(self):
        temp_dir = tempfile.mkdtemp()
        try:
            config = configparser.ConfigParser()
            checker = MergeTaskChecker(config)

            # test with empty config
            self.assertEqual(checker.get_incomplete_tasks_list(), [])

            # test with config with 2 entries no files on filesystem
            img_one = os.path.join(temp_dir, 'image_one.png')
            config.add_section('1')
            config.set('1', CHMJobCreator.MERGE_OUTPUT_IMAGE, img_one)

            img_two = os.path.join(temp_dir, 'image_two.png')
            config.add_section('2')
            config.set('2', CHMJobCreator.MERGE_OUTPUT_IMAGE, img_two)
            res = checker.get_incomplete_tasks_list()
            self.assertEqual(res, ['1', '2'])

            open(img_one, 'a').close()
            res = checker.get_incomplete_tasks_list()
            self.assertEqual(res, ['2'])

            open(img_two, 'a').close()
            res = checker.get_incomplete_tasks_list()
            self.assertEqual(res, [])
        finally:
            shutil.rmtree(temp_dir)

    def test_get_incomplete_jobs_list_relative_paths(self):
        temp_dir = tempfile.mkdtemp()
        try:
            config = configparser.ConfigParser()
            checker = MergeTaskChecker(config)

            # test with empty config
            self.assertEqual(checker.get_incomplete_tasks_list(), [])

            rundir = os.path.join(temp_dir, CHMJobCreator.RUN_DIR)
            os.makedirs(rundir, mode=0o755)
            # set jobdir in config
            config.set('', CHMJobCreator.JOB_DIR, temp_dir)

            # test with config with 2 entries no files on filesystem
            config.add_section('1')
            config.set('1', CHMJobCreator.MERGE_OUTPUT_IMAGE, 'image_one.png')

            img_two = os.path.join(temp_dir, 'image_two.png')
            config.add_section('2')
            config.set('2', CHMJobCreator.MERGE_OUTPUT_IMAGE, 'image_two.png')
            res = checker.get_incomplete_tasks_list()
            self.assertEqual(res, ['1', '2'])

            img_one = os.path.join(rundir, 'image_one.png')
            open(img_one, 'a').close()
            res = checker.get_incomplete_tasks_list()
            self.assertEqual(res, ['2'])

            img_two = os.path.join(rundir, 'image_two.png')
            open(img_two, 'a').close()
            res = checker.get_incomplete_tasks_list()
            self.assertEqual(res, [])
        finally:
            shutil.rmtree(temp_dir)


if __name__ == '__main__':
    unittest.main()
