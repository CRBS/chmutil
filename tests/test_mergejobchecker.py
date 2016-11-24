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

from chmutil.cluster import MergeJobChecker
from chmutil.core import CHMJobCreator


class TestMergeJobChecker(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_get_incomplete_jobs_list_empty_config(self):
        temp_dir = tempfile.mkdtemp()
        try:
            config = configparser.ConfigParser()
            checker = MergeJobChecker(config)

            # test with empty config
            self.assertEqual(checker.get_incomplete_jobs_list(), [])

            # test with config with 2 entries no files on filesystem
            img_one = os.path.join(temp_dir, 'image_one.png')
            config.add_section('1')
            config.set('1', CHMJobCreator.MERGE_OUTPUT_IMAGE, img_one)

            img_two = os.path.join(temp_dir, 'image_two.png')
            config.add_section('2')
            config.set('2', CHMJobCreator.MERGE_OUTPUT_IMAGE, img_two)
            res = checker.get_incomplete_jobs_list()
            self.assertEqual(res, ['1', '2'])

            open(img_one, 'a').close()
            res = checker.get_incomplete_jobs_list()
            self.assertEqual(res, ['2'])

            open(img_two, 'a').close()
            res = checker.get_incomplete_jobs_list()
            self.assertEqual(res, [])


        finally:
            shutil.rmtree(temp_dir)


if __name__ == '__main__':
    unittest.main()
