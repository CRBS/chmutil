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

from chmutil.core import CHMConfig
from chmutil.core import CHMJobCreator
from chmutil.cluster import BatchedJobsListGenerator


class TestBatchedJobsListGenerator(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_get_incomplete_jobs_list_one_non_existant_image(self):
        temp_dir = tempfile.mkdtemp()
        try:
            noimg = os.path.join(temp_dir, 'image1.png')

            config = configparser.ConfigParser()
            config.add_section('1')
            config.set('1', CHMJobCreator.CONFIG_OUTPUT_IMAGE, noimg)
            chmconfig = CHMConfig('images', 'model', temp_dir,
                                  '500x500', '20x20',
                                  config=config)

            gen = BatchedJobsListGenerator(chmconfig)

            j_list = gen._get_incomplete_jobs_list()

            self.assertEqual(len(j_list), 1)
            self.assertEqual(j_list[0], '1')

        finally:
            shutil.rmtree(temp_dir)


    def test_get_incomplete_jobs_list_two_images_one_missing(self):
        temp_dir = tempfile.mkdtemp()
        try:
            noimg = os.path.join(temp_dir, 'image1.png')
            img = os.path.join(temp_dir, 'image2.png')
            open(img, 'a').close()

            config = configparser.ConfigParser()
            config.add_section('1')
            config.set('1', CHMJobCreator.CONFIG_OUTPUT_IMAGE, img)
            config.add_section('2')
            config.set('2', CHMJobCreator.CONFIG_OUTPUT_IMAGE, noimg)

            chmconfig = CHMConfig('images', 'model', temp_dir,
                                  '500x500', '20x20',
                                  config=config)

            gen = BatchedJobsListGenerator(chmconfig)

            j_list = gen._get_incomplete_jobs_list()

            self.assertEqual(len(j_list), 1)
            self.assertEqual(j_list[0], '2')

        finally:
            shutil.rmtree(temp_dir)

