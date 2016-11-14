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

            j_list = gen.get_incomplete_jobs_list()

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

            j_list = gen.get_incomplete_jobs_list()

            self.assertEqual(len(j_list), 1)
            self.assertEqual(j_list[0], '2')

        finally:
            shutil.rmtree(temp_dir)

    def test_write_batched_job_config(self):
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
            bconfig = configparser.ConfigParser()
            bconfig.add_section('1')
            bconfig.set('', CHMJobCreator.BCONFIG_TASK_ID,
                        ' 1,2,3')
            # try writing where previous file does NOT exist
            gen._write_batched_job_config(bconfig)
            self.assertTrue(os.path.isfile(chmconfig.get_batchedjob_config()))
            self.assertFalse(os.path.isfile(chmconfig.get_batchedjob_config() +
                                            BatchedJobsListGenerator.
                                            OLD_SUFFIX))

            # try writing where there is a previous file
            gen._write_batched_job_config(bconfig)
            self.assertTrue(os.path.isfile(chmconfig.get_batchedjob_config()))
            self.assertTrue(os.path.isfile(chmconfig.get_batchedjob_config() +
                                           BatchedJobsListGenerator.
                                           OLD_SUFFIX))

        finally:
            shutil.rmtree(temp_dir)

    def test_generate_batched_jobs_list_all_jobs_complete(self):
        temp_dir = tempfile.mkdtemp()
        try:
            img = os.path.join(temp_dir, 'image2.png')
            open(img, 'a').close()

            config = configparser.ConfigParser()
            config.add_section('1')
            config.set('1', CHMJobCreator.CONFIG_OUTPUT_IMAGE, img)
            chmconfig = CHMConfig('images', 'model', temp_dir,
                                  '500x500', '20x20',
                                  config=config)

            gen = BatchedJobsListGenerator(chmconfig)
            self.assertEqual(gen.generate_batched_jobs_list(), 0)
        finally:
            shutil.rmtree(temp_dir)

    def test_generate_batched_jobs_list_one_job_not_complete(self):
        temp_dir = tempfile.mkdtemp()
        try:
            img = os.path.join(temp_dir, 'image2.png')

            config = configparser.ConfigParser()
            config.add_section('1')
            config.set('1', CHMJobCreator.CONFIG_OUTPUT_IMAGE, img)
            chmconfig = CHMConfig('images', 'model', temp_dir,
                                  '500x500', '20x20',
                                  config=config)

            gen = BatchedJobsListGenerator(chmconfig)
            self.assertEqual(gen.generate_batched_jobs_list(), 1)
            bconfig = configparser.ConfigParser()
            bconfig.read(chmconfig.get_batchedjob_config())
            self.assertEqual(bconfig.get('1', CHMJobCreator.BCONFIG_TASK_ID),
                             '1')
        finally:
            shutil.rmtree(temp_dir)

    def test_generate_batched_jobs_list_two_jobs_not_complete(self):
        temp_dir = tempfile.mkdtemp()
        try:
            img = os.path.join(temp_dir, 'image2.png')
            img2 = os.path.join(temp_dir, 'image3.png')

            config = configparser.ConfigParser()
            config.add_section('1')
            config.set('1', CHMJobCreator.CONFIG_OUTPUT_IMAGE, img)
            config.add_section('2')
            config.set('2', CHMJobCreator.CONFIG_OUTPUT_IMAGE, img2)

            chmconfig = CHMConfig('images', 'model', temp_dir,
                                  '500x500', '20x20',
                                  config=config)

            gen = BatchedJobsListGenerator(chmconfig)
            self.assertEqual(gen.generate_batched_jobs_list(), 2)
            bconfig = configparser.ConfigParser()
            bconfig.read(chmconfig.get_batchedjob_config())
            self.assertEqual(bconfig.get('1', CHMJobCreator.BCONFIG_TASK_ID),
                             '1')
            self.assertEqual(bconfig.get('2', CHMJobCreator.BCONFIG_TASK_ID),
                             '2')
        finally:
            shutil.rmtree(temp_dir)

    def test_generate_batched_jobs_list_two_jobs_batched_not_complete(self):
        temp_dir = tempfile.mkdtemp()
        try:
            img = os.path.join(temp_dir, 'image2.png')
            img2 = os.path.join(temp_dir, 'image3.png')

            config = configparser.ConfigParser()
            config.add_section('1')
            config.set('1', CHMJobCreator.CONFIG_OUTPUT_IMAGE, img)
            config.add_section('2')
            config.set('2', CHMJobCreator.CONFIG_OUTPUT_IMAGE, img2)

            chmconfig = CHMConfig('images', 'model', temp_dir,
                                  '500x500', '20x20',
                                  jobs_per_node=2,
                                  config=config)

            gen = BatchedJobsListGenerator(chmconfig)
            self.assertEqual(gen.generate_batched_jobs_list(), 1)
            bconfig = configparser.ConfigParser()
            bconfig.read(chmconfig.get_batchedjob_config())
            self.assertEqual(bconfig.get('1', CHMJobCreator.BCONFIG_TASK_ID),
                             '1,2')
        finally:
            shutil.rmtree(temp_dir)
