#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_chmconfigfromconfigfactory.py
----------------------------------

Tests for `CHMConfigFromConfigFactory` class
"""

import os
import tempfile
import shutil
import unittest
import configparser

from chmutil.core import CHMJobCreator
from chmutil.core import CHMConfigFromConfigFactory
from chmutil.core import InvalidJobDirError
from chmutil.core import LoadConfigError


class TestCHMConfigFromConfigFactory(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_constructor(self):
        try:
            CHMConfigFromConfigFactory(None)
            self.fail('Expected InvalidJobDirError')
        except InvalidJobDirError as e:
            self.assertEqual(str(e),
                             'job directory passed in cannot be null')
        fac = CHMConfigFromConfigFactory('foo')
        self.assertEqual(fac._job_dir, 'foo')

    def test_get_chmconfig_raise_exception(self):
        temp_dir = tempfile.mkdtemp()
        try:
            fac = CHMConfigFromConfigFactory(temp_dir)
            try:
                fac.get_chmconfig()
                self.fail('expected LoadConfigError')
            except LoadConfigError as e:
                cfile = os.path.join(temp_dir,
                                     CHMJobCreator.CONFIG_FILE_NAME)
                self.assertEqual(str(e), cfile +
                                 ' configuration file does not exist')
        finally:
            shutil.rmtree(temp_dir)

    def test_get_chmconfig_default_values_no_account(self):
        temp_dir = tempfile.mkdtemp()
        try:
            cfile = os.path.join(temp_dir,
                                 CHMJobCreator.CONFIG_FILE_NAME)
            config = configparser.ConfigParser()
            config.set('', CHMJobCreator.CONFIG_IMAGES, 'images')
            config.set('', CHMJobCreator.CONFIG_MODEL, 'model')
            config.set('', CHMJobCreator.CONFIG_TILE_SIZE, '500x600')
            config.set('', CHMJobCreator.CONFIG_OVERLAP_SIZE, '10x20')
            config.set('', CHMJobCreator.CONFIG_TILES_PER_TASK, 'tilesperjob')
            config.set('', CHMJobCreator.CONFIG_TASKS_PER_NODE, 'jobspernode')
            config.set('', CHMJobCreator.CONFIG_DISABLE_HISTEQ_IMAGES, 'True')
            config.set('', CHMJobCreator.CONFIG_CHM_BIN, 'chmbin')
            config.set('', CHMJobCreator.CONFIG_CLUSTER, 'mycluster')
            f = open(cfile, 'w')
            config.write(f)
            f.flush()
            f.close()

            fac = CHMConfigFromConfigFactory(temp_dir)
            chmconfig = fac.get_chmconfig()
            self.assertEqual(chmconfig.get_out_dir(), temp_dir)
            self.assertEqual(chmconfig.get_chm_binary(), 'chmbin')
            self.assertEqual(chmconfig.get_script_bin(), '')
            self.assertEqual(chmconfig.get_disable_histogram_eq_val(), True)
            self.assertEqual(chmconfig.get_images(), 'images')
            self.assertEqual(chmconfig.get_model(), 'model')
            self.assertEqual(chmconfig.get_number_tasks_per_node(),
                             'jobspernode')
            self.assertEqual(chmconfig.get_number_tiles_per_task(),
                             'tilesperjob')
            self.assertEqual(chmconfig.get_tile_height(), 600)
            self.assertEqual(chmconfig.get_tile_width(), 500)
            self.assertEqual(chmconfig.get_tile_size(), '500x600')
            self.assertEqual(chmconfig.get_overlap_height(), 20)
            self.assertEqual(chmconfig.get_overlap_width(), 10)
            self.assertEqual(chmconfig.get_overlap_size(), '10x20')
            self.assertEqual(chmconfig.get_cluster(), 'mycluster')
            self.assertEqual(chmconfig.get_account(), '')

            config.set('', CHMJobCreator.CONFIG_DISABLE_HISTEQ_IMAGES, 'False')
            f = open(cfile, 'w')
            config.write(f)
            f.flush()
            f.close()
            chmconfig = fac.get_chmconfig()
            self.assertEqual(chmconfig.get_disable_histogram_eq_val(), False)

        finally:
            shutil.rmtree(temp_dir)

    def test_get_chmconfig_default_values_with_account_set(self):
        temp_dir = tempfile.mkdtemp()
        try:
            cfile = os.path.join(temp_dir,
                                 CHMJobCreator.CONFIG_FILE_NAME)
            config = configparser.ConfigParser()
            config.set('', CHMJobCreator.CONFIG_IMAGES, 'images')
            config.set('', CHMJobCreator.CONFIG_MODEL, 'model')
            config.set('', CHMJobCreator.CONFIG_TILE_SIZE, '500x600')
            config.set('', CHMJobCreator.CONFIG_OVERLAP_SIZE, '10x20')
            config.set('', CHMJobCreator.CONFIG_TILES_PER_TASK, 'tilesperjob')
            config.set('', CHMJobCreator.CONFIG_TASKS_PER_NODE, 'jobspernode')
            config.set('', CHMJobCreator.CONFIG_DISABLE_HISTEQ_IMAGES, 'True')
            config.set('', CHMJobCreator.CONFIG_CHM_BIN, 'chmbin')
            config.set('', CHMJobCreator.CONFIG_CLUSTER, 'mycluster')
            config.set('', CHMJobCreator.CONFIG_ACCOUNT, 'gg123')
            f = open(cfile, 'w')
            config.write(f)
            f.flush()
            f.close()

            fac = CHMConfigFromConfigFactory(temp_dir)
            chmconfig = fac.get_chmconfig()
            self.assertEqual(chmconfig.get_out_dir(), temp_dir)
            self.assertEqual(chmconfig.get_chm_binary(), 'chmbin')
            self.assertEqual(chmconfig.get_script_bin(), '')
            self.assertEqual(chmconfig.get_disable_histogram_eq_val(), True)
            self.assertEqual(chmconfig.get_images(), 'images')
            self.assertEqual(chmconfig.get_model(), 'model')
            self.assertEqual(chmconfig.get_number_tasks_per_node(),
                             'jobspernode')
            self.assertEqual(chmconfig.get_number_tiles_per_task(),
                             'tilesperjob')
            self.assertEqual(chmconfig.get_tile_height(), 600)
            self.assertEqual(chmconfig.get_tile_width(), 500)
            self.assertEqual(chmconfig.get_tile_size(), '500x600')
            self.assertEqual(chmconfig.get_overlap_height(), 20)
            self.assertEqual(chmconfig.get_overlap_width(), 10)
            self.assertEqual(chmconfig.get_overlap_size(), '10x20')
            self.assertEqual(chmconfig.get_cluster(), 'mycluster')
            self.assertEqual(chmconfig.get_account(), 'gg123')

            config.set('', CHMJobCreator.CONFIG_DISABLE_HISTEQ_IMAGES, 'False')
            f = open(cfile, 'w')
            config.write(f)
            f.flush()
            f.close()
            chmconfig = fac.get_chmconfig()
            self.assertEqual(chmconfig.get_disable_histogram_eq_val(), False)

        finally:
            shutil.rmtree(temp_dir)

    def test_get_chmconfig_skip_loading_config_true_and_skip_merge_false(self):
        temp_dir = tempfile.mkdtemp()
        try:
            cfile = os.path.join(temp_dir,
                                 CHMJobCreator.MERGE_CONFIG_FILE_NAME)
            config = configparser.ConfigParser()
            config.set('', CHMJobCreator.CONFIG_IMAGES, 'images')
            config.set('', CHMJobCreator.CONFIG_CLUSTER, 'yocluster')
            config.set('', CHMJobCreator.MERGE_TASKS_PER_NODE, '4')
            f = open(cfile, 'w')
            config.write(f)
            f.flush()
            f.close()

            fac = CHMConfigFromConfigFactory(temp_dir)
            chmconfig = fac.get_chmconfig(skip_loading_config=True,
                                          skip_loading_mergeconfig=False)

            self.assertEqual(chmconfig.get_out_dir(), temp_dir)
            mcon = chmconfig.get_merge_config()
            self.assertEqual(mcon.get(CHMJobCreator.CONFIG_DEFAULT,
                                      CHMJobCreator.CONFIG_IMAGES),
                             'images')
            self.assertEqual(mcon.get(CHMJobCreator.CONFIG_DEFAULT,
                                      CHMJobCreator.CONFIG_CLUSTER),
                             'yocluster')
            self.assertEqual(mcon.getint(CHMJobCreator.CONFIG_DEFAULT,
                                         CHMJobCreator.MERGE_TASKS_PER_NODE),
                             4)

            self.assertEqual(chmconfig.get_cluster(), 'yocluster')

        finally:
            shutil.rmtree(temp_dir)


if __name__ == '__main__':
    unittest.main()
