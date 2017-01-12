#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_chmopts
----------------------------------

Tests for `CHMOpts` class
"""

import os
import unittest

from chmutil.core import CHMConfig
from chmutil.core import CHMJobCreator


class TestCHMConfig(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_constructor(self):
        opts = CHMConfig(None, None, None, None, None)
        self.assertEqual(opts.get_images(), None)
        self.assertEqual(opts.get_model(), None)
        self.assertEqual(opts.get_out_dir(), None)
        self.assertEqual(opts.get_tile_size(), None)
        self.assertEqual(opts.get_overlap_size(), None)
        self.assertEqual(opts.get_tile_height(), '')
        self.assertEqual(opts.get_tile_width(), '')
        self.assertEqual(opts.get_overlap_height(), '')
        self.assertEqual(opts.get_overlap_width(), '')
        self.assertEqual(opts.get_number_tasks_per_node(), 1)
        self.assertEqual(opts.get_number_tiles_per_task(), 1)
        self.assertEqual(opts.get_disable_histogram_eq_val(), True)
        self.assertEqual(opts.get_config(), None)
        self.assertEqual(opts.get_job_config(), CHMJobCreator.CONFIG_FILE_NAME)
        self.assertEqual(opts.get_batchedjob_config_file_path(),
                         CHMJobCreator.CONFIG_BATCHED_TASKS_FILE_NAME)
        self.assertEqual(opts.get_batched_mergejob_config_file_path(),
                         CHMJobCreator.MERGE_CONFIG_BATCHED_TASKS_FILE_NAME)
        self.assertEqual(opts.get_run_dir(),
                         CHMJobCreator.RUN_DIR)
        self.assertEqual(opts.get_script_bin(), '')
        self.assertEqual(opts.get_job_name(), 'chmjob')
        self.assertEqual(opts.get_walltime(), '12:00:00')
        self.assertEqual(opts.get_max_image_pixels(), 768000000)
        self.assertEqual(opts.get_merge_walltime(), '12:00:00')
        self.assertEqual(opts.get_mergejob_name(), 'mergechmjob')
        self.assertEqual(opts.get_max_chm_memory_in_gb(), 10)
        self.assertEqual(opts.get_max_merge_memory_in_gb(), 10)
        self.assertEqual(opts.get_account(), '')

        opts = CHMConfig('images', 'model', 'out', '500x600', '20x30',
                         number_tiles_per_task=122,
                         tasks_per_node=12,
                         disablehisteq=False,
                         config='hi',
                         scriptbin='/foo',
                         jobname='yo',
                         walltime='1:2:3',
                         mergewalltime='4:5:6',
                         mergejobname='mergy',
                         max_image_pixels=10,
                         max_chm_memory_in_gb=5,
                         max_merge_memory_in_gb=7,
                         account='yo12')
        self.assertEqual(opts.get_images(), 'images')
        self.assertEqual(opts.get_model(), 'model')
        self.assertEqual(opts.get_out_dir(), 'out')
        self.assertEqual(opts.get_tile_size(), '500x600')
        self.assertEqual(opts.get_overlap_size(), '20x30')
        self.assertEqual(opts.get_tile_height(), 600)
        self.assertEqual(opts.get_tile_width(), 500)
        self.assertEqual(opts.get_overlap_height(), 30)
        self.assertEqual(opts.get_overlap_width(), 20)
        self.assertEqual(opts.get_number_tasks_per_node(), 12)
        self.assertEqual(opts.get_number_tiles_per_task(), 122)
        self.assertEqual(opts.get_disable_histogram_eq_val(), False)
        self.assertEqual(opts.get_config(), 'hi')
        self.assertEqual(opts.get_job_config(),
                         os.path.join('out', CHMJobCreator.CONFIG_FILE_NAME))
        self.assertEqual(opts.get_batchedjob_config_file_path(),
                         os.path.join('out',
                                      CHMJobCreator.
                                      CONFIG_BATCHED_TASKS_FILE_NAME))
        self.assertEqual(opts.get_batched_mergejob_config_file_path(),
                         os.path.join('out',
                                      CHMJobCreator.
                                      MERGE_CONFIG_BATCHED_TASKS_FILE_NAME))
        self.assertEqual(opts.get_run_dir(),
                         os.path.join(opts.get_out_dir(),
                                      CHMJobCreator.RUN_DIR))
        self.assertEqual(opts.get_script_bin(), '/foo')
        self.assertEqual(opts.get_job_name(), 'yo')
        self.assertEqual(opts.get_walltime(), '1:2:3')
        self.assertEqual(opts.get_max_image_pixels(), 10)
        self.assertEqual(opts.get_merge_walltime(), '4:5:6')
        self.assertEqual(opts.get_mergejob_name(), 'mergy')
        self.assertEqual(opts.get_stdout_dir(),
                         os.path.join(opts.get_run_dir(),
                                      CHMJobCreator.STDOUT_DIR))
        self.assertEqual(opts.get_merge_stdout_dir(),
                         os.path.join(opts.get_run_dir(),
                                      CHMJobCreator.MERGE_STDOUT_DIR))
        self.assertEqual(opts.get_max_chm_memory_in_gb(), 5)
        self.assertEqual(opts.get_max_merge_memory_in_gb(), 7)

        opts.set_config('bye')
        self.assertEqual(opts.get_config(), 'bye')
        self.assertEqual(opts.get_account(), 'yo12')

    def test_extract_width_and_height(self):
        opts = CHMConfig(None, None, None, None, None)

        w, h = opts._extract_width_and_height(None)
        self.assertEqual(w, '')
        self.assertEqual(h, '')

        w, h = opts._extract_width_and_height('')
        self.assertEqual(w, '')
        self.assertEqual(h, '')

        w, h = opts._extract_width_and_height(50)
        self.assertEqual(w, 50)
        self.assertEqual(h, 50)

        w, h = opts._extract_width_and_height('300')
        self.assertEqual(w, 300)
        self.assertEqual(h, 300)

        w, h = opts._extract_width_and_height('10x20')
        self.assertEqual(w, 10)
        self.assertEqual(h, 20)

        w, h = opts._extract_width_and_height('10x20x')
        self.assertEqual(w, 10)
        self.assertEqual(h, 20)
        try:
            w, h = opts._extract_width_and_height('x')
            self.fail('Expected ValueError')
        except ValueError:
            pass


if __name__ == '__main__':
    unittest.main()
