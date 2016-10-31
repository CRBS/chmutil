#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_chmopts
----------------------------------

Tests for `CHMOpts` class
"""


import sys
import unittest

from chmutil.core import CHMOpts



class TestChmutil(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_constructor(self):
        opts = CHMOpts(None, None, None, None, None)
        self.assertEqual(opts.get_images(), None)
        self.assertEqual(opts.get_model(), None)
        self.assertEqual(opts.get_out_dir(), None)
        self.assertEqual(opts.get_tile_size(), None)
        self.assertEqual(opts.get_overlap_size(), None)
        self.assertEqual(opts.get_tile_height(), '')
        self.assertEqual(opts.get_tile_width(), '')
        self.assertEqual(opts.get_overlap_height(), '')
        self.assertEqual(opts.get_overlap_width(), '')
        self.assertEqual(opts.get_number_jobs_per_node(), 1)
        self.assertEqual(opts.get_number_tiles_per_job(), 1)
        self.assertEqual(opts.get_disable_histogram_eq_val(), True)

        opts = CHMOpts('images', 'model', 'out', '500x600', '20x30',
                       number_tiles_per_job=122,
                       jobs_per_node=12,
                       disablehisteq=False)
        self.assertEqual(opts.get_images(), 'images')
        self.assertEqual(opts.get_model(), 'model')
        self.assertEqual(opts.get_out_dir(), 'out')
        self.assertEqual(opts.get_tile_size(), '500x600')
        self.assertEqual(opts.get_overlap_size(), '20x30')
        self.assertEqual(opts.get_tile_height(), 600)
        self.assertEqual(opts.get_tile_width(), 500)
        self.assertEqual(opts.get_overlap_height(), 30)
        self.assertEqual(opts.get_overlap_width(), 20)
        self.assertEqual(opts.get_number_jobs_per_node(), 12)
        self.assertEqual(opts.get_number_tiles_per_job(), 122)
        self.assertEqual(opts.get_disable_histogram_eq_val(), False)



    def test_extract_width_and_height(self):
        opts = CHMOpts(None, None, None, None, None)

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




