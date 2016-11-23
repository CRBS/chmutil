#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_chmutil
----------------------------------

Tests for `chmutil` module.
"""

import unittest

from chmutil.core import CHMArgGenerator
from chmutil.core import CHMConfig
from chmutil.core import ImageStats
from chmutil.core import OverlapTooLargeForTileSizeError


class TestCHMArgGenerator(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_get_number_of_tiles_tuple_no_overlap(self):

        # no overlap test with various image_stats sizes
        opts = CHMConfig('/foo', 'model', 'outdir', '100x200', '0x0')
        gen = CHMArgGenerator(opts)

        # 100x200 image
        im_stats = ImageStats('f', 100, 200, 'PNG')
        w, h = gen._get_number_of_tiles_tuple(im_stats)
        self.assertEqual(w, 1)
        self.assertEqual(h, 1)

        # 101x201 image
        im_stats = ImageStats('f', 101, 201, 'PNG')
        w, h = gen._get_number_of_tiles_tuple(im_stats)
        self.assertEqual(w, 2)
        self.assertEqual(h, 2)

        # 199x299 image
        im_stats = ImageStats('f', 199, 299, 'PNG')
        w, h = gen._get_number_of_tiles_tuple(im_stats)
        self.assertEqual(w, 2)
        self.assertEqual(h, 2)

        # 505x650 image
        im_stats = ImageStats('f', 505, 650, 'PNG')
        w, h = gen._get_number_of_tiles_tuple(im_stats)
        self.assertEqual(w, 6)
        self.assertEqual(h, 4)

        # 32000x24000 image
        im_stats = ImageStats('f', 32000, 24000, 'PNG')
        w, h = gen._get_number_of_tiles_tuple(im_stats)
        self.assertEqual(w, 320)
        self.assertEqual(h, 120)

    def test_get_number_of_tiles_tuple_with_overlap(self):

        try:
            opts = CHMConfig('/foo', 'model', 'outdir', '100x200', '50x20')
            CHMArgGenerator(opts)
        except OverlapTooLargeForTileSizeError as e:
            self.assertEqual(str(e), 'Overlap width too large for tile')

        try:
            opts = CHMConfig('/foo', 'model', 'outdir', '100x200', '40x110')
            CHMArgGenerator(opts)
        except OverlapTooLargeForTileSizeError as e:
            self.assertEqual(str(e), 'Overlap height too large for tile')

        # overlap test with various image_stats sizes
        opts = CHMConfig('/foo', 'model', 'outdir', '100x200', '20x20')
        gen = CHMArgGenerator(opts)

        # 100x200 image
        im_stats = ImageStats('f', 100, 200, 'PNG')
        w, h = gen._get_number_of_tiles_tuple(im_stats)
        self.assertEqual(w, 2)
        self.assertEqual(h, 2)

        # 101x201 image
        im_stats = ImageStats('f', 101, 201, 'PNG')
        w, h = gen._get_number_of_tiles_tuple(im_stats)
        self.assertEqual(w, 2)
        self.assertEqual(h, 2)

        # 199x299 image
        im_stats = ImageStats('f', 199, 299, 'PNG')
        w, h = gen._get_number_of_tiles_tuple(im_stats)
        self.assertEqual(w, 4)
        self.assertEqual(h, 2)

        # 505x650 image
        im_stats = ImageStats('f', 505, 650, 'PNG')
        w, h = gen._get_number_of_tiles_tuple(im_stats)
        self.assertEqual(w, 9)
        self.assertEqual(h, 5)

        # 32000x24000 image
        im_stats = ImageStats('f', 32000, 24000, 'PNG')
        w, h = gen._get_number_of_tiles_tuple(im_stats)
        self.assertEqual(w, 534)
        self.assertEqual(h, 150)

    def test_get_args_one_tile(self):
        opts = CHMConfig('/foo', 'model', 'outdir', '100x200', '0x0')
        gen = CHMArgGenerator(opts)

        # 100x200 image
        im_stats = ImageStats('f', 100, 200, 'PNG')
        tlist = gen.get_args(im_stats)
        self.assertEqual(len(tlist), 1)
        self.assertEqual(tlist, [['-t 1,1']])

    def test_get_args_four_tiles(self):
        opts = CHMConfig('/foo', 'model', 'outdir', '100x200', '0x0')
        gen = CHMArgGenerator(opts)

        # 100x200 image
        im_stats = ImageStats('f', 101, 201, 'PNG')
        tlist = gen.get_args(im_stats)
        self.assertEqual(len(tlist), 4)
        self.assertEqual(tlist, [['-t 1,1'], ['-t 1,2'],
                                 ['-t 2,1'], ['-t 2,2']])


if __name__ == '__main__':
    unittest.main()
