#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_chmutil
----------------------------------

Tests for `chmutil` module.
"""

import unittest

from chmutil.core import ImageStats


class TestImageStats(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_constructor(self):
        im_stats = ImageStats(None, None, None, None)
        self.assertEqual(im_stats.get_width(), None)
        self.assertEqual(im_stats.get_height(), None)
        self.assertEqual(im_stats.get_file_path(), None)
        self.assertEqual(im_stats.get_format(), None)

        im_stats = ImageStats('fee', 'fi', 'foo', 'fum')
        self.assertEqual(im_stats.get_width(), 'fi')
        self.assertEqual(im_stats.get_height(), 'foo')
        self.assertEqual(im_stats.get_file_path(), 'fee')
        self.assertEqual(im_stats.get_format(), 'fum')


if __name__ == '__main__':
    unittest.main()
