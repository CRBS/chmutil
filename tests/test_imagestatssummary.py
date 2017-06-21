#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_chmutil
----------------------------------

Tests for `chmutil` module.
"""

import unittest

from chmutil.image import ImageStats
from chmutil.image import ImageStatsSummary


class TestImageStatsSummary(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_constructor(self):
        iss = ImageStatsSummary()
        self.assertEqual(iss.get_image_count(), 0)
        self.assertEqual(iss.get_total_size_of_images_in_bytes(), 0)
        self.assertEqual(iss.get_total_pixels(), 0)
        self.assertEqual(iss.get_image_dimensions_as_dict(), {})

    def test_add_image_stats_one_image(self):
        iss = ImageStatsSummary()
        istat = ImageStats('/foo', 100, 200, 'L',
                           size_in_bytes=50)
        iss.add_image_stats(istat)
        self.assertEqual(iss.get_image_count(), 1)
        self.assertEqual(iss.get_total_size_of_images_in_bytes(), 50)
        self.assertEqual(iss.get_total_pixels(), 20000)
        self.assertEqual(iss.get_image_dimensions_as_dict(), {(100, 200): 1})

    def test_add_image_stats_two_images_same_dims(self):
        iss = ImageStatsSummary()
        istat = ImageStats('/foo', 100, 200, 'L',
                           size_in_bytes=50)
        iss.add_image_stats(istat)

        istat = ImageStats('/foo2', 100, 200, 'L',
                           size_in_bytes=60)
        iss.add_image_stats(istat)

        self.assertEqual(iss.get_image_count(), 2)
        self.assertEqual(iss.get_total_size_of_images_in_bytes(), 110)
        self.assertEqual(iss.get_total_pixels(), 40000)
        self.assertEqual(iss.get_image_dimensions_as_dict(), {(100, 200): 2})

    def test_add_image_stats_three_images_diff_dims(self):
        iss = ImageStatsSummary()
        istat = ImageStats('/foo', 100, 200, 'L',
                           size_in_bytes=50)
        iss.add_image_stats(istat)

        istat = ImageStats('/foo2', 100, 200, 'L',
                           size_in_bytes=60)
        iss.add_image_stats(istat)

        istat = ImageStats('/foo2', 300, 400, 'L',
                           size_in_bytes=100)
        iss.add_image_stats(istat)

        self.assertEqual(iss.get_image_count(), 3)
        self.assertEqual(iss.get_total_size_of_images_in_bytes(), 210)
        self.assertEqual(iss.get_total_pixels(), 160000)
        self.assertEqual(iss.get_image_dimensions_as_dict(), {(100, 200): 2,
                                                              (300, 400): 1})


if __name__ == '__main__':
    unittest.main()
