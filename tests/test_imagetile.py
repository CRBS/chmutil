#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_imagetile
----------------------------------

Tests for `ImageTile in image`
"""

import unittest
from PIL import Image
from chmutil.image import ImageTile


class TestImageTile(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_getters(self):
        im = Image.new('L', (10, 10))
        tile = ImageTile(im)
        self.assertEqual(tile.get_box(), None)
        self.assertEqual(tile.get_image(), im)
        self.assertEqual(tile.get_row(), None)
        self.assertEqual(tile.get_col(), None)

        tile = ImageTile(im, box=(4, 5, 6, 7), row=2,
                         col=3)
        self.assertEqual(tile.get_box(), (4, 5, 6, 7))
        self.assertEqual(tile.get_image(), im)
        self.assertEqual(tile.get_row(), 2)
        self.assertEqual(tile.get_col(), 3)


if __name__ == '__main__':
    unittest.main()
