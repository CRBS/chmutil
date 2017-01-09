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

        tile = ImageTile(im, box=(4, 5, 6, 7))
        self.assertEqual(tile.get_box(), (4, 5, 6, 7))
        self.assertEqual(tile.get_image(), im)


if __name__ == '__main__':
    unittest.main()
