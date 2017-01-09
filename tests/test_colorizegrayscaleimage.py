#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_colorizegrayscaleimage
----------------------------------

Tests for `ColorizeGrayScaleImage in image`
"""

import unittest


from PIL import Image

from chmutil.image import ColorizeGrayscaleImage
from chmutil.image import InvalidImageError


class TestColorizeGrayscaleImage(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_get_color_tuple(self):
        colorizer = ColorizeGrayscaleImage()
        self.assertEqual(colorizer.get_color_tuple(), (1, 0, 0))

        colorizer = ColorizeGrayscaleImage(color=(3, 4, 5))
        self.assertEqual(colorizer.get_color_tuple(), (3, 4, 5))

    def test_colorize_image_none_passed_in(self):
        try:
            colorizer = ColorizeGrayscaleImage()
            colorizer.colorize_image(None)
            self.fail('Expected InvalidImageError')
        except InvalidImageError as e:
            self.assertEqual(str(e), 'Image is None')

    def test_colorize_image_with_small_image(self):
        im = Image.new('L', (10, 10), color=0)
        im.putpixel((1, 1), 255)
        im.putpixel((2, 2), 50)
        im.putpixel((3, 3), 10)
        im.putpixel((4, 4), 50)
        colorizer = ColorizeGrayscaleImage()
        res = colorizer.colorize_image(im)
        self.assertEqual(res.getpixel((1, 1)), (255, 0, 0, 150))


if __name__ == '__main__':
    unittest.main()
