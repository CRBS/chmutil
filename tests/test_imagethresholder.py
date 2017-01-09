#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_imagethresholder
----------------------------------

Tests for `ImageThresholder in image`
"""

import unittest


from PIL import Image

from chmutil.image import ImageThresholder
from chmutil.image import InvalidImageError


class TestImageThresholder(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_get_pixel_intensity_cutoff(self):
        # check default
        im = ImageThresholder()
        self.assertEqual(im.get_pixel_intensity_cutoff(), 76)

        # check 0%
        im = ImageThresholder(threshold_percent=0)
        self.assertEqual(im.get_pixel_intensity_cutoff(), 0)

        # check 100%
        im = ImageThresholder(threshold_percent=100)
        self.assertEqual(im.get_pixel_intensity_cutoff(), 255)

        # check 50%
        im = ImageThresholder(threshold_percent=50)
        self.assertEqual(im.get_pixel_intensity_cutoff(), 127)

    def test_threshold_image_with_none_image(self):
        try:
            im = ImageThresholder()
            im.threshold_image(None)
            self.fail('Expected InvalidImageError')
        except InvalidImageError as e:
            self.assertEqual(str(e), 'Image is None')

    def test_threshold_image_with_valid_image(self):
        img = Image.new('L', (10, 10), color=0)
        img.putpixel((5, 5), 74)
        img.putpixel((5, 6), 75)
        img.putpixel((5, 7), 76)
        im = ImageThresholder(threshold_percent=30)
        res = im.threshold_image(img)
        img.close()
        self.assertEqual(res.getpixel((0, 0)), 0)
        self.assertEqual(res.getpixel((5, 5)), 0)
        self.assertEqual(res.getpixel((5, 6)), 0)
        self.assertEqual(res.getpixel((5, 7)), 255)
        res.close()


if __name__ == '__main__':
    unittest.main()
