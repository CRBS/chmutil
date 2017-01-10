#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_simpleimagemerger
----------------------------------

Tests for `SimpleImageMerger in image`
"""

import unittest
import os
import tempfile
import shutil

from PIL import Image
from chmutil.image import SimpleImageMerger


class TestSimpleImageMerger(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_merge_two_images(self):
        temp_dir = tempfile.mkdtemp()
        try:
            im1 = Image.new('L', (10, 10))
            im1.putpixel((0, 0), 20)

            im2 = Image.new('L', (10, 10))
            im2.putpixel((0, 0), 19)
            im2.putpixel((0, 1), 10)
            img_path = os.path.join(temp_dir, 'img2.png')
            im2.save(img_path, 'PNG')
            im2.close()
            sim = SimpleImageMerger()
            res = sim._merge_two_images(im1, img_path)

            self.assertEqual(res.size, (10, 10))
            self.assertEqual(res.getpixel((0, 0)), 20)
            self.assertEqual(res.getpixel((0, 1)), 10)
            self.assertEqual(res.getpixel((0, 3)), 0)

        finally:
            shutil.rmtree(temp_dir)

    def test_merge_images_with_no_images(self):
        sim = SimpleImageMerger()
        self.assertEqual(sim.merge_images(None), None)
        self.assertEqual(sim.merge_images([]), None)

    def test_merge_images_with_one_image(self):
        temp_dir = tempfile.mkdtemp()
        try:

            im_list = []

            subim = Image.new('L', (301, 10))
            subim.putpixel((1, 0), 1)
            img_path = os.path.join(temp_dir, '1.png')
            im_list.append(img_path)
            subim.save(img_path, 'PNG')
            subim.close()

            sim = SimpleImageMerger()
            res = sim.merge_images(im_list)

            self.assertEqual(res.size, (301, 10))
            self.assertEqual(res.getpixel((1, 0)), 1)
            self.assertEqual(res.getpixel((1, 1)), 0)

        finally:
            shutil.rmtree(temp_dir)

    def test_merge_images_with_lots_of_images(self):
        temp_dir = tempfile.mkdtemp()
        try:

            im_list = []
            for x in range(0, 300):
                subim = Image.new('L', (301, 10))
                subim.putpixel((x, 0), x)
                img_path = os.path.join(temp_dir, str(x) + '.png')
                im_list.append(img_path)
                subim.save(img_path, 'PNG')
                subim.close()

            sim = SimpleImageMerger()
            res = sim.merge_images(im_list)

            self.assertEqual(res.size, (301, 10))
            self.assertEqual(res.getpixel((0, 0)), 0)
            self.assertEqual(res.getpixel((1, 0)), 1)
            self.assertEqual(res.getpixel((2, 0)), 2)
            self.assertEqual(res.getpixel((3, 0)), 3)
            self.assertEqual(res.getpixel((4, 0)), 4)

        finally:
            shutil.rmtree(temp_dir)


if __name__ == '__main__':
    unittest.main()
