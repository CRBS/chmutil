#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_mergetiles.py
----------------------------------

Tests for `mergetiles.py`
"""

import unittest
import os
import tempfile
import shutil
from PIL import Image

from chmutil import mergetiles


class TestMergeTiles(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_parse_arguments(self):
        pargs = mergetiles._parse_arguments('hi', ['imagedir', 'output'])
        self.assertEqual(pargs.imagedir, 'imagedir')
        self.assertEqual(pargs.output, 'output')
        self.assertEqual(pargs.maxpixels, 768000000)
        self.assertEqual(pargs.suffix, 'png')
        self.assertEqual(pargs.loglevel, 'WARNING')

    def test_main_invalid_input(self):
        temp_dir = tempfile.mkdtemp()
        try:
            self.assertEqual(mergetiles.main(['yo.py', temp_dir, temp_dir]), 1)
        finally:
            shutil.rmtree(temp_dir)

    def test_main_success(self):
        temp_dir = tempfile.mkdtemp()
        try:
            img_dir = os.path.join(temp_dir, 'images')
            os.makedirs(img_dir, mode=0o755)

            # create two images with a certain pixel lit up
            myimg = Image.new('L', (500, 500))
            myimg.putpixel((10, 10), 255)
            myimg.save(os.path.join(img_dir, '1.png'), 'PNG')

            myimg = Image.new('L', (500, 500))
            myimg.putpixel((20, 20), 255)
            myimg.save(os.path.join(img_dir, '2.png'), 'PNG')

            out_img = os.path.join(temp_dir, 'out.png')
            self.assertEqual(mergetiles.main(['yo.py', img_dir, out_img]), 0)
            merged_img = Image.open(out_img)
            self.assertEqual(merged_img.getpixel((10, 10)), 255)
            self.assertEqual(merged_img.getpixel((20, 20)), 255)
            self.assertEqual(merged_img.getpixel((0, 0)), 0)
            merged_img.close()
        finally:
            shutil.rmtree(temp_dir)

    def test_merge_tiles_no_images_in_dir(self):
        temp_dir = tempfile.mkdtemp()
        try:
            img_dir = os.path.join(temp_dir, 'images')
            os.makedirs(img_dir, mode=0o755)
            out_img = os.path.join(temp_dir, 'out.png')
            self.assertEqual(mergetiles._merge_image_tiles(img_dir,
                                                           out_img,
                                                           '.png'), 1)
        finally:
            shutil.rmtree(temp_dir)

    def test_merge_tiles_where_data_in_both_images_at_same_pixel(self):
        temp_dir = tempfile.mkdtemp()
        try:
            img_dir = os.path.join(temp_dir, 'images')
            os.makedirs(img_dir, mode=0o755)
            out_img = os.path.join(temp_dir, 'out.png')
            # create two images with a certain pixel lit up
            myimg = Image.new('L', (500, 500))
            myimg.putpixel((10, 10), 50)
            myimg.save(os.path.join(img_dir, '1.png'), 'PNG')

            myimg = Image.new('L', (500, 500))
            myimg.putpixel((10, 10), 100)
            myimg.save(os.path.join(img_dir, '2.png'), 'PNG')

            self.assertEqual(mergetiles._merge_image_tiles(img_dir,
                                                           out_img,
                                                           '.png'), 0)
            merged_img = Image.open(out_img)
            self.assertEqual(merged_img.getpixel((10, 10)), 100)
            merged_img.close()
        finally:
            shutil.rmtree(temp_dir)


if __name__ == '__main__':
    unittest.main()
