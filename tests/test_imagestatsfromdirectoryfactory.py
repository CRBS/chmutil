#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_chmutil
----------------------------------

Tests for `chmutil` module.
"""

import tempfile
import shutil
import os
import unittest

from PIL import Image
from chmutil.core import ImageStatsFromDirectoryFactory


class TestImageStatsFromDirectoryFactory(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_constructor_with_max_image_pixels(self):
        temp_dir = tempfile.mkdtemp()
        try:
            ImageStatsFromDirectoryFactory(temp_dir)
            self.assertEqual(Image.MAX_IMAGE_PIXELS, 768000000)
            ImageStatsFromDirectoryFactory(temp_dir, max_image_pixels=10)
            self.assertEqual(Image.MAX_IMAGE_PIXELS, 10)
        finally:
            shutil.rmtree(temp_dir)

    def test_empty_dir(self):
        temp_dir = tempfile.mkdtemp()
        try:
            fac = ImageStatsFromDirectoryFactory(temp_dir)
            res = fac.get_input_image_stats()
            self.assertEqual(len(res), 0)
        finally:
            shutil.rmtree(temp_dir)

    def test_path_is_not_dir(self):
        temp_dir = tempfile.mkdtemp()
        try:
            afile = os.path.join(temp_dir, 'foo')
            open(afile, 'a').close()
            fac = ImageStatsFromDirectoryFactory(afile)
            res = fac.get_input_image_stats()
            self.assertEqual(len(res), 0)
        finally:
            shutil.rmtree(temp_dir)

    def test_dir_non_image_file(self):
        temp_dir = tempfile.mkdtemp()
        try:
            afile = os.path.join(temp_dir, 'foo')
            open(afile, 'a').close()
            fac = ImageStatsFromDirectoryFactory(temp_dir)
            res = fac.get_input_image_stats()
            self.assertEqual(len(res), 0)
        finally:
            shutil.rmtree(temp_dir)

    def test_one_image(self):
        temp_dir = tempfile.mkdtemp()
        try:
            pngfile = os.path.join(temp_dir, 'foo.png')
            size = 128, 256
            myimg = Image.new('L', size)
            myimg.save(pngfile, 'PNG')

            fac = ImageStatsFromDirectoryFactory(temp_dir)
            res = fac.get_input_image_stats()
            self.assertEqual(len(res), 1)
            self.assertEqual(res[0].get_width(), 128)
            self.assertEqual(res[0].get_height(), 256)
            self.assertEqual(res[0].get_file_path(), pngfile)
            self.assertEqual(res[0].get_format(), 'PNG')
        finally:
            shutil.rmtree(temp_dir)

    def test_hundred_images(self):
        temp_dir = tempfile.mkdtemp()
        try:
            for x in range(1, 101):
                pngfile = os.path.join(temp_dir, str(x) + '.foo.png')
                size = 128, 256
                myimg = Image.new('L', size)
                myimg.save(pngfile, 'PNG')

            fac = ImageStatsFromDirectoryFactory(temp_dir)
            res = fac.get_input_image_stats()
            self.assertEqual(len(res), 100)
        finally:
            shutil.rmtree(temp_dir)


if __name__ == '__main__':
    unittest.main()
