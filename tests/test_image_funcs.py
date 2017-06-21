#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_core_funts
----------------------------------

Tests for functions in `core` module
"""

import os
import unittest
import tempfile
import shutil

from chmutil import image
from chmutil import core
from chmutil.image import InvalidImageDirError


class TestImageFunctions(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_get_image_path_no_keysort_func(self):
        temp_dir = tempfile.mkdtemp()
        try:
            try:
                image.get_image_path_list(None, None)
                self.fail('Expected InvalidImageDirError')
            except InvalidImageDirError as e:
                self.assertEqual(str(e), 'image_dir is None')

            try:
                image.get_image_path_list(os.path.join(temp_dir, 'hi'), None)
                self.fail('Expected InvalidImageDirError')
            except InvalidImageDirError as e:
                self.assertEqual(str(e), 'image_dir must be a directory')

            # no files
            res = image.get_image_path_list(temp_dir, None)
            self.assertEqual(len(res), 0)

            # one file
            onefile = os.path.join(temp_dir, 'foo.txt')
            open(onefile, 'a').close()
            res = image.get_image_path_list(temp_dir, None)
            self.assertEqual(len(res), 1)
            self.assertTrue(onefile in res)

            # two files and a directory
            twofile = os.path.join(temp_dir, 'two.png')
            open(twofile, 'a').close()

            adir = os.path.join(temp_dir, 'somedir.png')
            os.makedirs(adir, mode=0o775)
            res = image.get_image_path_list(temp_dir, None)
            self.assertEqual(len(res), 2)
            self.assertTrue(onefile in res)
            self.assertTrue(twofile in res)

            # suffix set to .png
            res = image.get_image_path_list(temp_dir, '.png')
            self.assertEqual(len(res), 1)
            self.assertTrue(twofile in res)

            # verify case DOES matter
            threefile = os.path.join(temp_dir, '.PNG')
            open(threefile, 'a').close()
            res = image.get_image_path_list(temp_dir, '.png')
            self.assertEqual(len(res), 1)
            self.assertTrue(twofile in res)

            # try a 1,000 files for fun
            for v in range(0, 999):
                af = os.path.join(temp_dir, str(v) + '.png')
                open(af, 'a').close()

            res = image.get_image_path_list(temp_dir, '.png')
            self.assertEqual(len(res), 1000)
        finally:
            shutil.rmtree(temp_dir)

    def test_get_image_path_with_longest_number_keysort(self):
        temp_dir = tempfile.mkdtemp()
        try:
            func = core.get_longest_sequence_of_numbers_in_string
            # no files
            res = image.get_image_path_list(temp_dir, None,
                                           keysortfunc=func)
            self.assertEqual(len(res), 0)

            # one file
            onefile = os.path.join(temp_dir, 'foo.2345.txt')
            open(onefile, 'a').close()
            res = image.get_image_path_list(temp_dir, None,
                                           keysortfunc=func)
            self.assertEqual(len(res), 1)
            self.assertTrue(onefile in res)

            # two files and a directory
            twofile = os.path.join(temp_dir, 'foo.1234.png')
            open(twofile, 'a').close()

            adir = os.path.join(temp_dir, 'somedir.png')
            os.makedirs(adir, mode=0o775)
            res = image.get_image_path_list(temp_dir, None,
                                           keysortfunc=func)
            self.assertEqual(len(res), 2)
            self.assertTrue(onefile in res)
            self.assertTrue(twofile in res)
            self.assertEqual(res[0], twofile)

            # suffix set to .png
            res = image.get_image_path_list(temp_dir, '.png',
                                           keysortfunc=func)
            self.assertEqual(len(res), 1)
            self.assertTrue(twofile in res)

            # verify case DOES matter
            threefile = os.path.join(temp_dir, '.PNG')
            open(threefile, 'a').close()
            res = image.get_image_path_list(temp_dir, '.png')
            self.assertEqual(len(res), 1)
            self.assertTrue(twofile in res)

            # try a 1,000 files for fun
            for v in range(0, 999):
                af = os.path.join(temp_dir, str(v) + '.png')
                open(af, 'a').close()

            res = image.get_image_path_list(temp_dir, '.png',
                                           keysortfunc=func)
            self.assertEqual(len(res), 1000)
        finally:
            shutil.rmtree(temp_dir)


if __name__ == '__main__':
    unittest.main()
