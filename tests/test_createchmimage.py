#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_createchmimage.py
----------------------------------

Tests for `createchmimage.py`
"""

import unittest
import os
import tempfile
import shutil
from PIL import Image

from chmutil import createchmimage
from chmutil.createchmimage import NoInputImageFoundError


class TestCreateCHMImage(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_parse_arguments(self):
        pargs = createchmimage._parse_arguments('hi', ['foo.png', 'out.png'])
        self.assertEqual(pargs.image, 'foo.png')
        self.assertEqual(pargs.output, 'out.png')
        self.assertEqual(pargs.downsample, 0)
        self.assertEqual(pargs.loglevel, 'WARNING')

    def test_main(self):
        temp_dir = tempfile.mkdtemp()
        try:
            # test with non existant image
            nonexistant_img = os.path.join(temp_dir, 'doesnotexist.png')
            output = os.path.join(temp_dir, 'output.png')
            createchmimage.main(['createchmjob.py', nonexistant_img,
                                 output])
            self.fail('expected LoadConfigError')
        except NoInputImageFoundError as e:
            self.assertEqual(str(e), 'Image ' + nonexistant_img + ' not found')

        finally:
            shutil.rmtree(temp_dir)

    def test_convert_image_on_valid_image(self):
        temp_dir = tempfile.mkdtemp()
        try:
            input = os.path.join(temp_dir, 'input.png')
            output = os.path.join(temp_dir, 'output')
            size = 800, 800
            myimg = Image.new('L', size)
            myimg.save(input, 'PNG')

            pargs = createchmimage._parse_arguments('hi', [input, output,
                                                           '--downsample',
                                                           '2',
                                                           '--equalize',
                                                           '--autocontrast',
                                                           '--gaussianblur'])
            val = createchmimage._convert_image(pargs.image,
                                                pargs.output, pargs)
            self.assertEqual(val, 0)

            myimg = Image.open(output + '.png', mode='r')
            self.assertEqual(myimg.size, (400, 400))
            myimg.close()
        finally:
            shutil.rmtree(temp_dir)

    def test_convert_image_on_valid_image_with_tiles(self):
        temp_dir = tempfile.mkdtemp()
        try:
            input = os.path.join(temp_dir, 'input.png')
            output = os.path.join(temp_dir, 'output')

            size = 400, 400
            myimg = Image.new('L', size)
            myimg.save(input, 'PNG')

            pargs = createchmimage._parse_arguments('hi', [input, output,
                                                           '--downsample',
                                                           '2',
                                                           '--equalize',
                                                           '--autocontrast',
                                                           '--gaussianblur',
                                                           '--gentiles',
                                                           '--tilesize',
                                                           '100'])
            val = createchmimage._convert_image(pargs.image,
                                                pargs.output, pargs)
            self.assertEqual(val, 0)

            self.assertTrue(os.path.isdir(output))

            img1 = Image.open(os.path.join(output, '0-r0_c0.png'))
            self.assertEqual(img1.size, (100, 100))
            img1.close()

            img2 = Image.open(os.path.join(output, '0-r0_c1.png'))
            self.assertEqual(img2.size, (100, 100))
            img2.close()

            img3 = Image.open(os.path.join(output, '0-r1_c0.png'))
            self.assertEqual(img3.size, (100, 100))
            img3.close()

            img4 = Image.open(os.path.join(output, '0-r1_c1.png'))
            self.assertEqual(img4.size, (100, 100))
            img4.close()

        finally:
            shutil.rmtree(temp_dir)


if __name__ == '__main__':
    unittest.main()
