#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_createprobmapoverlay.py
----------------------------------

Tests for `createprobmapoverlay.py`
"""

import unittest
import os
import tempfile
import shutil
from PIL import Image

from chmutil import createprobmapoverlay
from chmutil.createprobmapoverlay import NoInputImageFoundError


class TestCreateProbmapOverlay(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_parse_arguments(self):
        pargs = createprobmapoverlay._parse_arguments('hi', ['image',
                                                             'prob',
                                                             'out'])
        self.assertEqual(pargs.image, 'image')
        self.assertEqual(pargs.probmap, 'prob')
        self.assertEqual(pargs.output, 'out')
        self.assertEqual(pargs.overlaycolor, 'blue')
        self.assertEqual(pargs.threshpc, 30)
        self.assertEqual(pargs.opacity, 70)

    def test_get_pixel_coloring_tuple(self):
        res = createprobmapoverlay._get_pixel_coloring_tuple('red')
        self.assertEqual(res, (1, 0, 0))

        res = createprobmapoverlay._get_pixel_coloring_tuple('green')
        self.assertEqual(res, (0, 1, 0))

        res = createprobmapoverlay._get_pixel_coloring_tuple('yellow')
        self.assertEqual(res, (1, 1, 0))

        res = createprobmapoverlay._get_pixel_coloring_tuple('cyan')
        self.assertEqual(res, (0, 1, 1))

        res = createprobmapoverlay._get_pixel_coloring_tuple('magenta')
        self.assertEqual(res, (1, 0, 1))

        res = createprobmapoverlay._get_pixel_coloring_tuple('purple')
        self.assertEqual(res, (0.5, 0, 0.5))

        res = createprobmapoverlay._get_pixel_coloring_tuple('blue')
        self.assertEqual(res, (0, 0, 1))

        res = createprobmapoverlay._get_pixel_coloring_tuple('invalid')
        self.assertEqual(res, (0, 0, 1))

    def test_get_thresholded_probmap(self):
        temp_dir = tempfile.mkdtemp()
        try:
            im = Image.new('L', (10, 10))
            im.putpixel((5, 5), 100)
            probmap = os.path.join(temp_dir, 'probmap.png')
            im.save(probmap, 'PNG')
            im.close()
            pmap = createprobmapoverlay._get_thresholded_probmap(probmap, 30)
            self.assertEqual(pmap.getpixel((5, 5)), 255)
            self.assertEqual(pmap.getpixel((5, 4)), 0)
            pmap.close()

        finally:
            shutil.rmtree(temp_dir)

    def test_convert_image_no_image_file(self):
        temp_dir = tempfile.mkdtemp()
        try:
            img_file = os.path.join(temp_dir, 'doesnotexist.png')
            createprobmapoverlay._convert_image(img_file, 'foo', temp_dir,
                                                None)
            self.fail('Expected NoInputImageFoundError')
        except NoInputImageFoundError as e:
            self.assertEqual(str(e), 'Image ' + img_file + ' not found')
        finally:
            shutil.rmtree(temp_dir)

    def test_convert_image_no_probmap_file(self):
        temp_dir = tempfile.mkdtemp()
        try:
            img_file = os.path.join(temp_dir, 'image.png')
            im = Image.new('L', (10, 10))
            im.save(img_file, 'PNG')
            im.close()

            prob_file = os.path.join(temp_dir, 'doesnotexist.png')
            createprobmapoverlay._convert_image(img_file, prob_file,
                                                temp_dir, None)
            self.fail('Expected NoInputImageFoundError')
        except NoInputImageFoundError as e:
            self.assertEqual(str(e), 'Image ' + prob_file + ' not found')
        finally:
            shutil.rmtree(temp_dir)

    def test_main_success(self):
        temp_dir = tempfile.mkdtemp()
        try:
            img_file = os.path.join(temp_dir, 'image.png')
            im = Image.new('L', (10, 10))
            im.save(img_file, 'PNG')

            prob_file = os.path.join(temp_dir, 'probmap.png')
            im.putpixel((5, 5), 100)
            im.save(prob_file, 'PNG')
            im.close()
            out_file = os.path.join(temp_dir, 'out')

            res = createprobmapoverlay.main(['hi.py', img_file, prob_file,
                                             out_file])
            self.assertEqual(res, 0)
            self.assertTrue(os.path.isfile(out_file + '.png'))
            # TODO actually check it did the conversion properly
        finally:
            shutil.rmtree(temp_dir)


if __name__ == '__main__':
    unittest.main()
