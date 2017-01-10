#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_singlecolumnimagetilegenerator
----------------------------------

Tests for `SingleColumnImageTileGenerator in image`
"""

import unittest
from PIL import Image
from chmutil.image import SingleColumnImageTileGenerator
from chmutil.image import InvalidImageError


class TestSingleColumnImageTileGenerator(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_image_is_none(self):
        gen = SingleColumnImageTileGenerator(tileheight=300)
        try:
            for res in gen.get_image_tiles(None):
                self.fail('expected exception')
        except InvalidImageError as e:
            self.assertEqual(str(e), 'Image is None')

    def test_image_smaller_then_tile(self):
        im = Image.new('L', (500, 200))
        gen = SingleColumnImageTileGenerator(tileheight=300)
        counter = 0
        for res in gen.get_image_tiles(im):
            counter += 1
            self.assertEqual(res.get_image().size, (500, 200))
            self.assertEqual(res.get_box(), (0, 0, 500, 200))

        self.assertEqual(counter, 1)

    def test_image_height_of_tile(self):
        im = Image.new('L', (500, 200))
        gen = SingleColumnImageTileGenerator(tileheight=200)
        counter = 0
        for res in gen.get_image_tiles(im):
            counter += 1
            self.assertEqual(res.get_image().size, (500, 200))
            self.assertEqual(res.get_box(), (0, 0, 500, 200))

        self.assertEqual(counter, 1)

    def test_two_tiles_returned_perfectly_divisible(self):
        im = Image.new('L', (500, 200))
        gen = SingleColumnImageTileGenerator(tileheight=100)
        counter = 0
        im_list = []
        for res in gen.get_image_tiles(im):
            counter += 1
            im_list.append(res)

        self.assertEqual(im_list[0].get_image().size, (500, 100))
        self.assertEqual(im_list[0].get_box(), (0, 0, 500, 100))

        self.assertEqual(im_list[1].get_image().size, (500, 100))
        self.assertEqual(im_list[1].get_box(), (0, 100, 500, 200))

        self.assertEqual(counter, 2)

    def test_two_tiles_returned_last_tile_shorter(self):
        im = Image.new('L', (500, 190))
        gen = SingleColumnImageTileGenerator(tileheight=100)
        counter = 0
        im_list = []
        for res in gen.get_image_tiles(im):
            counter += 1
            im_list.append(res)

        self.assertEqual(im_list[0].get_image().size, (500, 100))
        self.assertEqual(im_list[0].get_box(), (0, 0, 500, 100))

        self.assertEqual(im_list[1].get_image().size, (500, 90))
        self.assertEqual(im_list[1].get_box(), (0, 100, 500, 190))

        self.assertEqual(counter, 2)


if __name__ == '__main__':
    unittest.main()
