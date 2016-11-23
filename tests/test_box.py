#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_box
----------------------------------

Tests for `Box in core`
"""

import unittest

from chmutil.core import Box
from chmutil.core import InvalidCommaDelimitedStringError


class TestCore(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_constructor(self):
        b = Box()
        self.assertEqual(b.get_box_as_comma_delimited_string(),
                         'None,None,None,None')

        b = Box(left=1, upper=2, right=3, lower=4)
        self.assertEqual(b.get_box_as_comma_delimited_string(),
                         '1,2,3,4')

    def test_are_any_corners_none(self):
        b = Box()
        self.assertEqual(b.are_any_corners_none(), True)

        b = Box(left=1, upper=2, right=3, lower=None)
        self.assertEqual(b.are_any_corners_none(), True)

        b = Box(left=1, upper=2, right=None, lower=4)
        self.assertEqual(b.are_any_corners_none(), True)

        b = Box(left=1, upper=None, right=3, lower=4)
        self.assertEqual(b.are_any_corners_none(), True)

        b = Box(left=None, upper=2, right=3, lower=4)
        self.assertEqual(b.are_any_corners_none(), True)

        b = Box(left=1, upper=2, right=3, lower=4)
        self.assertEqual(b.are_any_corners_none(), False)

    def test_load_from_comma_delimited_string(self):
        b = Box(left=2)
        b.load_from_comma_delimited_string('1,2,3,4')
        self.assertEqual(b.get_box_as_comma_delimited_string(),
                         '1,2,3,4')

        try:
            b.load_from_comma_delimited_string(None)
            self.fail('Expected InvalidCommaDelimitedStringError')
        except InvalidCommaDelimitedStringError as e:
            self.assertEqual(str(e), 'string is None')

        try:
            b.load_from_comma_delimited_string('')
            self.fail('Expected InvalidCommaDelimitedStringError')
        except InvalidCommaDelimitedStringError as e:
            self.assertEqual(str(e), 'string does not have 4 '
                                     'elements when parsed: ')

        try:
            b.load_from_comma_delimited_string('1,2,b,4')
            self.fail('Expected ValueError')
        except ValueError:
            pass

    def test_get_list_of_tuple_corner_coordinates(self):
        b = Box()
        self.assertEqual(b.get_list_of_tuple_of_corner_coordinates(), None)
        b.load_from_comma_delimited_string('10,50,20,100')
        res = b.get_list_of_tuple_of_corner_coordinates()
        self.assertEqual(res, [(10, 50), (10, 100), (20, 50), (20, 100)])

    def test_get_box_as_tuple(self):
        b = Box()
        self.assertEqual(b.get_box_as_tuple(), (None, None, None, None))

        b = Box(left=1, upper=2, right=3, lower=4)
        self.assertEqual(b.get_box_as_tuple(), (1, 2, 3, 4))

    def test_is_coordinate_in_box(self):
        b = Box()
        # test None passed in
        self.assertEqual(b.is_coordinate_in_box(None), False)

        # test where box has None values
        self.assertEqual(b.is_coordinate_in_box((123, 2345)), False)

        # test where coordinate is in box
        b = Box(left=10, upper=50, right=20, lower=100)
        self.assertEqual(b.is_coordinate_in_box((10, 50)), True)
        self.assertEqual(b.is_coordinate_in_box((20, 50)), True)
        self.assertEqual(b.is_coordinate_in_box((10, 100)), True)
        self.assertEqual(b.is_coordinate_in_box((20, 100)), True)
        self.assertEqual(b.is_coordinate_in_box((15, 75)), True)

        # test where coordinate is NOT in box
        self.assertEqual(b.is_coordinate_in_box((9, 50)), False)
        self.assertEqual(b.is_coordinate_in_box((20, 49)), False)
        self.assertEqual(b.is_coordinate_in_box((10, 101)), False)
        self.assertEqual(b.is_coordinate_in_box((21, 100)), False)
        self.assertEqual(b.is_coordinate_in_box((8, 40)), False)

    def test_does_box_intersect(self):
        a = Box()
        self.assertEqual(a.does_box_intersect(a), False)

        # test where one box is None
        b = Box(left=10, upper=50, right=20, lower=100)
        self.assertEqual(a.does_box_intersect(b), False)
        self.assertEqual(b.does_box_intersect(a), False)

        # test with same box
        self.assertEqual(b.does_box_intersect(b), True)

        # test with box inside other box
        a = Box(left=12, upper=60, right=18, lower=75)
        self.assertEqual(a.does_box_intersect(b), True)
        self.assertEqual(b.does_box_intersect(a), True)

        # test with box just overlapping
        a = Box(left=12, upper=40, right=18, lower=50)
        self.assertEqual(a.does_box_intersect(b), True)
        self.assertEqual(b.does_box_intersect(a), True)
        a = Box(left=18, upper=55, right=500, lower=60)
        self.assertEqual(a.does_box_intersect(b), True)
        self.assertEqual(b.does_box_intersect(a), True)

        # test with box not overlapping
        a = Box(left=12, upper=40, right=18, lower=45)
        self.assertEqual(a.does_box_intersect(b), False)
        self.assertEqual(b.does_box_intersect(a), False)


if __name__ == '__main__':
    unittest.main()
