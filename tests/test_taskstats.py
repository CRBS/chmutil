#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_taskstats
----------------------------------

Tests for `TaskStats in cluster`
"""

import unittest

from chmutil.cluster import TaskStats


class TestCore(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_getter_and_setters(self):
        ts = TaskStats()
        self.assertEqual(ts.get_completed_task_count(), 0)
        self.assertEqual(ts.get_total_task_count(), 0)
        ts.set_completed_task_count(1)
        ts.set_total_task_count(5)
        self.assertEqual(ts.get_completed_task_count(), 1)
        self.assertEqual(ts.get_total_task_count(), 5)


if __name__ == '__main__':
    unittest.main()
