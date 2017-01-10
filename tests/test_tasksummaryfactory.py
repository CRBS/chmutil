#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_tasksummaryfactory
----------------------------------

Tests for `TaskSummaryFactory in cluster`
"""

import unittest
import configparser

from chmutil.cluster import TaskSummaryFactory
from chmutil.core import CHMConfig


class TestCore(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_get_chm_task_stats_everything_is_none(self):
        tsf = TaskSummaryFactory(None)
        ts = tsf._get_chm_task_stats()
        self.assertEqual(ts.get_completed_task_count(), 0)
        self.assertEqual(ts.get_total_task_count(), 0)

    def test_get_chm_task_stats_get_config_is_none(self):
        con = CHMConfig('./images', './model', './outdir', '500x500', '20x20')
        tsf = TaskSummaryFactory(con)
        ts = tsf._get_chm_task_stats()
        self.assertEqual(ts.get_completed_task_count(), 0)
        self.assertEqual(ts.get_total_task_count(), 0)

    def test_get_chm_task_stats(self):
        con = CHMConfig('./images', './model', './outdir', '500x500', '20x20')
        cfig = configparser.ConfigParser()
        cfig.add_section('1')
        cfig.set('1', 'hi', 'val')
        cfig.add_section('2')
        cfig.set('2', 'hi', 'val')
        cfig.add_section('3')
        cfig.set('3', 'hi', 'val')
        cfig.add_section('4')
        cfig.set('4', 'hi', 'val')

        # try with none for lists
        con.set_config(cfig)
        tsf = TaskSummaryFactory(con)
        ts = tsf._get_chm_task_stats()
        self.assertEqual(ts.get_completed_task_count(), 0)
        self.assertEqual(ts.get_total_task_count(), 4)

        # try with empty lists
        tsf = TaskSummaryFactory(con, chm_incomplete_tasks=[],
                                 merge_incomplete_tasks=[])
        ts = tsf._get_chm_task_stats()
        self.assertEqual(ts.get_completed_task_count(), 4)
        self.assertEqual(ts.get_total_task_count(), 4)

        # try with lists with elements
        tsf = TaskSummaryFactory(con, chm_incomplete_tasks=['hi'],
                                 merge_incomplete_tasks=['a', 'b'])
        ts = tsf._get_chm_task_stats()
        self.assertEqual(ts.get_completed_task_count(), 3)
        self.assertEqual(ts.get_total_task_count(), 4)

    def test_get_task_summary(self):
        con = CHMConfig('./images', './model', './outdir', '500x500', '20x20')
        cfig = configparser.ConfigParser()
        cfig.add_section('1')
        cfig.set('1', 'hi', 'val')
        cfig.add_section('2')
        cfig.set('2', 'hi', 'val')
        con.set_config(cfig)

        mfig = configparser.ConfigParser()
        mfig.add_section('3')
        mfig.set('3', 'hi', 'val')
        con.set_merge_config(mfig)

        tsf = TaskSummaryFactory(con, chm_incomplete_tasks=[],
                                 merge_incomplete_tasks=['a'])
        ts = tsf.get_task_summary()
        self.assertEqual(ts.get_summary(), 'chmutil version: unknown\n'
                                           'Tiles: 500x500 with 20x20 '
                                           'overlap\nDisable histogram '
                                           'equalization in CHM: True\n'
                                           'Tasks: 1 tiles per task, 1 '
                                           'tasks(s) per node\nTrained '
                                           'CHM model: ./model\nCHM binary: '
                                           './chm-0.1.0.img\n\nCHM tasks: '
                                           '100% complete (2 of 2 completed)'
                                           '\nMerge tasks: 0% complete (0 of '
                                           '1 completed)\n')

if __name__ == '__main__':
    unittest.main()
