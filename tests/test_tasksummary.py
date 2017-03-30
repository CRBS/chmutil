#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_tasksummary
----------------------------------

Tests for `TaskSummary in cluster`
"""

import unittest
import sys

from chmutil.cluster import TaskStats
from chmutil.cluster import TaskSummary
from chmutil.core import CHMConfig


class TestCore(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_get_summary_from_task_stats(self):
        tsum = TaskSummary(None)

        self.assertEqual(tsum._get_summary_from_task_stats(None), 'NA')
        ts = TaskStats()
        ts.set_total_task_count(0)
        self.assertEqual(tsum._get_summary_from_task_stats(ts),
                         'Total number of tasks is <= 0')

        ts.set_completed_task_count(0)
        ts.set_total_task_count(2)
        self.assertEqual(tsum._get_summary_from_task_stats(ts),
                         '0% complete (0 of 2 completed)')

        ts.set_completed_task_count(1)
        self.assertEqual(tsum._get_summary_from_task_stats(ts),
                         '50% complete (1 of 2 completed)')

        ts.set_completed_task_count(2)
        self.assertEqual(tsum._get_summary_from_task_stats(ts),
                         '100% complete (2 of 2 completed)')

        ts.set_completed_task_count(3490)
        ts.set_total_task_count(10000)
        if sys.version_info[0] == 2 and sys.version_info[1] <= 6:
            self.assertEqual(tsum._get_summary_from_task_stats(ts),
                             '35% complete (3490 of 10000 completed)')
        else:
            self.assertEqual(tsum._get_summary_from_task_stats(ts),
                             '35% complete (3,490 of 10,000 completed)')

    def test__get_compute_summary_from_task_stats(self):

        # test passing None
        tsum = TaskSummary(None)
        self.assertEqual(tsum._get_compute_summary_from_task_stats(None,
                                                                   None),
                         '')

        # set prefix
        self.assertEqual(tsum._get_compute_summary_from_task_stats('foo',
                                                                   None),
                         '')

        # try empty stats
        ts = TaskStats()
        self.assertEqual(tsum._get_compute_summary_from_task_stats('foo', ts),
                         '')

        # try stats with 1 cpu task but zero for everything
        ts.set_total_tasks_with_cputimes(1)
        res = tsum._get_compute_summary_from_task_stats('foo', ts)
        self.assertTrue('\nfoo runtime: 0.0 hours per '
                        'task (0.0GB ram)\n' in res)
        self.assertTrue('foo CPU consumption so far: '
                        '0.0 CPU hours (~0.0 years)\n' in res)
        self.assertTrue('\nfoo estimated remaining compute: '
                        'NA CPU hours (~NA years)\n' in res)

        # try stats with 1 task with information
        ts.set_total_cpu_usertime(10800)
        ts.set_total_cpu_walltime(3600)
        ts.set_total_memory_in_kb(1000)
        ts.set_total_task_count(3)
        ts.set_completed_task_count(1)
        res = tsum._get_compute_summary_from_task_stats('foo', ts)
        self.assertTrue('\nfoo runtime: 1.0 hours per task '
                        '(1.0GB ram)\n' in res)
        self.assertTrue('foo CPU consumption so far: '
                        '3.0 CPU hours (~0.0 years)\n' in res)
        self.assertTrue('\nfoo estimated remaining compute: '
                        '6.0 CPU hours (~0.0 years)\n' in res)

        # try stats with 2 task with information
        ts.set_total_tasks_with_cputimes(2)
        ts.set_completed_task_count(2)
        res = tsum._get_compute_summary_from_task_stats('foo', ts)
        self.assertTrue('\nfoo runtime: 0.5 hours per task '
                        '(0.5GB ram)\n' in res)
        self.assertTrue('foo CPU consumption so far: '
                        '3.0 CPU hours (~0.0 years)\n' in res)
        self.assertTrue('\nfoo estimated remaining compute: '
                        '1.5 CPU hours (~0.0 years)\n' in res)

        # try stats with 10,000 tasks and years of compute
        ts.set_completed_task_count(10000)
        ts.set_total_tasks_with_cputimes(10000)
        ts.set_total_task_count(20000)
        ts.set_total_cpu_usertime(28382400)
        ts.set_total_cpu_walltime(9460800)
        ts.set_total_memory_in_kb(50000000)
        res = tsum._get_compute_summary_from_task_stats('foo', ts)
        self.assertTrue('\nfoo runtime: 0.3 hours per task '
                        '(5.0GB ram)\n' in res)
        self.assertTrue('foo CPU consumption so far: '
                        '7,884.0 CPU hours (~0.9 years)\n' in res)
        self.assertTrue('\nfoo estimated remaining compute: '
                        '7,883.3 CPU hours (~0.9 years)\n' in res)

    def test_get_summary(self):
        tsum = TaskSummary(None)
        self.assertEqual(tsum.get_summary(),
                         'CHM tasks: NA\nMerge tasks: NA\n')
        ts = TaskStats()
        ts.set_completed_task_count(1)
        ts.set_total_task_count(2)
        tsum = TaskSummary(None, chm_task_stats=ts)
        rts = tsum.get_chm_task_stats()
        self.assertEqual(rts.get_completed_task_count(), 1)
        self.assertEqual(tsum.get_summary(),
                         'CHM tasks: 50% complete (1 of 2 completed)\n'
                         'Merge tasks: NA\n')

        ts = TaskStats()
        ts.set_completed_task_count(1)
        ts.set_total_task_count(2)
        tsum = TaskSummary(None, merge_task_stats=ts)
        rts = tsum.get_merge_task_stats()
        self.assertEqual(rts.get_total_task_count(), 2)
        self.assertEqual(tsum.get_summary(),
                         'CHM tasks: NA\n'
                         'Merge tasks: 50% complete (1 of 2 completed)\n')

        mts = TaskStats()
        mts.set_completed_task_count(3)
        mts.set_total_task_count(4)

        tsum = TaskSummary(None, chm_task_stats=ts,
                           merge_task_stats=mts)
        self.assertEqual(tsum.get_summary(),
                         'CHM tasks: 50% complete (1 of 2 completed)\n'
                         'Merge tasks: 75% complete (3 of 4 completed)\n')

        # test with chmconfig
        con = CHMConfig('./images', './model', './outdir', '500x500', '20x20')
        tsum = TaskSummary(con)
        self.assertEqual(tsum.get_summary(), 'chmutil version: unknown\n'
                                             'Tiles: 500x500 with 20x20 '
                                             'overlap\nDisable histogram '
                                             'equalization in CHM: True\n'
                                             'Tasks: 1 tiles per task, 1 '
                                             'tasks(s) per node\nTrained '
                                             'CHM model: ./model\nCHM binary: '
                                             './chm-0.1.0.img\n\nCHM tasks: '
                                             'NA\nMerge tasks: NA\n')

        tsum = TaskSummary(con, chm_task_stats=ts,
                           merge_task_stats=mts)
        self.assertEqual(tsum.get_summary(), 'chmutil version: unknown\n'
                                             'Tiles: 500x500 with 20x20 '
                                             'overlap\nDisable histogram '
                                             'equalization in CHM: True\n'
                                             'Tasks: 1 tiles per task, 1 '
                                             'tasks(s) per node\nTrained '
                                             'CHM model: ./model\nCHM binary: '
                                             './chm-0.1.0.img\n\nCHM tasks: '
                                             '50% complete (1 of 2 completed)'
                                             '\nMerge tasks: 75% complete '
                                             '(3 of 4 completed)\n')


if __name__ == '__main__':
    unittest.main()
