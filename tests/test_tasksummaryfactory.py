#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_tasksummaryfactory
----------------------------------

Tests for `TaskSummaryFactory in cluster`
"""

import unittest
import configparser
import tempfile
import os
import shutil
from PIL import Image

from chmutil.cluster import TaskSummaryFactory
from chmutil.core import CHMConfig
from chmutil.core import CHMJobCreator
from chmutil.cluster import TaskStats


class TestTaskSummaryFactory(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_get_files_in_directory_generator_none_passed_in(self):
        tsf = TaskSummaryFactory(None)
        count = 0
        for item in tsf._get_files_in_directory_generator(None):
            count += 1
        self.assertEqual(count, 0)

    def test_get_files_in_directory_generator_on_empty_dir(self):
        temp_dir = tempfile.mkdtemp()
        try:
            tsf = TaskSummaryFactory(None)
            count = 0
            for item in tsf._get_files_in_directory_generator(temp_dir):
                count += 1
            self.assertEqual(count, 0)
        finally:
            shutil.rmtree(temp_dir)

    def test_get_files_in_directory_generator_on_one_file(self):
        temp_dir = tempfile.mkdtemp()
        try:
            tsf = TaskSummaryFactory(None)
            count = 0
            onefile = os.path.join(temp_dir, 'foo.txt')
            open(onefile, 'a').close()

            for item in tsf._get_files_in_directory_generator(onefile):
                self.assertEqual(item, onefile)
                count += 1

            self.assertEqual(count, 1)
        finally:
            shutil.rmtree(temp_dir)

    def test_get_files_in_directory_generator_on_dir_with_file(self):
        temp_dir = tempfile.mkdtemp()
        try:
            tsf = TaskSummaryFactory(None)
            count = 0
            onefile = os.path.join(temp_dir, 'foo.txt')
            open(onefile, 'a').close()

            for item in tsf._get_files_in_directory_generator(temp_dir):
                self.assertEqual(item, onefile)
                count += 1

            self.assertEqual(count, 1)
        finally:
            shutil.rmtree(temp_dir)

    def test_get_files_in_directory_generator_on_multiple_files(self):
        temp_dir = tempfile.mkdtemp()
        try:
            tsf = TaskSummaryFactory(None)
            one = os.path.join(temp_dir, '1.txt')
            two = os.path.join(temp_dir, '2.txt')
            three = os.path.join(temp_dir, '3.txt')
            open(one, 'a').close()
            open(two, 'a').close()
            open(three, 'a').close()
            thefiles = []
            for item in tsf._get_files_in_directory_generator(temp_dir):
                thefiles.append(item)
            self.assertTrue(one in thefiles)
            self.assertTrue(two in thefiles)
            self.assertTrue(three in thefiles)
            self.assertEqual(len(thefiles), 3)
        finally:
            shutil.rmtree(temp_dir)

    def test_get_files_in_directory_generator_on_multiple_directories(self):
        temp_dir = tempfile.mkdtemp()
        try:
            tsf = TaskSummaryFactory(None)
            one = os.path.join(temp_dir, '1.txt')
            two = os.path.join(temp_dir, '2.txt')
            subdir = os.path.join(temp_dir, 'foodir')
            os.makedirs(subdir, mode=0o0775)
            three = os.path.join(subdir, '3.txt')
            four = os.path.join(subdir, '4.txt')
            open(one, 'a').close()
            open(two, 'a').close()
            open(three, 'a').close()
            open(four, 'a').close()
            thefiles = []
            for item in tsf._get_files_in_directory_generator(temp_dir):
                thefiles.append(item)
            self.assertTrue(one in thefiles)
            self.assertTrue(two in thefiles)
            self.assertTrue(three in thefiles)
            self.assertTrue(four in thefiles)
            self.assertEqual(len(thefiles), 4)
        finally:
            shutil.rmtree(temp_dir)

    def test_chm_compute_hours_consumed_empty_and_with_files(self):
        temp_dir = tempfile.mkdtemp()
        try:
            con = CHMConfig('./images', './model', './outdir', '500x500',
                            '20x20')

            # test empty directory
            tsf = TaskSummaryFactory(con)
            res = tsf._get_compute_hours_consumed(temp_dir)
            self.assertEqual(len(res), 0)

            # test one file valid format old way ie real, user, sys
            oldformatfile = os.path.join(temp_dir, '1234.1')
            f = open(oldformatfile, 'w')
            f.write('HOST: comet-22-63\nDATE: blah\ns\n\n')
            f.write('real 150.05\nuser 250.10\nsys 66.66\n')
            f.write('chmrunner.py exited with code: 0\n')
            f.flush()
            f.close()
            res = tsf._get_compute_hours_consumed(temp_dir)
            self.assertEqual(len(res), 1)
            self.assertEqual(res, [(250.1, 150.05, 0)])

            # test 2 files one with no content
            open(os.path.join(temp_dir, '234.22'), 'a').close()
            res = tsf._get_compute_hours_consumed(temp_dir)
            self.assertEqual(len(res), 1)
            self.assertEqual(res, [(250.1, 150.05, 0)])

            # test 3 files one with content but not run stats
            no_time_file = os.path.join(temp_dir, 'hello.txt')
            f = open(no_time_file, 'w')
            f.write('h\nasdfasd\n how\n are\n you\n')
            f.flush()
            f.close()
            res = tsf._get_compute_hours_consumed(temp_dir)
            self.assertEqual(len(res), 1)
            self.assertEqual(res, [(250.1, 150.05, 0)])

            # test 5 files 3 have content, one old format, two new format
            new_format_file = os.path.join(temp_dir, '778786.1')
            f = open(new_format_file, 'w')
            f.write('a\n\b\nc\n')
            f.write('User time (seconds): 9823.73\n')
            f.write('System time (seconds): 9281.73\n')
            f.write('Percent of CPU this job got: 1591%\n')
            f.write('Elapsed (wall clock) time (h:mm:ss or m:ss): 3:23:15\n')
            f.write('Average shared text size (kbytes): 0\n')
            f.write('Average unshared data size (kbytes): 0\n')
            f.write('Average stack size (kbytes): 0\n')
            f.flush()
            f.close()

            new_format_file2 = os.path.join(temp_dir, '778786.2')
            f = open(new_format_file2, 'w')
            f.write('a\n\b\nc\n')
            f.write('        User time (seconds): 100.00\n')
            f.write('  System time (seconds): 200.00\n')
            f.write('  Percent of CPU this job got: 1591%\n')
            f.write(' Elapsed (wall clock) time (h:mm:ss or m:ss): 20:15\n')
            f.write('Average shared text size (kbytes): 0\n')
            f.write('Average unshared data size (kbytes): 0\n')
            f.write('Average stack size (kbytes): 0\n')
            f.write('        Maximum resident set size (kbytes): 5287148\n')
            f.flush()
            f.close()

            res = tsf._get_compute_hours_consumed(temp_dir)
            self.assertEqual(len(res), 3)
            res.sort()
            self.assertEqual(res, [(100.0, 1215.0, 5287148),
                                   (250.1, 150.05, 0),
                                   (9823.73, 12195.0, 0)])
        finally:
            shutil.rmtree(temp_dir)

    def test_update_chm_task_stats_with_compute(self):
        # pass a None list and taskstats
        tsf = TaskSummaryFactory(None, output_compute=True)
        res = tsf._update_chm_task_stats_with_compute(None, None)
        self.assertEqual(res, None)

        # None for res list
        ts = TaskStats()
        res = tsf._update_chm_task_stats_with_compute(ts, None)
        self.assertEqual(res, ts)

        # None for taskstats
        res = tsf._update_chm_task_stats_with_compute(None, [])
        self.assertEqual(res, None)

        # empty res list
        ts = TaskStats()
        res = tsf._update_chm_task_stats_with_compute(ts, [])
        self.assertEqual(res, ts)

        # try 1 stats
        ts = TaskStats()
        res = tsf._update_chm_task_stats_with_compute(ts, [(1, 2, 3)])
        self.assertEqual(res, ts)
        self.assertEqual(res.get_total_tasks_with_cputimes(), 1)
        self.assertEqual(res.get_max_memory_in_kb(), 3)
        self.assertEqual(res.get_total_cpu_usertime(), 1)
        self.assertEqual(res.get_total_cpu_walltime(), 2)

        # try 3 entries
        ts = TaskStats()
        res = tsf._update_chm_task_stats_with_compute(ts, [(5, 5, 10),
                                                           (1, 2, 3),
                                                           (4, 5, 1)])
        self.assertEqual(res, ts)
        self.assertEqual(res.get_total_tasks_with_cputimes(), 3)
        self.assertEqual(res.get_max_memory_in_kb(), 10)
        self.assertEqual(res.get_total_cpu_usertime(), 10)
        self.assertEqual(res.get_total_cpu_walltime(), 12)

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

        # try with lists with elements 2 incomplete
        tsf = TaskSummaryFactory(con, chm_incomplete_tasks=['hi', 'bye'],
                                 merge_incomplete_tasks=['a', 'b'])
        ts = tsf._get_chm_task_stats()
        self.assertEqual(ts.get_completed_task_count(), 2)
        self.assertEqual(ts.get_total_task_count(), 4)

    def test_get_chm_task_stats_outputcompute_true(self):
        temp_dir = tempfile.mkdtemp()
        try:
            con = CHMConfig('./images', './model', temp_dir, '500x500',
                            '20x20')
            run_dir = os.path.join(temp_dir, CHMJobCreator.RUN_DIR)
            stdout_dir = os.path.join(run_dir,
                                      CHMJobCreator.STDOUT_DIR)
            os.makedirs(stdout_dir, mode=0o755)

            new_format_file2 = os.path.join(stdout_dir, '778786.2')
            f = open(new_format_file2, 'w')
            f.write('a\n\b\nc\n')
            f.write('        User time (seconds): 100.00\n')
            f.write('  System time (seconds): 200.00\n')
            f.write('  Percent of CPU this job got: 1591%\n')
            f.write(' Elapsed (wall clock) time (h:mm:ss or m:ss): 20:15\n')
            f.write('Average shared text size (kbytes): 0\n')
            f.write('Average unshared data size (kbytes): 0\n')
            f.write('Average stack size (kbytes): 0\n')
            f.write('        Maximum resident set size (kbytes): 5287148\n')
            f.flush()
            f.close()

            cfig = configparser.ConfigParser()
            cfig.add_section('1')
            cfig.set('1', 'hi', 'val')
            cfig.add_section('2')
            cfig.set('2', 'hi', 'val')
            cfig.add_section('3')
            cfig.set('3', 'hi', 'val')
            cfig.add_section('4')
            cfig.set('4', 'hi', 'val')
            con.set_config(cfig)
            # try with lists with elements
            tsf = TaskSummaryFactory(con, chm_incomplete_tasks=['hi'],
                                     merge_incomplete_tasks=['a', 'b'],
                                     output_compute=True)
            ts = tsf._get_chm_task_stats()
            self.assertEqual(ts.get_completed_task_count(), 3)
            self.assertEqual(ts.get_total_task_count(), 4)
            self.assertEqual(ts.get_total_tasks_with_cputimes(), 1)
            self.assertEqual(ts.get_max_memory_in_kb(), 5287148)
            self.assertEqual(ts.get_total_cpu_usertime(), 100)
            self.assertEqual(ts.get_total_cpu_walltime(), 1215)

            # try with a second output file
            oldformatfile = os.path.join(stdout_dir, '1234.1')
            f = open(oldformatfile, 'w')
            f.write('HOST: comet-22-63\nDATE: blah\ns\n\n')
            f.write('real 150.0\nuser 250.0\nsys 60.0\n')
            f.write('chmrunner.py exited with code: 0\n')
            f.flush()
            f.close()
            tsf = TaskSummaryFactory(con, chm_incomplete_tasks=['hi'],
                                     merge_incomplete_tasks=['a', 'b'],
                                     output_compute=True)
            ts = tsf._get_chm_task_stats()
            self.assertEqual(ts.get_completed_task_count(), 3)
            self.assertEqual(ts.get_total_task_count(), 4)
            self.assertEqual(ts.get_total_tasks_with_cputimes(), 2)
            self.assertEqual(ts.get_max_memory_in_kb(), 5287148)
            self.assertEqual(ts.get_total_cpu_usertime(), 350)
            self.assertEqual(ts.get_total_cpu_walltime(), 1365)

        finally:
            shutil.rmtree(temp_dir)

    def test_get_image_stats_summary_output_compute_false(self):
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
                                 merge_incomplete_tasks=[])
        isum = tsf._get_image_stats_summary()
        self.assertEqual(isum, None)

    def test_get_image_stats_summary_output_compute_true_but_invalid_dir(self):
        temp_dir = tempfile.mkdtemp()
        try:
            con = CHMConfig(os.path.join(temp_dir, 'images'),
                            './model', './outdir', '500x500', '20x20')
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
                                     merge_incomplete_tasks=[],
                                     output_compute=True)
            isum = tsf._get_image_stats_summary()
            self.assertEqual(isum.get_image_count(), 0)
        finally:
            shutil.rmtree(temp_dir)

    def test_get_image_stats_summary_output_compute_true_no_images(self):
        temp_dir = tempfile.mkdtemp()
        try:
            con = CHMConfig(temp_dir,
                            './model', './outdir', '500x500', '20x20')
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
                                     merge_incomplete_tasks=[],
                                     output_compute=True)
            isum = tsf._get_image_stats_summary()
            self.assertEqual(isum.get_image_count(), 0)
        finally:
            shutil.rmtree(temp_dir)

    def test_get_image_stats_summary_output_compute_true_one_image(self):
        temp_dir = tempfile.mkdtemp()
        try:
            im = Image.new('L', (10, 10))
            iname = os.path.join(temp_dir, '1.png')
            im.save(iname)
            expsize = os.path.getsize(iname)
            con = CHMConfig(temp_dir,
                            './model', './outdir', '500x500', '20x20')
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
                                     merge_incomplete_tasks=[],
                                     output_compute=True)
            isum = tsf._get_image_stats_summary()
            self.assertEqual(isum.get_image_count(), 1)
            self.assertEqual(isum.get_total_pixels(), 100)
            self.assertEqual(isum.get_total_size_of_images_in_bytes(), expsize)
            self.assertEqual(isum.get_image_dimensions_as_dict(),
                             {(10, 10): 1})

        finally:
            shutil.rmtree(temp_dir)

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
