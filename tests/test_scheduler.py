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
import stat

from chmutil.cluster import Scheduler
from chmutil.cluster import InvalidScriptNameError
from chmutil.cluster import InvalidWorkingDirError



class TestScheduler(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass


    def test_getters(self):
        sched = Scheduler(None)
        self.assertEqual(sched.get_clustername(), None)
        self.assertEqual(sched.get_jobid_for_arrayjob_variable(), None)
        self.assertEqual(sched.get_jobid_variable(), None)
        self.assertEqual(sched.get_taskid_variable(), None)
        self.assertEqual(sched.get_job_out_file_name(), 'None' +
                         Scheduler.OUT_SUFFIX)
        self.assertEqual(sched.get_array_job_out_file_name(), 'None.None' +
                         Scheduler.OUT_SUFFIX)
        self.assertEqual(sched._get_script_header(None, None, None, None), '')

        sched = Scheduler('foo',queue='queue',account='account',
                          jobid_for_filepath='jobidfilepath',
                          jobid_for_arrayjob='arrayjobid',
                          jobid='jobid',
                          taskid_for_filepath='filepathtaskid',
                          taskid='taskid',
                          submitcmd='submit',
                          arrayflag='aflag',
                          load_singularity_cmd='cmd')
        self.assertEqual(sched.get_clustername(), 'foo')
        self.assertEqual(sched.get_jobid_for_arrayjob_variable(),
                         'arrayjobid')
        self.assertEqual(sched.get_jobid_variable(), 'jobid')
        self.assertEqual(sched.get_taskid_variable(), 'taskid')
        self.assertEqual(sched.get_job_out_file_name(),
                         'jobidfilepath' +
                         Scheduler.OUT_SUFFIX)
        self.assertEqual(sched.get_array_job_out_file_name(),
                         'jobidfilepath.filepathtaskid' +
                         Scheduler.OUT_SUFFIX)
        self.assertEqual(sched._get_script_header(None, None, None, None), '')

    def test_make_script_executable(self):
        temp_dir = tempfile.mkdtemp()
        try:
            tfile = os.path.join(temp_dir, 'foo.txt')
            open(tfile, 'a').close()
            res = stat.S_IMODE(os.stat(tfile).st_mode) & stat.S_IXUSR
            self.assertEqual(res, 0)
            sched = Scheduler(None)
            sched._make_script_executable(tfile)
            res = stat.S_IMODE(os.stat(tfile).st_mode) & stat.S_IXUSR
            self.assertTrue(res > 0)

            # test non existant file, has no effect other then logging
            sched._make_script_executable(os.path.join(temp_dir,
                                                       'doesnotexist'))
        finally:
            shutil.rmtree(temp_dir)

    def test_write_submit_script_invalid_script_name(self):
        sched = Scheduler('foo')
        try:
            sched.write_submit_script(None,'hi', 'foo', 'yo', 'hi', '')
            self.fail('Expected InvalidScriptNameError')
        except InvalidScriptNameError as e:
            self.assertEqual(str(e), 'Script name cannot be None')

    def test_write_submit_script_invalid_working_dir(self):
        sched = Scheduler('foo')
        try:
            sched.write_submit_script('bye', None, 'foo', 'yo', 'hi', '')
            self.fail('Expected InvalidWorkingDirError')
        except InvalidWorkingDirError as e:
            self.assertEqual(str(e), 'Working dir cannot be None')

    def test_write_submit_script_unable_to_write_file(self):
        temp_dir = tempfile.mkdtemp()
        try:
            sched = Scheduler('foo')
            invalid_dir = os.path.join(temp_dir,'nonexistantdir')
            sched.write_submit_script('myscript', invalid_dir, 'foo', 'yo', 'hi', '')
            self.fail('Expected IOError')
        except IOError:
            pass
        finally:
            shutil.rmtree(temp_dir)
if __name__ == '__main__':
    unittest.main()
