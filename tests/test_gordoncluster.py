#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_gordoncluster
----------------------------------

Tests for `GordonCluster` class
"""

import os
import unittest
import tempfile
import shutil

from chmutil.core import CHMConfig
from chmutil.cluster import GordonCluster
from chmutil.core import CHMJobCreator


class TestGordonCluster(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_get_suggested_tasks_per_node(self):
        rc = GordonCluster(None)
        self.assertEqual(rc.get_suggested_tasks_per_node(None),
                         GordonCluster.DEFAULT_JOBS_PER_NODE)

        self.assertEqual(rc.get_suggested_tasks_per_node(0),
                         GordonCluster.DEFAULT_JOBS_PER_NODE)

        self.assertEqual(rc.get_suggested_tasks_per_node('foo'),
                         GordonCluster.DEFAULT_JOBS_PER_NODE)

        self.assertEqual(rc.get_suggested_tasks_per_node(5), 5)

        self.assertEqual(rc.get_suggested_tasks_per_node(5.5), 5)

    def test_get_cluster(self):
        rc = GordonCluster(None)
        self.assertEqual(rc.get_cluster(), GordonCluster.CLUSTER)

    def test_get_merge_submit_script_path(self):
        opts = CHMConfig('images', 'model', 'out',
                         '500x500', '20x20')
        rc = GordonCluster(None)
        self.assertEqual(rc._get_merge_submit_script_path(),
                         GordonCluster.MERGE_SUBMIT_SCRIPT_NAME)

        opts = CHMConfig('images', 'model', 'out',
                         '500x500', '20x20')
        rc = GordonCluster(opts)
        self.assertEqual(rc._get_merge_submit_script_path(),
                         os.path.join('out',
                                      GordonCluster.MERGE_SUBMIT_SCRIPT_NAME))

    def test_get_merge_runner_path(self):
        rc = GordonCluster(None)
        self.assertEqual(rc._get_merge_runner_path(),
                         CHMJobCreator.MERGERUNNER)

        opts = CHMConfig('images', 'model', 'out',
                         '500x500', '20x20', scriptbin='foo')
        rc = GordonCluster(opts)
        self.assertEqual(rc._get_merge_runner_path(),
                         os.path.join('foo',
                                      CHMJobCreator.MERGERUNNER))

    def test_get_submit_script_path(self):
        opts = CHMConfig('images', 'model', 'out',
                         '500x500', '20x20')
        gen = GordonCluster(None)
        val = gen._get_submit_script_path()
        self.assertEqual(val,
                         GordonCluster.SUBMIT_SCRIPT_NAME)
        gen = GordonCluster(opts)

        val = gen._get_submit_script_path()
        script = os.path.join(opts.get_out_dir(),
                              GordonCluster.SUBMIT_SCRIPT_NAME)
        self.assertEqual(val, script)

    def test_get_chm_runner_path(self):
        gen = GordonCluster(None)
        self.assertEqual(gen._get_chm_runner_path(),
                         CHMJobCreator.CHMRUNNER)

        opts = CHMConfig('images', 'model', 'out',
                         '500x500', '20x20')
        gen = GordonCluster(opts)
        self.assertEqual(gen._get_chm_runner_path(),
                         CHMJobCreator.CHMRUNNER)

        opts = CHMConfig('images', 'model', 'out',
                         '500x500', '20x20',
                         scriptbin='/home/foo/.local/bin')
        gen = GordonCluster(opts)
        spath = os.path.join('/home/foo/.local/bin',
                             CHMJobCreator.CHMRUNNER)
        self.assertEqual(gen._get_chm_runner_path(),
                         spath)

    def test_get_chm_submit_command(self):
        temp_dir = tempfile.mkdtemp()
        try:
            opts = CHMConfig('images', 'model', temp_dir,
                             '500x500', '20x20')
            rc = GordonCluster(opts)
            self.assertEqual(rc.get_chm_submit_command(5),
                             'cd "' + temp_dir + '";qsub ' +
                             GordonCluster.SUBMIT_SCRIPT_NAME)
        finally:
            shutil.rmtree(temp_dir)

    def test_get_chm_submit_command_too_many_jobs(self):
        temp_dir = tempfile.mkdtemp()
        try:
            opts = CHMConfig('images', 'model', temp_dir,
                             '500x500', '20x20')
            rc = GordonCluster(opts)
            self.assertTrue(GordonCluster.WARNING_MESSAGE +
                            'cd "' + temp_dir + '";qsub ' +
                            GordonCluster.SUBMIT_SCRIPT_NAME
                            in rc.get_chm_submit_command(1001))
        finally:
            shutil.rmtree(temp_dir)

    def test_get_checkchmjob_command(self):
        temp_dir = tempfile.mkdtemp()
        try:
            opts = CHMConfig('images', 'model', temp_dir,
                             '500x500', '20x20')

            rc = GordonCluster(opts)
            self.assertEqual(rc.get_checkchmjob_command(),
                             CHMJobCreator.CHECKCHMJOB + ' "' + temp_dir +
                             '" --submit')
        finally:
            shutil.rmtree(temp_dir)

    def test_get_merge_submit_command(self):
        temp_dir = tempfile.mkdtemp()
        try:
            opts = CHMConfig('images', 'model', temp_dir,
                             '500x500', '20x20')

            rc = GordonCluster(opts)
            self.assertEqual(rc.get_merge_submit_command(100),
                             'cd "' + temp_dir + '";qsub ' +
                             GordonCluster.MERGE_SUBMIT_SCRIPT_NAME)
        finally:
            shutil.rmtree(temp_dir)

    def test_get_merge_submit_command_too_many_jobs(self):
        temp_dir = tempfile.mkdtemp()
        try:
            opts = CHMConfig('images', 'model', temp_dir,
                             '500x500', '20x20')

            rc = GordonCluster(opts)
            self.assertEqual(rc.get_merge_submit_command(4000),
                             GordonCluster.WARNING_MESSAGE +
                             'cd "' + temp_dir + '";qsub ' +
                             GordonCluster.MERGE_SUBMIT_SCRIPT_NAME)
        finally:
            shutil.rmtree(temp_dir)

    def test_generate_submit_script(self):
        temp_dir = tempfile.mkdtemp()
        try:
            opts = CHMConfig('images', 'model', temp_dir,
                             '500x500', '20x20', account='tt234')
            gen = GordonCluster(None)
            gen.set_chmconfig(opts)
            script = gen._get_submit_script_path()
            self.assertEqual(os.path.isfile(script), False)
            gen.generate_submit_script()
            self.assertEqual(os.path.isfile(script), True)
            # TODO Test qsub script file has correct data in it
            f = open(script, 'r')
            script_data = f.read()
            f.close()
            self.assertTrue('#PBS -A tt234\n' in script_data)
        finally:
            shutil.rmtree(temp_dir)

    def test_generate_merge_submit_script(self):
        temp_dir = tempfile.mkdtemp()
        try:
            opts = CHMConfig('images', 'model', temp_dir,
                             '500x500', '20x20')
            gen = GordonCluster(opts)
            script = gen._get_merge_submit_script_path()
            self.assertEqual(os.path.isfile(script), False)
            gen.generate_merge_submit_script()
            self.assertEqual(os.path.isfile(script), True)
            # TODO Test qsub script file has correct data in it
            # TODO Test qsub script file has correct data in it
            f = open(script, 'r')
            script_data = f.read()
            f.close()
            self.assertTrue('#PBS -A \n' in script_data)
        finally:
            shutil.rmtree(temp_dir)


if __name__ == '__main__':
    unittest.main()
