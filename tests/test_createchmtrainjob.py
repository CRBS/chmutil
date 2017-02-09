#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_createchmtrainjob.py
----------------------------------

Tests for `createchmtrainjob.py`
"""

import unittest
import os
import tempfile
import shutil

from chmutil import createchmtrainjob
from chmutil.core import Parameters
from chmutil.createchmtrainjob import UnsupportedClusterError
from chmutil.createchmtrainjob import InvalidOutDirError


class TestCreateCHMTrainJob(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_parse_arguments(self):
        params = createchmtrainjob._parse_arguments('hi',
                                                    ['images', 'labels',
                                                     'outdir'])
        self.assertEqual(params.images, 'images')
        self.assertEqual(params.labels, 'labels')
        self.assertEqual(params.outdir, 'outdir')
        self.assertEqual(params.chmbin, './chm-0.1.0.img')
        self.assertEqual(params.stage, 2)
        self.assertEqual(params.level, 4)
        self.assertEqual(params.cluster, 'rocce')
        self.assertEqual(params.account, '')
        self.assertEqual(params.jobname, 'chmtrainjob')
        self.assertEqual(params.walltime, '24:00:00')
        self.assertEqual(params.maxmem, 90)
        self.assertEqual(params.cluster, 'rocce')
        self.assertEqual(params.loglevel, 'WARNING')

    def test_create_directories_and_readme_outdir_isnot_dir(self):
        temp_dir = tempfile.mkdtemp()
        try:
            rundir = os.path.join(temp_dir, 'run')
            open(rundir, 'a').close()
            createchmtrainjob._create_directories_and_readme(rundir,
                                                             'some args here')
            self.fail('Expected InvalidOutDirError')
        except InvalidOutDirError as e:
            self.assertEqual(str(e), rundir +
                             ' exists, but is not a directory')
        finally:
            shutil.rmtree(temp_dir)

    def test_create_directories_and_readme_outdir_exists(self):
        temp_dir = tempfile.mkdtemp()
        try:
            createchmtrainjob._create_directories_and_readme(temp_dir,
                                                             'some args here')
            stdout_dir = os.path.join(temp_dir, createchmtrainjob.STDOUT_DIR)
            self.assertTrue(os.path.isdir(stdout_dir))
            thetmp_dir = os.path.join(temp_dir, createchmtrainjob.TMP_DIR)
            self.assertTrue(os.path.isdir(thetmp_dir))
            readme = os.path.join(temp_dir, createchmtrainjob.README_FILE)
            self.assertTrue(os.path.isfile(readme))
            f = open(os.path.join(temp_dir, createchmtrainjob.README_FILE), 'r')
            data = f.read()
            self.assertTrue('some args here' in data)
            f.close()
        finally:
            shutil.rmtree(temp_dir)

    def test_create_directories_and_readme_outdir_doesnotexist(self):
        temp_dir = tempfile.mkdtemp()
        try:
            rundir = os.path.join(temp_dir, 'run')
            createchmtrainjob._create_directories_and_readme(rundir,
                                                             'some args here')
            stdout_dir = os.path.join(rundir, createchmtrainjob.STDOUT_DIR)
            self.assertTrue(os.path.isdir(stdout_dir))
            thetmp_dir = os.path.join(rundir, createchmtrainjob.TMP_DIR)
            self.assertTrue(os.path.isdir(rundir))
            readme = os.path.join(rundir, createchmtrainjob.README_FILE)
            self.assertTrue(os.path.isfile(readme))
            f = open(os.path.join(rundir, createchmtrainjob.README_FILE), 'r')
            data = f.read()
            self.assertTrue('some args here' in data)
            f.close()
        finally:
            shutil.rmtree(temp_dir)

    def test_create_submit_script_unsupported_cluster(self):
        params = Parameters()
        params.cluster = 'foo'
        try:
            createchmtrainjob._create_submit_script(params)
            self.fail('Expected UnsupportedClusterError')
        except UnsupportedClusterError as e:
            self.assertEqual(str(e), 'foo is not known')

    def test_create_submit_script_rocce_cluster(self):
        temp_dir = tempfile.mkdtemp()
        try:
            params = createchmtrainjob._parse_arguments('hi',
                                                        ['./images',
                                                         './labels',
                                                         temp_dir,
                                                         '--cluster',
                                                         'rocce'])

            script = os.path.join(temp_dir, createchmtrainjob.RUNTRAIN +
                                  'rocce')
            ucmd = createchmtrainjob._create_submit_script(params)
            self.assertEqual(ucmd, 'To submit run: cd ' + temp_dir +
                             '; qsub ' + script)

            f = open(script, 'r')
            data = f.read()
            f.close()
            self.assertTrue('-N chmtrainjob' in data)
            self.assertTrue('.img train ./images ./labels -S 2 -L 4 -m '
                            './tmp' in data)

            # check the submit command got appended to end of readme file
            f = open(os.path.join(temp_dir, createchmtrainjob.README_FILE), 'r')
            data = f.read()
            self.assertTrue('To submit run: cd ' + temp_dir in data)
            f.close()
        finally:
            shutil.rmtree(temp_dir)

    def test_main_success(self):
        temp_dir = tempfile.mkdtemp()
        try:
            rundir = os.path.join(temp_dir, 'run')
            res = createchmtrainjob.main(['me.py', './images', './labels',
                                          rundir, '--cluster', 'rocce'])
            self.assertEqual(res, 0)
            stdout_dir = os.path.join(rundir, createchmtrainjob.STDOUT_DIR)
            self.assertTrue(os.path.isdir(stdout_dir))
            thetmp_dir = os.path.join(rundir, createchmtrainjob.TMP_DIR)
            self.assertTrue(os.path.isdir(rundir))
            readme = os.path.join(rundir, createchmtrainjob.README_FILE)
            self.assertTrue(os.path.isfile(readme))
            f = open(os.path.join(rundir, createchmtrainjob.README_FILE), 'r')
            data = f.read()
            self.assertTrue('me.py ./images ./labels' in data)
            f.close()

            script = os.path.join(rundir, createchmtrainjob.RUNTRAIN +
                                  'rocce')
            f = open(script, 'r')
            data = f.read()
            f.close()
            self.assertTrue('-N chmtrainjob' in data)
            self.assertTrue('.img train ./images ./labels -S 2 -L 4 -m '
                            './tmp' in data)
        finally:
            shutil.rmtree(temp_dir)


if __name__ == '__main__':
    unittest.main()
