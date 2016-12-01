#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_runchmjob.py
----------------------------------

Tests for `runchmjob.py`
"""

import unittest
import os
import tempfile
import shutil
from PIL import Image

from chmutil import runchmjob
from chmutil import createchmjob
from chmutil.core import LoadConfigError
from chmutil.core import CHMJobCreator


def create_successful_job(a_tmp_dir):
    """creates a successful job
    """
    images = os.path.join(a_tmp_dir, 'images')
    os.makedirs(images, mode=0o755)

    # add a fake png image
    pngfile = os.path.join(images, 'foo.png')
    size = 800, 800
    myimg = Image.new('L', size)
    myimg.save(pngfile, 'PNG')

    model = os.path.join(a_tmp_dir, 'model')
    os.makedirs(model, mode=0o755)
    p_mat = os.path.join(model, 'param.mat')
    open(p_mat, 'a').close()

    out = os.path.join(a_tmp_dir, 'out')

    pargs = createchmjob._parse_arguments('hi',
                                          [images, model,
                                           out,
                                           '--tilesize',
                                           '520x520'])
    pargs.program = 'foo'
    pargs.version = '0.1.2'
    createchmjob._create_chm_job(pargs)
    return out


class TestRunCHMJob(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_main(self):
        temp_dir = tempfile.mkdtemp()
        try:
            # test with bad args which should fail
            val = runchmjob.main(['createchmjob.py', temp_dir])
            self.fail('expected LoadConfigError')
        except LoadConfigError as e:
            config = os.path.join(temp_dir, CHMJobCreator.CONFIG_FILE_NAME)
            self.assertEqual(str(e), config + ' configuration file does not'
                                              ' exist')

        finally:
            shutil.rmtree(temp_dir)

    def test_parse_arguments(self):
        pargs = runchmjob._parse_arguments('hi', ['1'])
        self.assertEqual(pargs.jobdir, '1')
        self.assertEqual(pargs.cluster, 'rocce')

    def test_run_chm_job_success(self):
        temp_dir = tempfile.mkdtemp()
        try:
            out = create_successful_job(temp_dir)
            pargs = runchmjob._parse_arguments('hi', [out])
            pargs.program = 'foo'
            pargs.version = '1.0.0'
            val = runchmjob._run_chm_job(pargs)
            self.assertEqual(val, 0)
        finally:
            shutil.rmtree(temp_dir)

    def test_run_chm_job_no_jobs_to_run(self):
        temp_dir = tempfile.mkdtemp()
        try:
            out = create_successful_job(temp_dir)
            pargs = runchmjob._parse_arguments('hi', [out])
            pargs.program = 'foo'
            pargs.version = '1.0.0'
            img_tile = os.path.join(out, CHMJobCreator.RUN_DIR,
                                    'foo.png', '001.foo.png')
            size = 800, 800
            myimg = Image.new('L', size)
            myimg.save(img_tile, 'PNG')
            val = runchmjob._run_chm_job(pargs)
            self.assertEqual(val, 0)
        finally:
            shutil.rmtree(temp_dir)

    def test_run_chm_job_invalid_cluster(self):
        temp_dir = tempfile.mkdtemp()
        try:
            out = create_successful_job(temp_dir)
            pargs = runchmjob._parse_arguments('hi', [out])
            pargs.program = 'foo'
            pargs.version = '1.0.0'
            pargs.cluster = 'foo'
            val = runchmjob._run_chm_job(pargs)
            self.assertEqual(val, 2)
        finally:
            shutil.rmtree(temp_dir)


if __name__ == '__main__':
    unittest.main()
