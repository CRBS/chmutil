#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_createchmjob.py
----------------------------------

Tests for `createchmjob.py`
"""

import unittest
import os
import tempfile
import shutil
from PIL import Image

from chmutil import createchmjob


class TestCreateCHMJob(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_main(self):
        # test with bad args which should fail
        val = createchmjob.main(['createchmjob.py', '1', '2', '3'])
        self.assertEqual(val, 2)

    def test_parse_arguments(self):
        pargs = createchmjob._parse_arguments('hi', ['1', '2', '3'])
        self.assertEqual(pargs.images, '1')
        self.assertEqual(pargs.model, '2')
        self.assertEqual(pargs.outdir, '3')
        self.assertEqual(pargs.chmbin, './chm-0.1.0.img')
        self.assertEqual(pargs.tilesize, '512x512')
        self.assertEqual(pargs.overlapsize, '0x0')
        self.assertEqual(pargs.tilespertask, 50)
        self.assertEqual(pargs.taskspernode, 0)
        self.assertEqual(pargs.cluster, 'rocce')
        self.assertEqual(pargs.walltime, '12:00:00')
        self.assertEqual(pargs.jobname, 'chmjob')

    def test_create_chm_job_where_not_able_to_create_job(self):
        temp_dir = tempfile.mkdtemp()
        try:
            pargs = createchmjob._parse_arguments('hi',
                                                  [temp_dir, temp_dir,
                                                   temp_dir,
                                                   '--tilesize',
                                                   ''])
            pargs.program = 'foo'
            pargs.version = '0.1.2'
            val = createchmjob._create_chm_job(pargs)
            self.assertEqual(val, 2)

        finally:
            shutil.rmtree(temp_dir)

    def test_create_chm_job_success(self):
        temp_dir = tempfile.mkdtemp()
        try:
            images = os.path.join(temp_dir, 'images')
            os.makedirs(images, mode=0o755)

            # add a fake png image
            pngfile = os.path.join(images, 'foo.png')
            size = 800, 800
            myimg = Image.new('L', size)
            myimg.save(pngfile, 'PNG')

            model = os.path.join(temp_dir, 'model')
            os.makedirs(model, mode=0o755)
            p_mat = os.path.join(model, 'param.mat')
            open(p_mat, 'a').close()

            out = os.path.join(temp_dir, 'out')

            pargs = createchmjob._parse_arguments('hi',
                                                  [images, model,
                                                   out,
                                                   '--tilesize',
                                                   '520x520'])
            pargs.program = 'foo'
            pargs.version = '0.1.2'
            pargs.rawargs = 'hi how are you'
            val = createchmjob._create_chm_job(pargs)
            self.assertEqual(val, 0)
        finally:
            shutil.rmtree(temp_dir)


if __name__ == '__main__':
    unittest.main()
