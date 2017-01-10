#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_checkchmjob.py
----------------------------------

Tests for `checkchmjob.py`
"""

import unittest
import os
import tempfile
import shutil
from PIL import Image

from chmutil import checkchmjob
from chmutil import createchmjob
from chmutil.core import LoadConfigError
from chmutil.core import CHMJobCreator
from chmutil.core import CHMConfigFromConfigFactory


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
    pargs.rawargs = 'hi how are you'
    createchmjob._create_chm_job(pargs)
    return out


class TestCheckCHMJob(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_main(self):
        temp_dir = tempfile.mkdtemp()
        try:
            # test with bad args which should fail
            checkchmjob.main(['createchmjob.py', temp_dir])
            self.fail('expected LoadConfigError')
        except LoadConfigError as e:
            config = os.path.join(temp_dir, CHMJobCreator.CONFIG_FILE_NAME)
            self.assertEqual(str(e), config + ' configuration file does not'
                                              ' exist')

        finally:
            shutil.rmtree(temp_dir)

    def test_parse_arguments(self):
        pargs = checkchmjob._parse_arguments('hi', ['1'])
        self.assertEqual(pargs.jobdir, '1')

    def test_check_chm_job_success(self):
        temp_dir = tempfile.mkdtemp()
        try:
            out = create_successful_job(temp_dir)
            pargs = checkchmjob._parse_arguments('hi', [out])
            pargs.program = 'foo'
            pargs.version = '1.0.0'
            val = checkchmjob._check_chm_job(pargs)
            self.assertEqual(val, 0)
        finally:
            shutil.rmtree(temp_dir)

    def test_check_chm_job_no_jobs_to_run(self):
        temp_dir = tempfile.mkdtemp()
        try:
            out = create_successful_job(temp_dir)
            pargs = checkchmjob._parse_arguments('hi', [out])
            pargs.program = 'foo'
            pargs.version = '1.0.0'
            img_tile = os.path.join(out, CHMJobCreator.RUN_DIR,
                                    CHMJobCreator.TILES_DIR,
                                    'foo.png', '001.foo.png')
            size = 800, 800
            myimg = Image.new('L', size)
            myimg.save(img_tile, 'PNG')
            val = checkchmjob._check_chm_job(pargs)
            self.assertEqual(val, 0)
        finally:
            shutil.rmtree(temp_dir)

    def test_check_chm_job_no_jobs_to_run_and_skipchm_true(self):
        temp_dir = tempfile.mkdtemp()
        try:
            out = create_successful_job(temp_dir)
            pargs = checkchmjob._parse_arguments('hi', [out, '--skipchm'])
            pargs.program = 'foo'
            pargs.version = '1.0.0'
            img_tile = os.path.join(out, CHMJobCreator.RUN_DIR,
                                    CHMJobCreator.TILES_DIR,
                                    'foo.png', '001.foo.png')
            size = 800, 800
            myimg = Image.new('L', size)
            myimg.save(img_tile, 'PNG')
            val = checkchmjob._check_chm_job(pargs)
            self.assertEqual(val, 0)
        finally:
            shutil.rmtree(temp_dir)

    def test_check_chm_job_no_jobs_to_run_and_detailed_true(self):
        temp_dir = tempfile.mkdtemp()
        try:
            out = create_successful_job(temp_dir)
            pargs = checkchmjob._parse_arguments('hi', [out, '--detailed'])
            pargs.program = 'foo'
            pargs.version = '1.0.0'
            img_tile = os.path.join(out, CHMJobCreator.RUN_DIR,
                                    CHMJobCreator.TILES_DIR,
                                    'foo.png', '001.foo.png')
            size = 800, 800
            myimg = Image.new('L', size)
            myimg.save(img_tile, 'PNG')
            val = checkchmjob._check_chm_job(pargs)
            self.assertEqual(val, 0)
        finally:
            shutil.rmtree(temp_dir)

    def test_check_chm_job_no_jobs_to_run_and_submit_true(self):
        temp_dir = tempfile.mkdtemp()
        try:
            out = create_successful_job(temp_dir)
            pargs = checkchmjob._parse_arguments('hi', [out, '--submit'])
            pargs.program = 'foo'
            pargs.version = '1.0.0'
            img_tile = os.path.join(out, CHMJobCreator.RUN_DIR,
                                    CHMJobCreator.TILES_DIR,
                                    'foo.png', '001.foo.png')
            size = 800, 800
            myimg = Image.new('L', size)
            myimg.save(img_tile, 'PNG')
            val = checkchmjob._check_chm_job(pargs)

            self.assertEqual(val, 0)
        finally:
            shutil.rmtree(temp_dir)

    def test_check_chm_job_invalid_cluster(self):
        temp_dir = tempfile.mkdtemp()
        try:
            out = create_successful_job(temp_dir)

            cfac = CHMConfigFromConfigFactory(out)

            # alter cluster name to be an invalid one
            #
            chmconfig = cfac.get_chmconfig()
            con = chmconfig.get_config()
            con.set(CHMJobCreator.CONFIG_DEFAULT, CHMJobCreator.CONFIG_CLUSTER,
                    'doesnotexist')
            f = open(chmconfig.get_job_config(), 'w')
            con.write(f)
            f.flush()
            f.close()
            #

            pargs = checkchmjob._parse_arguments('hi', [out, '--submit'])
            pargs.program = 'foo'
            pargs.version = '1.0.0'
            img_tile = os.path.join(out, CHMJobCreator.RUN_DIR,
                                    CHMJobCreator.TILES_DIR,
                                    'foo.png', '001.foo.png')
            size = 800, 800
            myimg = Image.new('L', size)
            myimg.save(img_tile, 'PNG')
            val = checkchmjob._check_chm_job(pargs)

            self.assertEqual(val, 2)
        finally:
            shutil.rmtree(temp_dir)

    def test_check_chm_job_one_chm_job_to_run(self):
        temp_dir = tempfile.mkdtemp()
        try:
            out = create_successful_job(temp_dir)
            pargs = checkchmjob._parse_arguments('hi', [out, '--submit'])
            pargs.program = 'foo'
            pargs.version = '1.0.0'

            val = checkchmjob._check_chm_job(pargs)
            self.assertEqual(val, 0)

            cfac = CHMConfigFromConfigFactory(out)
            chmconfig = cfac.get_chmconfig()

            path = chmconfig.get_batchedjob_config_file_path()
            self.assertTrue(os.path.isfile(path))

        finally:
            shutil.rmtree(temp_dir)

    def test_check_chm_job_all_tasks_complete_with_submit(self):
        temp_dir = tempfile.mkdtemp()
        try:
            out = create_successful_job(temp_dir)
            pargs = checkchmjob._parse_arguments('hi', [out, '--submit'])
            pargs.program = 'foo'
            pargs.version = '1.0.0'
            img_tile = os.path.join(out, CHMJobCreator.RUN_DIR,
                                    CHMJobCreator.TILES_DIR,
                                    'foo.png', '001.foo.png')
            size = 800, 800
            myimg = Image.new('L', size)
            myimg.save(img_tile, 'PNG')

            probmap = os.path.join(out, CHMJobCreator.RUN_DIR,
                                   CHMJobCreator.PROBMAPS_DIR,
                                   'foo.png')
            myimg.save(probmap, 'PNG')

            val = checkchmjob._check_chm_job(pargs)
            self.assertEqual(val, 0)

            cfac = CHMConfigFromConfigFactory(out)
            chmconfig = cfac.get_chmconfig()

            path = chmconfig.get_batchedjob_config_file_path()
            self.assertEqual(os.path.isfile(path), False)

            mpath = chmconfig.get_batched_mergejob_config_file_path()
            self.assertEqual(os.path.isfile(mpath), False)

        finally:
            shutil.rmtree(temp_dir)

if __name__ == '__main__':
    unittest.main()
