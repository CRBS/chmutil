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
import stat

from chmutil import createchmtrainjob
from chmutil.core import Parameters
from chmutil.createchmtrainjob import UnsupportedClusterError
from chmutil.createchmtrainjob import InvalidOutDirError
from chmutil.createchmtrainjob import IMODConversionError
from chmutil.createchmtrainjob import InvalidInputDataError


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
        self.assertEqual(params.imodbindir, '')

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
            f = open(os.path.join(temp_dir, createchmtrainjob.README_FILE),
                     'r')
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
            cwd = os.getcwd()

            labelsdir = os.path.abspath(os.path.join(cwd, 'labels'))
            imgdir = os.path.abspath(os.path.join(cwd, 'images'))
            tmpdir = os.path.abspath(os.path.join(temp_dir, 'tmp'))
            mystr = ('.img train ' + imgdir + ' ' + labelsdir +
                     ' -S 2 -L 4 -m ' + tmpdir)

            self.assertTrue(mystr in data)

            # check the submit command got appended to end of readme file
            f = open(os.path.join(temp_dir, createchmtrainjob.README_FILE),
                     'r')
            data = f.read()
            self.assertTrue('To submit run: cd ' + temp_dir in data)
            f.close()
        finally:
            shutil.rmtree(temp_dir)

    def test_convert_mod_mrc_files_images_is_not_a_dir_or_file(self):
        temp_dir = tempfile.mkdtemp()
        try:
            images_file = os.path.join(temp_dir, 'images.mrc')
            labels_file = os.path.join(temp_dir, 'labels.mod')
            params = createchmtrainjob._parse_arguments('hi',
                                                        [images_file,
                                                         labels_file,
                                                         temp_dir,
                                                         '--cluster',
                                                         'rocce',
                                                         '--imodbindir',
                                                         temp_dir])

            createchmtrainjob._convert_mod_mrc_files(params)
            self.fail('Expected InvalidInputDataError')
        except InvalidInputDataError as e:
            self.assertTrue(images_file + ' does not exist' in str(e))

        finally:
            shutil.rmtree(temp_dir)

    def test_convert_mod_mrc_files_mrc2tif_fails_on_images(self):
        temp_dir = tempfile.mkdtemp()
        try:
            images_file = os.path.join(temp_dir, 'images.mrc')
            labels_file = os.path.join(temp_dir, 'labels.mod')
            os.makedirs(os.path.join(temp_dir,
                                     createchmtrainjob.TMP_DIR), mode=0o755)
            open(images_file, 'a').close()
            open(labels_file, 'a').close()
            mrc2tif = os.path.join(temp_dir, 'mrc2tif')
            f = open(mrc2tif, 'w')
            f.write('#!/usr/bin/env python\nimport sys;sys.exit(1)\n')
            f.flush()
            f.close()
            os.chmod(mrc2tif, stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH)
            params = createchmtrainjob._parse_arguments('hi',
                                                        [images_file,
                                                         labels_file,
                                                         temp_dir,
                                                         '--cluster',
                                                         'rocce',
                                                         '--imodbindir',
                                                         temp_dir])

            createchmtrainjob._convert_mod_mrc_files(params)
            self.fail('Expected IMODConversionError')
        except IMODConversionError as e:
            self.assertTrue('Non zero exit code from '
                            'mrc2tif: 1 :  :' in str(e))

        finally:
            shutil.rmtree(temp_dir)

    def test_convert_mod_mrc_files_labels_not_file(self):
        temp_dir = tempfile.mkdtemp()
        try:
            images_file = os.path.join(temp_dir, 'images.mrc')
            labels_file = os.path.join(temp_dir, 'labels.mod')
            os.makedirs(os.path.join(temp_dir, 'tmp'), mode=0o755)
            open(images_file, 'a').close()

            mrc2tif = os.path.join(temp_dir, 'mrc2tif')
            f = open(mrc2tif, 'w')
            f.write('#!/usr/bin/env python\nimport sys;sys.exit(0)\n')
            f.flush()
            f.close()
            os.chmod(mrc2tif, stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH)

            params = createchmtrainjob._parse_arguments('hi',
                                                        [images_file,
                                                         labels_file,
                                                         temp_dir,
                                                         '--cluster',
                                                         'rocce',
                                                         '--imodbindir',
                                                         temp_dir])

            createchmtrainjob._convert_mod_mrc_files(params)
            self.fail('Expected IMODConversionError')
        except IMODConversionError as e:
            self.assertTrue(labels_file + ' is not a file, '
                                          'cannot convert' in str(e))

        finally:
            shutil.rmtree(temp_dir)

    def test_convert_mod_mrc_files_imodmop_fails(self):
        temp_dir = tempfile.mkdtemp()
        try:
            images_file = os.path.join(temp_dir, 'images.mrc')
            labels_file = os.path.join(temp_dir, 'labels.mod')
            os.makedirs(os.path.join(temp_dir,
                                     createchmtrainjob.TMP_DIR), mode=0o755)
            open(images_file, 'a').close()
            open(labels_file, 'a').close()
            mrc2tif = os.path.join(temp_dir, 'mrc2tif')
            f = open(mrc2tif, 'w')
            f.write('#!/usr/bin/env python\nimport sys;sys.exit(0)\n')
            f.flush()
            f.close()
            os.chmod(mrc2tif, stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH)

            imodmop = os.path.join(temp_dir, 'imodmop')
            f = open(imodmop, 'w')
            f.write('#!/usr/bin/env python\nimport sys;sys.exit(1)\n')
            f.flush()
            f.close()
            os.chmod(imodmop, stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH)

            params = createchmtrainjob._parse_arguments('hi',
                                                        [images_file,
                                                         labels_file,
                                                         temp_dir,
                                                         '--cluster',
                                                         'rocce',
                                                         '--imodbindir',
                                                         temp_dir])

            createchmtrainjob._convert_mod_mrc_files(params)
            self.fail('Expected IMODConversionError')
        except IMODConversionError as e:
            self.assertTrue('Non zero exit code from '
                            'imodmop: 1 :  :' in str(e))

        finally:
            shutil.rmtree(temp_dir)

    def test_convert_mod_mrc_files_second_mrc2tif_fails(self):
        temp_dir = tempfile.mkdtemp()
        try:
            images_file = os.path.join(temp_dir, 'images.mrc')
            labels_file = os.path.join(temp_dir, 'labels.mod')
            os.makedirs(os.path.join(temp_dir,
                                     createchmtrainjob.TMP_DIR), mode=0o755)
            open(images_file, 'a').close()
            open(labels_file, 'a').close()
            mrc2tif = os.path.join(temp_dir, 'mrc2tif')
            f = open(mrc2tif, 'w')
            f.write('#!/usr/bin/env python\nimport sys\n')
            f.write('if "tmp.mrc" in sys.argv[2]:\n sys.exit(2)\n'
                    'sys.exit(0)\n')
            f.flush()
            f.close()
            os.chmod(mrc2tif, stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH)

            imodmop = os.path.join(temp_dir, 'imodmop')
            f = open(imodmop, 'w')
            f.write('#!/usr/bin/env python\nimport sys;sys.exit(0)\n')
            f.flush()
            f.close()
            os.chmod(imodmop, stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH)

            params = createchmtrainjob._parse_arguments('hi',
                                                        [images_file,
                                                         labels_file,
                                                         temp_dir,
                                                         '--cluster',
                                                         'rocce',
                                                         '--imodbindir',
                                                         temp_dir])

            createchmtrainjob._convert_mod_mrc_files(params)
            self.fail('Expected IMODConversionError')
        except IMODConversionError as e:
            self.assertTrue('Non zero exit code from '
                            'mrc2tif: 2 :  :' in str(e))

        finally:
            shutil.rmtree(temp_dir)

    def test_convert_mod_mrc_files_success(self):
        temp_dir = tempfile.mkdtemp()
        try:
            images_file = os.path.join(temp_dir, 'images.mrc')
            labels_file = os.path.join(temp_dir, 'labels.mod')
            os.makedirs(os.path.join(temp_dir,
                                     createchmtrainjob.TMP_DIR), mode=0o755)
            open(images_file, 'a').close()
            open(labels_file, 'a').close()
            mrc2tif = os.path.join(temp_dir, 'mrc2tif')
            f = open(mrc2tif, 'w')
            f.write('#!/usr/bin/env python\nimport sys\n')
            f.write('sys.exit(0)\n')
            f.flush()
            f.close()
            os.chmod(mrc2tif, stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH)

            imodmop = os.path.join(temp_dir, 'imodmop')
            f = open(imodmop, 'w')
            f.write('#!/usr/bin/env python\nimport sys;sys.exit(0)\n')
            f.flush()
            f.close()
            os.chmod(imodmop, stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH)

            params = createchmtrainjob._parse_arguments('hi',
                                                        [images_file,
                                                         labels_file,
                                                         temp_dir,
                                                         '--cluster',
                                                         'rocce',
                                                         '--imodbindir',
                                                         temp_dir])

            img, lbl = createchmtrainjob._convert_mod_mrc_files(params)
            self.assertEqual(img, os.path.join(temp_dir,
                                               createchmtrainjob.IMAGES_DIR))
            self.assertEqual(lbl, os.path.join(temp_dir,
                                               createchmtrainjob.LABELS_DIR))

        finally:
            shutil.rmtree(temp_dir)

    def test_main_success(self):
        temp_dir = tempfile.mkdtemp()
        try:
            rundir = os.path.join(temp_dir, 'run')
            cwd = os.getcwd()
            imgdir = os.path.abspath(os.path.join(cwd, 'images'))
            labelsdir = os.path.abspath(os.path.join(cwd, 'labels'))
            tmpdir = os.path.abspath(os.path.join(rundir, 'tmp'))

            res = createchmtrainjob.main(['me.py', './images', './labels',
                                          rundir, '--cluster', 'rocce'])
            self.assertEqual(res, 0)
            stdout_dir = os.path.join(rundir, createchmtrainjob.STDOUT_DIR)
            self.assertTrue(os.path.isdir(stdout_dir))
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

            mystr = ('.img train ' + imgdir + ' ' + labelsdir +
                     ' -S 2 -L 4 -m ' + tmpdir)
            self.assertTrue(mystr in data)
        finally:
            shutil.rmtree(temp_dir)

    def test_main_success_with_mrc_and_mod_file(self):
        temp_dir = tempfile.mkdtemp()
        try:
            images_file = os.path.join(temp_dir, 'images.mrc')
            labels_file = os.path.join(temp_dir, 'labels.mod')
            open(images_file, 'a').close()
            open(labels_file, 'a').close()

            mrc2tif = os.path.join(temp_dir, 'mrc2tif')
            f = open(mrc2tif, 'w')
            f.write('#!/usr/bin/env python\nimport sys\n')
            f.write('sys.exit(0)\n')
            f.flush()
            f.close()
            os.chmod(mrc2tif, stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH)

            imodmop = os.path.join(temp_dir, 'imodmop')
            f = open(imodmop, 'w')
            f.write('#!/usr/bin/env python\nimport sys;sys.exit(0)\n')
            f.flush()
            f.close()
            os.chmod(imodmop, stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH)

            tmpdir = os.path.abspath(os.path.join(temp_dir,
                                                  createchmtrainjob.TMP_DIR))

            res = createchmtrainjob.main(['me.py', images_file, labels_file,
                                          temp_dir, '--cluster', 'rocce',
                                          '--imodbindir', temp_dir])
            self.assertEqual(res, 0)
            stdout_dir = os.path.join(temp_dir, createchmtrainjob.STDOUT_DIR)
            self.assertTrue(os.path.isdir(stdout_dir))
            readme = os.path.join(temp_dir, createchmtrainjob.README_FILE)
            self.assertTrue(os.path.isfile(readme))
            f = open(os.path.join(temp_dir, createchmtrainjob.README_FILE),
                     'r')
            data = f.read()
            f.close()

            imgdir = os.path.join(temp_dir, createchmtrainjob.IMAGES_DIR)
            labelsdir = os.path.join(temp_dir, createchmtrainjob.LABELS_DIR)
            self.assertTrue('me.py ' + images_file + ' ' + labels_file in data)

            script = os.path.join(temp_dir, createchmtrainjob.RUNTRAIN +
                                  'rocce')
            f = open(script, 'r')
            data = f.read()
            f.close()
            self.assertTrue('-N chmtrainjob' in data)

            mystr = ('.img train ' + imgdir + ' ' + labelsdir +
                     ' -S 2 -L 4 -m ' + tmpdir)
            self.assertTrue(mystr in data)
        finally:
            shutil.rmtree(temp_dir)


if __name__ == '__main__':
    unittest.main()
