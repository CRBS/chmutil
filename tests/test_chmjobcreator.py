#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_chmutil
----------------------------------

Tests for `chmutil` module.
"""

import unittest
import tempfile
import shutil
import os
import configparser


from PIL import Image

from chmutil.core import CHMJobCreator
from chmutil.core import CHMConfig
from chmutil.core import ImageStats


class TestCHMJobCreator(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def _create_png_image(self, path, size_tuple):
        myimg = Image.new('L', size_tuple)
        myimg.save(path, 'PNG')

    def test_write_readme_no_args(self):
        temp_dir = tempfile.mkdtemp()
        try:
            opts = CHMConfig('/foo', 'model', temp_dir, '200x100',
                             '20x20', tasks_per_node=20)
            creator = CHMJobCreator(opts)
            con = creator._create_config()
            creator._write_readme(con)
            readme = os.path.join(temp_dir, CHMJobCreator.README_TXT_FILE)
            self.assertTrue(os.path.isfile(readme))
            f = open(readme, 'r')
            data = f.read()
            f.close()
            self.assertTrue('command line:\n\nUnknown' in data)
            self.assertTrue('Chmutil version: unknown' in data)

        finally:
            shutil.rmtree(temp_dir)

    def test_write_readme_with_args(self):
        temp_dir = tempfile.mkdtemp()
        try:
            opts = CHMConfig('/foo', 'model', temp_dir, '200x100',
                             '20x20', tasks_per_node=20, version='2.0',
                             rawargs='hi how are you')
            creator = CHMJobCreator(opts)
            con = creator._create_config()
            creator._write_readme(con)
            readme = os.path.join(temp_dir, CHMJobCreator.README_TXT_FILE)
            self.assertTrue(os.path.isfile(readme))
            f = open(readme, 'r')
            data = f.read()
            f.close()
            self.assertTrue('cd ' + os.getcwd() + ';hi how are you' in data)
            self.assertTrue('Chmutil version: 2.0' in data)
        finally:
            shutil.rmtree(temp_dir)

    def test_create_config_and_write_config(self):
        temp_dir = tempfile.mkdtemp()
        try:
            opts = CHMConfig('/foo', 'model', temp_dir, '200x100',
                             '20x20', tasks_per_node=20)
            creator = CHMJobCreator(opts)
            con = creator._create_config()

            self.assertEqual(con.get('DEFAULT', 'model'), 'model')
            self.assertEqual(con.get('DEFAULT',
                                     CHMJobCreator.CONFIG_TILES_PER_TASK), '1')
            self.assertEqual(con.get('DEFAULT', 'tilesize'), '200x100')
            self.assertEqual(con.get('DEFAULT', 'overlapsize'), '20x20')
            self.assertEqual(con.get('DEFAULT', 'disablehisteqimages'), 'True')
            self.assertEqual(con.get('DEFAULT',
                                     CHMJobCreator.CONFIG_TASKS_PER_NODE),
                             '20')
            cfile = creator._write_config(con)
            self.assertEqual(os.path.isfile(cfile), True)

        finally:
            shutil.rmtree(temp_dir)

    def test_create_run_dir(self):
        temp_dir = tempfile.mkdtemp()
        try:
            opts = CHMConfig(os.path.join(temp_dir, 'images'), 'model',
                             temp_dir, '200x100', '20x20')
            creator = CHMJobCreator(opts)
            run_dir = creator._create_run_dir()
            self.assertTrue(os.path.isdir(run_dir))
            stdout_dir = os.path.join(run_dir, CHMJobCreator.STDOUT_DIR)
            self.assertTrue(os.path.isdir(stdout_dir))
        finally:
            shutil.rmtree(temp_dir)

    def test_create_output_image_dir(self):
        temp_dir = tempfile.mkdtemp()
        try:
            opts = CHMConfig(os.path.join(temp_dir, 'images'), 'model',
                             temp_dir, '200x100', '20x20')
            creator = CHMJobCreator(opts)
            run_dir = creator._create_run_dir()
            self.assertTrue(os.path.isdir(run_dir))
            iis = ImageStats(os.path.join(temp_dir, 'images', 'foo123.png'),
                             500, 400, 'PNG')
            i_name = creator._create_output_image_dir(iis, run_dir)
            self.assertTrue(os.path.isdir(os.path.join(run_dir,
                                                       CHMJobCreator.TILES_DIR,
                                                       'foo123.png')))
            self.assertEqual(i_name, 'foo123.png')

        finally:
            shutil.rmtree(temp_dir)

    def test_add_job_for_image_to_config(self):
        temp_dir = tempfile.mkdtemp()
        try:
            opts = CHMConfig(os.path.join(temp_dir, 'images'), 'model',
                             temp_dir, '200x100', '20x20')
            creator = CHMJobCreator(opts)
            config = creator._create_config()
            run_dir = creator._create_run_dir()
            self.assertTrue(os.path.isdir(run_dir))
            iis = ImageStats(os.path.join(temp_dir, 'images', 'foo123.png'),
                             500, 400, 'PNG')
            i_name = creator._create_output_image_dir(iis, run_dir)
            self.assertEqual(i_name, 'foo123.png')

            creator._add_task_for_image_to_config(config, '12', i_name,
                                                  2, ['-t 1,1'])
            self.assertEqual(config.get('12',
                                        CHMJobCreator.CONFIG_INPUT_IMAGE),
                             'foo123.png')
            self.assertEqual(config.get('12',
                                        CHMJobCreator.CONFIG_ARGS),
                             '-t 1,1')
            self.assertEqual(config.get('12',
                                        CHMJobCreator.CONFIG_OUTPUT_IMAGE),
                             os.path.join(CHMJobCreator.TILES_DIR,
                                          i_name, '002.' + i_name))
        finally:
            shutil.rmtree(temp_dir)

    def test_create_job_one_image_one_tile_per_job(self):
        temp_dir = tempfile.mkdtemp()
        try:
            image_dir = os.path.join(temp_dir, 'images')
            os.makedirs(image_dir, mode=0o775)
            fooimg = os.path.join(image_dir, 'foo1.png')
            self._create_png_image(fooimg, (400, 300))

            opts = CHMConfig(image_dir, 'model',
                             temp_dir, '200x100', '0x0')
            creator = CHMJobCreator(opts)
            opts = creator.create_job()
            self.assertEqual(opts.get_out_dir(), temp_dir)
            self.assertEqual(opts.get_job_config(),
                             os.path.join(temp_dir,
                                          CHMJobCreator.CONFIG_FILE_NAME))
            tmpdir = os.path.join(opts.get_run_dir(), CHMJobCreator.STDOUT_DIR)
            self.assertTrue(os.path.isdir(tmpdir))
            config = configparser.ConfigParser()
            config.read(opts.get_job_config())
            self.assertEqual(config.sections(),
                             ['1', '2', '3', '4', '5', '6'])
            self.assertEqual(config.get('1',
                                        CHMJobCreator.CONFIG_INPUT_IMAGE),
                             'foo1.png')
            self.assertEqual(config.get('1', CHMJobCreator.CONFIG_ARGS),
                             '-t 1,1')
            self.assertEqual(config.get('1',
                                        CHMJobCreator.CONFIG_OUTPUT_IMAGE),
                             os.path.join(CHMJobCreator.TILES_DIR,
                                          'foo1.png', '001.foo1.png'))

            self.assertEqual(config.get('1', CHMJobCreator.CONFIG_MODEL),
                             'model')
            self.assertEqual(config.get('1',
                                        CHMJobCreator.
                                        CONFIG_DISABLE_HISTEQ_IMAGES),
                             'True')
            self.assertEqual(config.get('1',
                                        CHMJobCreator.CONFIG_TASKS_PER_NODE),
                             '1')

        finally:
            shutil.rmtree(temp_dir)

    def test_create_job_one_image_five_tiles_per_job(self):
        temp_dir = tempfile.mkdtemp()
        try:
            image_dir = os.path.join(temp_dir, 'images')
            os.makedirs(image_dir, mode=0o775)
            fooimg = os.path.join(image_dir, 'foo1.png')
            self._create_png_image(fooimg, (400, 300))

            opts = CHMConfig(image_dir, 'model',
                             temp_dir, '200x100', '0x0',
                             number_tiles_per_task=5)
            creator = CHMJobCreator(opts)
            opts = creator.create_job()
            self.assertEqual(opts.get_out_dir(), temp_dir)
            self.assertEqual(opts.get_job_config(),
                             os.path.join(temp_dir,
                                          CHMJobCreator.CONFIG_FILE_NAME))
            self.assertTrue(os.path.isdir(os.path.join(opts.get_out_dir(),
                                                       CHMJobCreator.RUN_DIR)))
            config = configparser.ConfigParser()
            config.read(opts.get_job_config())

            self.assertEqual(config.sections(),
                             ['1', '2'])
            self.assertEqual(config.get('1',
                                        CHMJobCreator.CONFIG_INPUT_IMAGE),
                             'foo1.png')
            self.assertEqual(config.get('1', CHMJobCreator.CONFIG_ARGS),
                             '-t 1,1 -t 1,2 -t 1,3 -t 2,1 -t 2,2')

            self.assertEqual(config.get('1',
                                        CHMJobCreator.CONFIG_OUTPUT_IMAGE),
                             os.path.join(CHMJobCreator.TILES_DIR,
                                          'foo1.png', '001.foo1.png'))
            self.assertTrue(os.path.isdir(os.path.join(temp_dir,
                                                       CHMJobCreator.RUN_DIR,
                                                       CHMJobCreator.TILES_DIR,
                                                       'foo1.png')))

            self.assertEqual(config.get('2',
                                        CHMJobCreator.CONFIG_INPUT_IMAGE),
                             'foo1.png')
            self.assertEqual(config.get('2', CHMJobCreator.CONFIG_ARGS),
                             '-t 2,3')
            self.assertEqual(config.get('2',
                                        CHMJobCreator.CONFIG_OUTPUT_IMAGE),
                             os.path.join(CHMJobCreator.TILES_DIR,
                                          'foo1.png', '002.foo1.png'))

            self.assertEqual(config.get('2', CHMJobCreator.CONFIG_MODEL),
                             'model')
            self.assertEqual(config.get('2',
                                        CHMJobCreator.
                                        CONFIG_DISABLE_HISTEQ_IMAGES),
                             'True')
            self.assertEqual(config.get('2',
                                        CHMJobCreator.CONFIG_TASKS_PER_NODE),
                             '1')
        finally:
            shutil.rmtree(temp_dir)

    def test_create_job_three_images_five_tiles_per_job(self):
        temp_dir = tempfile.mkdtemp()
        try:
            image_dir = os.path.join(temp_dir, 'images')
            os.makedirs(image_dir, mode=0o775)
            foo1img = os.path.join(image_dir, 'foo1.png')
            self._create_png_image(foo1img, (400, 300))

            foo2img = os.path.join(image_dir, 'foo2.png')
            self._create_png_image(foo2img, (200, 100))

            foo3img = os.path.join(image_dir, 'foo3.png')
            self._create_png_image(foo3img, (800, 400))

            opts = CHMConfig(image_dir, 'model',
                             temp_dir, '200x100', '0x0',
                             number_tiles_per_task=5)
            creator = CHMJobCreator(opts)
            opts = creator.create_job()
            self.assertEqual(opts.get_out_dir(), temp_dir)
            self.assertEqual(opts.get_job_config(),
                             os.path.join(temp_dir,
                                          CHMJobCreator.CONFIG_FILE_NAME))

            config = configparser.ConfigParser()
            config.read(opts.get_job_config())
            self.assertEqual(len(config.sections()),
                             7)

            self.assertEqual(config.get('1',
                                        CHMJobCreator.CONFIG_OUTPUT_IMAGE),
                             os.path.join(CHMJobCreator.TILES_DIR,
                                          'foo1.png', '001.foo1.png'))
            self.assertTrue(os.path.isdir(os.path.join(temp_dir,
                                                       CHMJobCreator.RUN_DIR,
                                                       CHMJobCreator.TILES_DIR,
                                                       'foo1.png')))
            self.assertEqual(config.get('3',
                                        CHMJobCreator.CONFIG_OUTPUT_IMAGE),
                             os.path.join(CHMJobCreator.TILES_DIR,
                                          'foo2.png', '001.foo2.png'))
            self.assertTrue(os.path.isdir(os.path.join(temp_dir,
                                                       CHMJobCreator.RUN_DIR,
                                                       CHMJobCreator.TILES_DIR,
                                                       'foo2.png')))

            self.assertEqual(config.get('7',
                                        CHMJobCreator.CONFIG_OUTPUT_IMAGE),
                             os.path.join(CHMJobCreator.TILES_DIR,
                                          'foo3.png', '004.foo3.png'))
            self.assertTrue(os.path.isdir(os.path.join(temp_dir,
                                                       CHMJobCreator.RUN_DIR,
                                                       CHMJobCreator.TILES_DIR,
                                                       'foo3.png')))
        finally:
            shutil.rmtree(temp_dir)


if __name__ == '__main__':
    unittest.main()
