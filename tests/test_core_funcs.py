#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_core_funts
----------------------------------

Tests for functions in `core` module
"""

import os
import argparse
import unittest
import tempfile
import shutil
import stat

from chmutil.core import Parameters
from chmutil import core
from chmutil.core import InvalidImageDirError


class TestCoreFunctions(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_add_standard_parameters(self):
        p = Parameters()
        help_formatter = argparse.RawDescriptionHelpFormatter
        parser = argparse.ArgumentParser(description='hi',
                                         formatter_class=help_formatter)
        parser.add_argument("foo", help='foo help')
        core.add_standard_parameters(parser)

        parser.parse_args(['hi'], namespace=p)

        self.assertEqual(p.scratchdir, '/tmp')
        self.assertEqual(p.loglevel, 'WARNING')
        parser.parse_args(['hi', '--log', 'DEBUG',
                           '--scratchdir', 'yo'], namespace=p)
        self.assertEqual(p.scratchdir, 'yo')
        self.assertEqual(p.loglevel, 'DEBUG')

    def test_run_external_command_where_command_is_none(self):
        ecode, out, err = core.run_external_command(None, None)
        self.assertEqual(ecode, 256)
        self.assertEqual(out, '')
        self.assertEqual(err, 'Command must be set')

    def test_run_external_command_where_tmpdir_is_none_or_not_a_dir(self):
        ecode, out, err = core.run_external_command('foo', None)
        self.assertEqual(ecode, 255)
        self.assertEqual(out, '')
        self.assertEqual(err, 'Tmpdir must be set')

        temp_dir = tempfile.mkdtemp()
        try:
            notdir = os.path.join(temp_dir, 'blah')
            ecode, out, err = core.run_external_command('foo', notdir)
            self.assertEqual(ecode, 254)
            self.assertEqual(out, '')
            self.assertEqual(err, 'Tmpdir must be a directory')
        finally:
            shutil.rmtree(temp_dir)

    def test_run_external_command_success(self):
        temp_dir = tempfile.mkdtemp()
        try:
            fakecmd = os.path.join(temp_dir, 'fake.py')
            f = open(fakecmd, 'w')
            f.write('#!/usr/bin/env python\n\n')
            f.write('import sys\n')
            f.write('sys.stdout.write("somestdout")\n')
            f.write('sys.stderr.write("somestderr")\n')
            f.write('sys.exit(0)\n')
            f.flush()
            f.close()
            os.chmod(fakecmd, stat.S_IRWXU)

            ecode, out, err = core.run_external_command(fakecmd, temp_dir,
                                                        polling_sleep_time=0.1)
            self.assertEqual(ecode, 0)
            self.assertEqual(out, 'somestdout')
            self.assertEqual(err, 'somestderr')

        finally:
            shutil.rmtree(temp_dir)

    def test_run_external_command_fail_no_output(self):
        temp_dir = tempfile.mkdtemp()
        try:
            fakecmd = os.path.join(temp_dir, 'fake.py')
            f = open(fakecmd, 'w')
            f.write('#!/usr/bin/env python\n\n')
            f.write('import sys\n')
            f.write('sys.exit(1)\n')
            f.flush()
            f.close()
            os.chmod(fakecmd, stat.S_IRWXU)

            ecode, out, err = core.run_external_command(fakecmd, temp_dir,
                                                        polling_sleep_time=0.1)
            self.assertEqual(ecode, 1)
            self.assertEqual(out, '')
            self.assertEqual(err, '')

        finally:
            shutil.rmtree(temp_dir)

    def test_get_image_path_no_keysort_func(self):
        temp_dir = tempfile.mkdtemp()
        try:
            try:
                core.get_image_path_list(None, None)
                self.fail('Expected InvalidImageDirError')
            except InvalidImageDirError as e:
                self.assertEqual(str(e), 'image_dir is None')

            try:
                core.get_image_path_list(os.path.join(temp_dir, 'hi'), None)
                self.fail('Expected InvalidImageDirError')
            except InvalidImageDirError as e:
                self.assertEqual(str(e), 'image_dir must be a directory')

            # no files
            res = core.get_image_path_list(temp_dir, None)
            self.assertEqual(len(res), 0)

            # one file
            onefile = os.path.join(temp_dir, 'foo.txt')
            open(onefile, 'a').close()
            res = core.get_image_path_list(temp_dir, None)
            self.assertEqual(len(res), 1)
            self.assertTrue(onefile in res)

            # two files and a directory
            twofile = os.path.join(temp_dir, 'two.png')
            open(twofile, 'a').close()

            adir = os.path.join(temp_dir, 'somedir.png')
            os.makedirs(adir, mode=0o775)
            res = core.get_image_path_list(temp_dir, None)
            self.assertEqual(len(res), 2)
            self.assertTrue(onefile in res)
            self.assertTrue(twofile in res)

            # suffix set to .png
            res = core.get_image_path_list(temp_dir, '.png')
            self.assertEqual(len(res), 1)
            self.assertTrue(twofile in res)

            # verify case DOES matter
            threefile = os.path.join(temp_dir, '.PNG')
            open(threefile, 'a').close()
            res = core.get_image_path_list(temp_dir, '.png')
            self.assertEqual(len(res), 1)
            self.assertTrue(twofile in res)

            # try a 1,000 files for fun
            for v in range(0, 999):
                af = os.path.join(temp_dir, str(v) + '.png')
                open(af, 'a').close()

            res = core.get_image_path_list(temp_dir, '.png')
            self.assertEqual(len(res), 1000)
        finally:
            shutil.rmtree(temp_dir)

    def test_get_image_path_with_longest_number_keysort(self):
        temp_dir = tempfile.mkdtemp()
        try:
            func = core.get_longest_sequence_of_numbers_in_string
            # no files
            res = core.get_image_path_list(temp_dir, None,
                                           keysortfunc=func)
            self.assertEqual(len(res), 0)

            # one file
            onefile = os.path.join(temp_dir, 'foo.2345.txt')
            open(onefile, 'a').close()
            res = core.get_image_path_list(temp_dir, None,
                                           keysortfunc=func)
            self.assertEqual(len(res), 1)
            self.assertTrue(onefile in res)

            # two files and a directory
            twofile = os.path.join(temp_dir, 'foo.1234.png')
            open(twofile, 'a').close()

            adir = os.path.join(temp_dir, 'somedir.png')
            os.makedirs(adir, mode=0o775)
            res = core.get_image_path_list(temp_dir, None,
                                           keysortfunc=func)
            self.assertEqual(len(res), 2)
            self.assertTrue(onefile in res)
            self.assertTrue(twofile in res)
            self.assertEqual(res[0], twofile)

            # suffix set to .png
            res = core.get_image_path_list(temp_dir, '.png',
                                           keysortfunc=func)
            self.assertEqual(len(res), 1)
            self.assertTrue(twofile in res)

            # verify case DOES matter
            threefile = os.path.join(temp_dir, '.PNG')
            open(threefile, 'a').close()
            res = core.get_image_path_list(temp_dir, '.png')
            self.assertEqual(len(res), 1)
            self.assertTrue(twofile in res)

            # try a 1,000 files for fun
            for v in range(0, 999):
                af = os.path.join(temp_dir, str(v) + '.png')
                open(af, 'a').close()

            res = core.get_image_path_list(temp_dir, '.png',
                                           keysortfunc=func)
            self.assertEqual(len(res), 1000)
        finally:
            shutil.rmtree(temp_dir)

    def test_wait_for_children_to_exit(self):
        self.assertEqual(core.wait_for_children_to_exit(None), 0)
        self.assertEqual(core.wait_for_children_to_exit([]), 0)
        self.assertEqual(core.wait_for_children_to_exit([123, 456]), 0)

    def test_get_longest_sequence_of_numbers_in_string(self):
        self.assertEqual(core.get_longest_sequence_of_numbers_in_string(None),
                         0)
        self.assertEqual(core.get_longest_sequence_of_numbers_in_string(''),
                         0)
        self.assertEqual(core.get_longest_sequence_of_numbers_in_string('1'),
                         1)

        val = 'bin1-3view-final.0.png'
        self.assertEqual(core.get_longest_sequence_of_numbers_in_string(val),
                         1)

        val = 'bin1-3view-final.0635.png'
        self.assertEqual(core.get_longest_sequence_of_numbers_in_string(val),
                         635)

        val = '/foo/345643/bin1-3view-final.1635.png'
        self.assertEqual(core.get_longest_sequence_of_numbers_in_string(val),
                         1635)

    def test_get_first_sequence_of_numbers_in_string(self):
        self.assertEqual(core.get_first_sequence_of_numbers_in_string(None),
                         0)

        self.assertEqual(core.get_first_sequence_of_numbers_in_string(''),
                         0)

        self.assertEqual(core.get_first_sequence_of_numbers_in_string('4'),
                         4)

        val = 'bin1-3view-final.0635.png'
        self.assertEqual(core.get_first_sequence_of_numbers_in_string(val),
                         1)

        val = '023.bin1-3view-final.123132.png'
        self.assertEqual(core.get_first_sequence_of_numbers_in_string(val),
                         23)


if __name__ == '__main__':
    unittest.main()
