#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_chmrunner
----------------------------------

Tests for `chmrunner.py`
"""

import unittest
import os
import tempfile
import shutil
import configparser

from chmutil import chmrunner
from chmutil.core import LoadConfigError
from chmutil.core import CHMJobCreator


class TestCHMRunner(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_parse_arguments(self):
        pargs = chmrunner._parse_arguments('hi', ['taskid', 'jobdir'])
        self.assertEqual(pargs.taskid, 'taskid')
        self.assertEqual(pargs.jobdir, 'jobdir')

    def test_run_chm_job_no_config(self):
        temp_dir = tempfile.mkdtemp()
        try:
            pargs = chmrunner._parse_arguments('hi', ['taskid', temp_dir])
            chmrunner._run_chm_job(pargs)
            self.fail('Expected LoadConfigError')
        except LoadConfigError as e:
            self.assertEqual(str(e),
                             os.path.join(temp_dir,
                                          CHMJobCreator.CONFIG_FILE_NAME) +
                             ' configuration file does not exist')
        finally:
            shutil.rmtree(temp_dir)




if __name__ == '__main__':
    unittest.main()
