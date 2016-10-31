#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_chmutil
----------------------------------

Tests for `chmutil` module.
"""


import sys
import unittest
import tempfile
import shutil
import os

from chmutil.core import CHMJobCreator
from chmutil.core import CHMOpts



class TestCHMJobCreator(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_create_config_and_write_config(self):
        temp_dir = tempfile.mkdtemp()
        try:
            opts = CHMOpts('/foo', 'model', temp_dir, '200x100',
                           '20x20',
                           jobs_per_node=20)
            creator = CHMJobCreator(opts)
            con = creator._create_config()

            self.assertEqual(con.get('DEFAULT', 'model'), 'model')
            self.assertEqual(con.get('DEFAULT', 'tilesperjob'), '1')
            self.assertEqual(con.get('DEFAULT', 'tilesize'), '200x100')
            self.assertEqual(con.get('DEFAULT', 'overlapsize'), '20x20')
            self.assertEqual(con.get('DEFAULT', 'disablehisteqimages'), 'True')
            self.assertEqual(con.get('DEFAULT', 'jobspernode'), '20')
            cfile = creator._write_config(con)
            self.assertEqual(os.path.isfile(cfile), True)

        finally:
            shutil.rmtree(temp_dir)





