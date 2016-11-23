#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_roccesubmitscriptgenerator
----------------------------------

Tests for `RocceSubmitScriptGenerator` class
"""

import os
import unittest
import tempfile
import shutil

from chmutil.core import CHMConfig
from chmutil.cluster import RocceCluster


class TestRocceSubmitScriptGenerator(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_get_submit_script_path(self):
        opts = CHMConfig('images', 'model', 'out',
                         '500x500', '20x20')
        gen = RocceCluster(None)
        val = gen._get_submit_script_path()
        self.assertEqual(val,
                         RocceCluster.SUBMIT_SCRIPT_NAME)
        gen = RocceCluster(opts)

        val = gen._get_submit_script_path()
        script = os.path.join(opts.get_out_dir(),
                              RocceCluster.SUBMIT_SCRIPT_NAME)
        self.assertEqual(val, script)

    def test_get_chm_runner_path(self):
        gen = RocceCluster(None)
        self.assertEqual(gen._get_chm_runner_path(),
                         RocceCluster.CHMRUNNER)

        opts = CHMConfig('images', 'model', 'out',
                         '500x500', '20x20')
        gen = RocceCluster(opts)
        self.assertEqual(gen._get_chm_runner_path(),
                         RocceCluster.CHMRUNNER)

        opts = CHMConfig('images', 'model', 'out',
                         '500x500', '20x20',
                         scriptbin='/home/foo/.local/bin')
        gen = RocceCluster(opts)
        spath = os.path.join('/home/foo/.local/bin',
                             RocceCluster.CHMRUNNER)
        self.assertEqual(gen._get_chm_runner_path(),
                         spath)

    def test_generate_submit_script(self):
        temp_dir = tempfile.mkdtemp()
        try:
            opts = CHMConfig('images', 'model', temp_dir,
                             '500x500', '20x20')
            gen = RocceCluster(opts)
            script = gen._get_submit_script_path()
            self.assertEqual(os.path.isfile(script), False)
            gen.generate_submit_script()
            self.assertEqual(os.path.isfile(script), True)
            # TODO Test qsub script file has correct data in it
        finally:
            shutil.rmtree(temp_dir)


if __name__ == '__main__':
    unittest.main()
