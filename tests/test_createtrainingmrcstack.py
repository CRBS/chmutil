#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_createtrainingmrcstack.py
----------------------------------

Tests for `createtrainingmrcstack.py`
"""

import unittest
import os
import tempfile
import shutil
from PIL import Image

from chmutil.core import Parameters
from chmutil import createtrainingmrcstack
from chmutil.createtrainingmrcstack import NoInputImageFoundError


class TestCreateTrainingMRCStack(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_parse_arguments(self):
        pargs = createtrainingmrcstack._parse_arguments('hi', ['idir',
                                                               '5', 'outdir'])
        self.assertEqual(pargs.imagedir, 'idir')
        self.assertEqual(pargs.numtiles, 5)
        self.assertEqual(pargs.output, 'outdir')

        self.assertEqual(pargs.scratchdir, '/tmp')
        self.assertEqual(pargs.suffix, '.png')

        self.assertEqual(pargs.loglevel, 'WARNING')

    def test_create_mrc_stack_no_images_found(self):
        temp_dir = tempfile.mkdtemp()
        try:
            params = Parameters()
            params.suffix = '.png'
            dest_file = os.path.join(temp_dir, 'foo.mrc')
            res = createtrainingmrcstack._create_mrc_stack(temp_dir, 2,
                                                           dest_file,
                                                           params)
            self.assertEqual(res, 1)
        finally:
            shutil.rmtree(temp_dir)


if __name__ == '__main__':
    unittest.main()
