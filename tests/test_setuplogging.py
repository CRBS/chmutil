#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_setuplogging
----------------------------------

Tests for `setup_logging in core`
"""

import unittest
import logging

from chmutil import core


class TestCore(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_setup_logging_defaults(self):
        mylogger = logging.getLogger('fooey')
        core.setup_logging(mylogger)

        self.assertEqual(mylogger.getEffectiveLevel(),
                         logging.WARNING)

        clogger = logging.getLogger('chmutil.core')
        self.assertEqual(clogger.getEffectiveLevel(),
                         logging.WARNING)

        clusterlogger = logging.getLogger('chmutil.cluster')
        self.assertEqual(clusterlogger.getEffectiveLevel(),
                         logging.WARNING)

        core.setup_logging(mylogger, loglevel='DEBUG')
        self.assertEqual(mylogger.getEffectiveLevel(),
                         logging.DEBUG)

        clogger = logging.getLogger('chmutil.core')
        self.assertEqual(clogger.getEffectiveLevel(),
                         logging.DEBUG)

        clusterlogger = logging.getLogger('chmutil.cluster')
        self.assertEqual(clusterlogger.getEffectiveLevel(),
                         logging.DEBUG)

        core.setup_logging(mylogger, loglevel='INFO')
        self.assertEqual(mylogger.getEffectiveLevel(),
                         logging.INFO)

        core.setup_logging(mylogger, loglevel='ERROR')
        self.assertEqual(mylogger.getEffectiveLevel(),
                         logging.ERROR)

        core.setup_logging(mylogger, loglevel='CRITICAL')
        self.assertEqual(mylogger.getEffectiveLevel(),
                         logging.CRITICAL)


if __name__ == '__main__':
    unittest.main()
