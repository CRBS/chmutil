#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_clusterfactory
----------------------------------

Tests for `ClusterFactory in cluster`
"""

import unittest

from chmutil.cluster import ClusterFactory
from chmutil.cluster import RocceCluster
from chmutil.cluster import GordonCluster
from chmutil.cluster import CometCluster


class TestClusterFactory(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_get_cluster_by_name(self):
        cfac = ClusterFactory()

        self.assertEqual(cfac.get_cluster_by_name(None), None)
        self.assertEqual(cfac.get_cluster_by_name('doesnotexist'), None)

        # verify rocce is returned
        c = cfac.get_cluster_by_name(RocceCluster.CLUSTER)
        self.assertEqual(c.get_cluster(), RocceCluster.CLUSTER)

        # verify gordon is returned
        c = cfac.get_cluster_by_name(GordonCluster.CLUSTER)
        self.assertEqual(c.get_cluster(), GordonCluster.CLUSTER)

        # verify case insensitive
        c = cfac.get_cluster_by_name(GordonCluster.CLUSTER.upper())
        self.assertEqual(c.get_cluster(), GordonCluster.CLUSTER)

        # verify comet is returned
        c = cfac.get_cluster_by_name(CometCluster.CLUSTER)
        self.assertEqual(c.get_cluster(), CometCluster.CLUSTER)

if __name__ == '__main__':
    unittest.main()
