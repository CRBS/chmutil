#! /usr/bin/env python

import sys
import os
import argparse
import logging
import chmutil

from chmutil.core import CHMConfigFromConfigFactory
from chmutil.cluster import RocceCluster
from chmutil.cluster import BatchedJobsListGenerator
from chmutil.core import Parameters
from chmutil.cluster import MergeJobChecker
from chmutil import core


# create logger
logger = logging.getLogger('chmutil.runmergejob')


def _parse_arguments(desc, args):
    """Parses command line arguments using argparse.
    """
    parsed_arguments = Parameters()

    help_formatter = argparse.RawDescriptionHelpFormatter
    parser = argparse.ArgumentParser(description=desc,
                                     formatter_class=help_formatter)
    parser.add_argument("jobdir", help='Directory containing chm.list.job'
                                       'file')
    parser.add_argument("--cluster", help='Cluster job is to run on '
                                          '(default rocce)',
                        default='rocce')
    parser.add_argument("--log", dest="loglevel", choices=['DEBUG',
                        'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help="Set the logging level (default WARNING)",
                        default='WARNING')
    parser.add_argument('--version', action='version',
                        version=('%(prog)s ' + chmutil.__version__))

    return parser.parse_args(args, namespace=parsed_arguments)


def _run_merge_job(theargs):
    """Runs all jobs for task
    """
    cfac = CHMConfigFromConfigFactory(os.path.abspath(theargs.jobdir))
    chmconfig = cfac.get_chmconfig(skip_loading_mergeconfig=False)
    checker = MergeJobChecker(chmconfig.get_merge_config())
    gen = BatchedJobsListGenerator(checker,
                                   chmconfig.get_number_jobs_per_node())
    num_jobs = gen.\
        generate_batched_jobs_list(chmconfig.
                                   get_batched_mergejob_config_file_path())
    if num_jobs is 0:
        sys.stdout.write('\nNo jobs need to be run\n\n')
        return 0

    if theargs.cluster == 'rocce':
        clust = RocceCluster(chmconfig)

    if clust is not None:
        sys.stdout.write('Run this:\n\n ' +
                         clust.get_merge_submit_command(num_jobs) +
                         '\n\n')
        return 0

    logger.error('Cluster not supported: ' + theargs.cluster)
    return 2


def main(arglist):
    """Main function
    :param arglist: Should be set to sys.argv which is list of arguments
                    passed on commandline including script being run as arg 0
    :returns: exit code. 0 is success otherwise failure
    """
    desc = """
              Version {version}

              Submits merge jobs to cluster.


              Example Usage:

              runmergejob.py /foo/somechmjob --cluster rocce
              """.format(version=chmutil.__version__)

    theargs = _parse_arguments(desc, arglist[1:])
    theargs.program = arglist[0]
    theargs.version = chmutil.__version__
    core.setup_logging(logger, loglevel=theargs.loglevel)
    try:
        return _run_merge_job(theargs)
    finally:
        logging.shutdown()


if __name__ == '__main__':  # pragma: no cover
    sys.exit(main(sys.argv))
