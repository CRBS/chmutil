#! /usr/bin/env python

import sys
import os
import argparse
import logging
import chmutil

from chmutil.core import CHMConfigFromConfigFactory
from chmutil.cluster import ClusterFactory
from chmutil.cluster import BatchedJobsListGenerator
from chmutil.core import Parameters
from chmutil.cluster import CHMJobChecker
from chmutil import core


# create logger
logger = logging.getLogger('chmutil.runchmjob')


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
                        choices=['rocce'],
                        default='rocce')
    parser.add_argument("--log", dest="loglevel", choices=['DEBUG',
                        'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help="Set the logging level (default WARNING)",
                        default='WARNING')
    parser.add_argument('--version', action='version',
                        version=('%(prog)s ' + chmutil.__version__))

    return parser.parse_args(args, namespace=parsed_arguments)


def _run_chm_job(theargs):
    """Runs all jobs for task
    """
    # TODO refactor this cause its too hard to test
    cfac = CHMConfigFromConfigFactory(os.path.abspath(theargs.jobdir))
    chmconfig = cfac.get_chmconfig()
    checker = CHMJobChecker(chmconfig.get_config())
    gen = BatchedJobsListGenerator(checker,
                                   chmconfig.get_number_jobs_per_node())
    num_jobs = gen.\
        generate_batched_jobs_list(chmconfig.
                                   get_batchedjob_config_file_path())

    cfac = ClusterFactory()
    clust = cfac.get_cluster_by_name(theargs.cluster)

    if clust is None:
        logger.error('Cluster not supported: ' + theargs.cluster)
        return 2

    clust.set_chmconfig(chmconfig)
    if num_jobs is 0:
        sys.stdout.write('\nNo chm jobs need to be run\n\n')
        runmerge = os.path.join(chmconfig.get_script_bin(), 'runmergejob.py')
        sys.stdout.write('Run this to submit merge job\n' +
                         '  ' + runmerge + ' "' + chmconfig.get_out_dir() +
                         '" --cluster ' +
                         clust.get_cluster() + '\n')
        return 0

    sys.stdout.write('Run this:\n\n ' +
                     clust.get_chm_submit_command(num_jobs) +
                     '\n\n')
    return 0




def main(arglist):
    """Main function
    :param arglist: Should be set to sys.argv which is list of arguments
                    passed on commandline including script being run as arg 0
    :returns: exit code. 0 is success otherwise failure
    """
    desc = """
              Version {version}

              Submits chm job to cluster.


              Example Usage:

              runchmjob.py /foo/somechmjob --cluster rocce
              """.format(version=chmutil.__version__)

    theargs = _parse_arguments(desc, arglist[1:])
    theargs.program = arglist[0]
    theargs.version = chmutil.__version__
    core.setup_logging(logger, loglevel=theargs.loglevel)
    try:
        return _run_chm_job(theargs)
    finally:
        logging.shutdown()


if __name__ == '__main__':  # pragma: no cover
    sys.exit(main(sys.argv))
