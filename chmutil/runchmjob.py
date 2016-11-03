#! /usr/bin/env python

import sys
import os
import argparse
import logging
import chmutil

from chmutil.core import CHMConfigFromConfigFactory
from chmutil.cluster import RocceSubmitScriptGenerator
from chmutil.cluster import BatchedJobsListGenerator

LOG_FORMAT = "%(asctime)-15s %(levelname)s %(name)s %(message)s"

# create logger
logger = logging.getLogger('chmutil.runchmjob')


class Parameters(object):
    """Placeholder class for parameters
    """
    pass


def _setup_logging(theargs):
    """hi
    """
    theargs.logformat = LOG_FORMAT
    theargs.numericloglevel = logging.NOTSET
    if theargs.loglevel == 'DEBUG':
        theargs.numericloglevel = logging.DEBUG
    if theargs.loglevel == 'INFO':
        theargs.numericloglevel = logging.INFO
    if theargs.loglevel == 'WARNING':
        theargs.numericloglevel = logging.WARNING
    if theargs.loglevel == 'ERROR':
        theargs.numericloglevel = logging.ERROR
    if theargs.loglevel == 'CRITICAL':
        theargs.numericloglevel = logging.CRITICAL

    logger.setLevel(theargs.numericloglevel)
    logging.basicConfig(format=theargs.logformat)

    logging.getLogger('chmutil.runchmjob').setLevel(theargs.numericloglevel)
    logging.getLogger('chmutil.core').setLevel(theargs.numericloglevel)
    logging.getLogger('chmutil.cluster').setLevel(theargs.numericloglevel)


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


def _run_chm_job(theargs):
    """Runs all jobs for task
    """
    cfac = CHMConfigFromConfigFactory(os.path.abspath(theargs.jobdir))
    chmconfig = cfac.get_chmconfig()

    gen = BatchedJobsListGenerator(chmconfig)
    num_jobs = gen.generate_batched_jobs_list()
    if num_jobs is 0:
        sys.stdout.write('\nNo jobs need to be run\n\n')
        return 0

    if num_jobs > 1:
        num_jobs -= 1

    if theargs.cluster == 'rocce':
        sys.stdout.write('Run this:\n\n  cd ' + chmconfig.get_out_dir() + ';' +
                         'qsub -t 1-' + str(num_jobs) + ' ' +
                         RocceSubmitScriptGenerator.SUBMIT_SCRIPT_NAME +
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

              Submits chm job to cluster.


              Example Usage:

              runchmjob.py /foo/somechmjob --cluster rocce
              """.format(version=chmutil.__version__)

    theargs = _parse_arguments(desc, arglist[1:])
    theargs.program = arglist[0]
    theargs.version = chmutil.__version__
    _setup_logging(theargs)
    try:
        return _run_chm_job(theargs)
    finally:
        logging.shutdown()


if __name__ == '__main__':
    sys.exit(main(sys.argv))
