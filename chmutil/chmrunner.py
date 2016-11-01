#! /usr/bin/env python

import sys
import argparse
import logging
import chmutil

from chmutil.core import CHMJobCreator
from chmutil.core import CHMOpts


LOG_FORMAT = "%(asctime)-15s %(levelname)s %(name)s %(message)s"

# create logger
logger = logging.getLogger('chmutil.chmrunner')



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

    logging.getLogger('chmutil.chmrunner').setLevel(theargs.numericloglevel)
    logging.getLogger('chmutil.core').setLevel(theargs.numericloglevel)


def _parse_arguments(desc, args):
    """Parses command line arguments using argparse.
    """
    parsed_arguments = Parameters()

    help_formatter = argparse.RawDescriptionHelpFormatter
    parser = argparse.ArgumentParser(description=desc,
                                     formatter_class=help_formatter)
    parser.add_argument("jobid", help='Job id')
    parser.add_argument("taskid", help='Task id')
    parser.add_argument("jobdir", help='Directory containing chm.list.job'
                                       'file')
    parser.add_argument("--scratchdir", help='Scratch Directory (default /tmp)',
                        default='/tmp')
    parser.add_argument("--log", dest="loglevel", choices=['DEBUG',
                        'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help="Set the logging level (default WARNING)",
                        default='WARNING')
    parser.add_argument('--version', action='version',
                        version=('%(prog)s ' + chmutil.__version__))

    return parser.parse_args(args, namespace=parsed_arguments)


def _run_chm_job(theargs):
    """Creates CHM Job
    :param theargs: list of arguments obtained from _parse_arguments()
    :returns: exit code for program. 0 success otherwise failure
    """
    try:
        opts = CHMOpts(theargs.images, theargs.model,
                                theargs.outdir,
                                tilesize=theargs.tilesize,
                                overlapsize=theargs.overlapsize,
                                disablehisteq=theargs.disablechmhisteq,
                                tilesperjob=theargs.tilesperjob,
                                jobspernode=theargs.jobspernode)
        creator = CHMJobCreator(opts)
        job = creator.create_job()

        # ssfac = SubmitScriptGeneratorFactory(opts)
        # ss = ssfac.get_submit_script_generator_for_cluster()
        # ss.generateSubmitScript()
        # sys.stdout.write(ss.get_run_instructions())

        return 0
    except Exception:
        logger.exception("Error caught exception")
        return 2


def main(arglist):
    """Main function
    :param arglist: Should be set to sys.argv which is list of arguments
                    passed on commandline including script being run as arg 0
    :returns: exit code. 0 is success otherwise failure
    """
    desc = """
              Version {version}

              Runs CHM for <jobid> and <taskid> specified on command
              line.


              Example Usage:

              chmrunner.py 12345 1 /foo/chmjob /scratch

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
