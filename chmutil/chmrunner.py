#! /usr/bin/env python

import sys
import os
import argparse
import logging
import uuid
import configparser
import subprocess
import shlex
import shutil
import chmutil

from chmutil.core import CHMJobCreator
from chmutil.core import CHMConfig


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


def _run_external_command(cmd_to_run):
    """Runs command
    """
    # TODO MOVE THIS INTO A SEPARATE RUN CLASS
    if cmd_to_run is None:
        return 256, '', 'Command must be set'

    logger.info("Running command " + cmd_to_run)

    p = subprocess.Popen(shlex.split(cmd_to_run),
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)

    out, err = p.communicate()
    return p.returncode, out, err


def _run_chm_job(theargs):
    """runs CHM Job
    :param theargs: list of arguments obtained from _parse_arguments()
    :returns: exit code for program. 0 success otherwise failure
    """
    # TODO REFACTOR THIS INTO FACTORY CLASS TO GET CONFIG
    # TODO REFACTOR THIS INTO CLASS TO GENERATE CHM JOB COMMAND
    try:
        out_dir = os.path.join(theargs.scratchdir, uuid.uuid4().hex)
        config = configparser.ConfigParser()
        config.read(os.path.join(theargs.jobdir,
                    CHMJobCreator.CONFIG_FILE_NAME))
        input_image = config.get(theargs.taskid,
                                 CHMJobCreator.CONFIG_INPUT_IMAGE)
        logger.debug('Creating directory ' + out_dir)
        os.makedirs(out_dir, mode=0775)
        if config.get(theargs.taskid, CHMJobCreator.CONFIG_DISABLE_HISTEQ_IMAGES) == 'True':
            histeq_flag = ' -h '
        else:
            histeq_flag = ' '

        cmd = (config.get('DEFAULT', CHMJobCreator.CONFIG_CHM_BIN) + ' test ' +
               input_image +
               ' ' + out_dir + ' -m ' +
               config.get(theargs.taskid, CHMJobCreator.CONFIG_MODEL) +
               ' -b ' +
               config.get(theargs.taskid, CHMJobCreator.CONFIG_TILE_SIZE) +
               ' -o ' +
               config.get(theargs.taskid, CHMJobCreator.CONFIG_OVERLAP_SIZE) +
               histeq_flag + ' ' +
               config.get(theargs.taskid, CHMJobCreator.CONFIG_ARGS))
        exitcode, out, err = _run_external_command(cmd)

        sys.stdout.write(out)
        sys.stderr.write(err)
        sys.stdout.flush()
        sys.stderr.flush()

        prob_map = os.path.join(out_dir, os.path.basename(input_image))
        if os.path.isfile(prob_map) is False:
            logger.error('Result file missing : ' + prob_map)
            return 2

        shutil.move(prob_map,
                    config.get(theargs.taskid,
                    CHMJobCreator.CONFIG_OUTPUT_IMAGE))
        shutil.rmtree(out_dir)

        return exitcode
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

              Runs CHM for <taskid> specified on command
              line.


              Example Usage:

              chmrunner.py 1 /foo/chmjob --scratchdir /scratch

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
