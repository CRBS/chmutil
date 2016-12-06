#! /usr/bin/env python

import sys
import os
import argparse
import logging
import uuid
import configparser
import shutil
import chmutil

from chmutil.core import CHMJobCreator
from chmutil.core import CHMConfigFromConfigFactory
from chmutil.core import Parameters
from chmutil import core

LOG_FORMAT = "%(asctime)-15s %(levelname)s (%(process)d) %(name)s %(message)s"

# create logger
logger = logging.getLogger('chmutil.chmrunner')


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

    core.add_standard_parameters(parser)

    return parser.parse_args(args, namespace=parsed_arguments)


def _run_merge_job(theargs):
    """Runs all jobs for task
    """
    cfac = CHMConfigFromConfigFactory(theargs.jobdir)
    chmconfig = cfac.get_chmconfig(skip_loading_config=True,
                                   skip_loading_mergeconfig=False)
    if not os.path.isfile(chmconfig.get_batched_mergejob_config_file_path()):
        logger.error('No batched merge config found: ' +
                     chmconfig.get_batched_mergejob_config_file_path())
        return 1
    return _run_jobs(chmconfig, theargs, theargs.taskid)


def _run_jobs(chmconfig, theargs, taskid):
    """Runs jobs for task in parallel
    """
    bconfig = configparser.ConfigParser()
    bconfig.read(chmconfig.get_batched_mergejob_config_file_path())
    tasks = bconfig.get(taskid, CHMJobCreator.BCONFIG_TASK_ID).split(',')
    process_list = []
    logger.debug('Running ' + str(len(tasks)) + 'child processes')
    for t in tasks:
        pid = os.fork()
        if pid is 0:
            logger.debug('In child submitting job to run task ' + t)
            return _run_single_merge_job(theargs, t)
        else:
            logger.debug('Appending child process to list: ' + str(pid))
            process_list.append(pid)

    return core.wait_for_children_to_exit(process_list)


def _run_single_merge_job(theargs, taskid):
    """runs CHM Job
    :param theargs: list of arguments obtained from _parse_arguments()
    :returns: exit code for program. 0 success otherwise failure
    """
    # TODO REFACTOR THIS INTO FACTORY CLASS TO GET CONFIG
    # TODO REFACTOR THIS INTO CLASS TO GENERATE CHM JOB COMMAND
    out_dir = None
    try:
        out_dir = os.path.join(theargs.scratchdir, str(taskid) +
                               '.' + uuid.uuid4().hex)
        config = configparser.ConfigParser()
        config.read(os.path.join(theargs.jobdir,
                    CHMJobCreator.MERGE_CONFIG_FILE_NAME))
        thebin = config.get(taskid, CHMJobCreator.MERGE_MERGETILES_BIN)

        input_dir = config.get(taskid,
                               CHMJobCreator.MERGE_INPUT_IMAGE_DIR)
        # TODO TEST that relative paths work with MERGE phase
        if not input_dir.startswith('/'):
            input_dir = os.path.join(theargs.jobdir, CHMJobCreator.RUN_DIR,
                                     input_dir)

        out_file = config.get(taskid,
                              CHMJobCreator.MERGE_OUTPUT_IMAGE)

        if not out_file.startswith('/'):
            out_file = os.path.join(theargs.jobdir, CHMJobCreator.RUN_DIR,
                                    out_file)

        logger.debug('Creating directory ' + out_dir)
        os.makedirs(out_dir, mode=0o775)
        cmd = (thebin + ' ' +
               input_dir + ' ' + out_file + ' --suffix png --log DEBUG')
        exitcode, out, err = core.run_external_command(cmd, out_dir)

        sys.stdout.write(out)
        sys.stderr.write(err)
        sys.stdout.flush()
        sys.stderr.flush()
        return exitcode
    except Exception:
        logger.exception("Error caught exception")
        return 2
    finally:
        if out_dir is not None:
            if os.path.isdir(out_dir):
                shutil.rmtree(out_dir)


def main(arglist):
    """Main function
    :param arglist: Should be set to sys.argv which is list of arguments
                    passed on commandline including script being run as arg 0
    :returns: exit code. 0 is success otherwise failure
    """
    desc = """
              Version {version}

              Runs Merge tiles for <taskid> specified on command
              line.


              Example Usage:

              mergetilerunner.py 1 /foo/chmjob --scratchdir /scratch

              """.format(version=chmutil.__version__)

    theargs = _parse_arguments(desc, arglist[1:])
    theargs.program = arglist[0]
    theargs.version = chmutil.__version__
    core.setup_logging(logger, log_format=LOG_FORMAT,
                       loglevel=theargs.loglevel)
    try:
        return _run_merge_job(theargs)
    finally:
        logging.shutdown()


if __name__ == '__main__':  # pragma: no cover
    sys.exit(main(sys.argv))
