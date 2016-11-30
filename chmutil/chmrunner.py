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
from chmutil.core import SingularityAbortError
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


def _run_chm_job(theargs):
    """Runs all jobs for task
    :raises LoadConfigError: if no config is found in job dir
    :returns: status of `_run_jobs` call 0 for success otherwise error
    """
    cfac = CHMConfigFromConfigFactory(theargs.jobdir)
    chmconfig = cfac.get_chmconfig()
    return _run_jobs(chmconfig, theargs, theargs.taskid)


def _run_jobs(chmconfig, theargs, taskid):
    """Runs jobs for task in parallel
    """
    # TODO Switch to using multiprocessing.Process
    bconfig = configparser.ConfigParser()
    bconfig.read(chmconfig.get_batchedjob_config_file_path())

    config = configparser.ConfigParser()
    config.read(os.path.join(theargs.jobdir,
                             CHMJobCreator.CONFIG_FILE_NAME))

    tasks = bconfig.get(taskid, CHMJobCreator.BCONFIG_TASK_ID).split(',')
    process_list = []
    logger.debug('Running ' + str(len(tasks)) + ' child processes')
    for t in tasks:
        pid = os.fork()
        if pid is 0:
            logger.debug('In child submitting job to run task ' + t)
            try:
                return _run_single_chm_job(theargs.scratchdir, t, config)
            except SingularityAbortError:
                logger.exception('Caught SingularityAbortError, retrying job')
                return _run_single_chm_job(theargs.scratchdir, t, config)
            except Exception:
                logger.exception('Caught exception')
                return 2
        else:
            logger.debug('Appending child process to list: ' + str(pid))
            process_list.append(pid)

    return core.wait_for_children_to_exit(process_list)


def _run_single_chm_job(scratchdir, taskid, config):
    """runs CHM Job
    :param scratchdir: temp directory
    :returns: exit code for program. 0 success otherwise failure
    """
    # TODO REFACTOR THIS INTO FACTORY CLASS TO GET CONFIG
    # TODO REFACTOR THIS INTO CLASS TO GENERATE CHM JOB COMMAND
    out_dir = None
    try:
        # TODO try using JOB_ID and TASK_ID environment variables first and
        # TODO then fallback to uuid for directory name
        out_dir = os.path.join(scratchdir, uuid.uuid4().hex)
        input_image = config.get(taskid,
                                 CHMJobCreator.CONFIG_INPUT_IMAGE)
        logger.debug('Creating directory ' + out_dir)
        os.makedirs(out_dir, mode=0o775)
        if config.get(taskid, CHMJobCreator.
                      CONFIG_DISABLE_HISTEQ_IMAGES) == 'True':
            histeq_flag = ' -h '
        else:
            histeq_flag = ' '

        cmd = ('"' + config.get('DEFAULT', CHMJobCreator.CONFIG_CHM_BIN) +
               '" test "' + input_image + '" ' + out_dir + ' -m "' +
               config.get(taskid, CHMJobCreator.CONFIG_MODEL) +
               '" -b ' +
               config.get(taskid, CHMJobCreator.CONFIG_TILE_SIZE) +
               ' -o ' +
               config.get(taskid, CHMJobCreator.CONFIG_OVERLAP_SIZE) +
               histeq_flag + ' ' +
               config.get(taskid, CHMJobCreator.CONFIG_ARGS))
        exitcode, out, err = core.run_external_command(cmd, out_dir)

        sys.stdout.write(out)
        sys.stderr.write(err)
        sys.stdout.flush()
        sys.stderr.flush()
        logger.info('Job has completed with exit code: ' + str(exitcode))

        prob_map = os.path.join(out_dir, os.path.basename(input_image))
        if os.path.isfile(prob_map) is False:
            logger.error('Result file missing : ' + prob_map)
            # this handles case where singularity pukes cause the
            # directory under /tmp already exists
            if 'ABORT: Could not create temporary directory /tmp' in err:
                raise SingularityAbortError(err)
            if 'ABORT: Could not create directory /tmp' in err:
                raise SingularityAbortError(err)
            return 3

        out_image = config.get(taskid,
                               CHMJobCreator.CONFIG_OUTPUT_IMAGE)

        logger.debug('Copying image ' + prob_map +
                     ' to final destination: ' +
                     out_image)

        shutil.move(prob_map, out_image)

        return exitcode
    finally:
        if out_dir is not None:
            if os.path.isdir(out_dir):
                logger.debug('Removing directory: ' + out_dir)
                shutil.rmtree(out_dir)


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
    core.setup_logging(logger, log_format=LOG_FORMAT,
                       loglevel=theargs.loglevel)
    try:
        return _run_chm_job(theargs)
    finally:
        logging.shutdown()


if __name__ == '__main__':  # pragma: no cover
    sys.exit(main(sys.argv))
