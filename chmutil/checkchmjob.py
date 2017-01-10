#! /usr/bin/env python

import sys
import os
import argparse
import logging
import chmutil

from chmutil.core import CHMConfigFromConfigFactory
from chmutil.cluster import ClusterFactory
from chmutil.cluster import BatchedTasksListGenerator
from chmutil.core import Parameters
from chmutil.cluster import CHMTaskChecker
from chmutil.cluster import MergeTaskChecker
from chmutil.core import CHMJobCreator
from chmutil.cluster import TaskSummaryFactory
from chmutil import core


# create logger
logger = logging.getLogger('chmutil.runchmjob')

SUBMIT = 'submit'
SUBMIT_FLAG = '--' + SUBMIT
DETAILED = 'detailed'
DETAILED_FLAG = '--' + DETAILED


def _parse_arguments(desc, args):
    """Parses command line arguments using argparse.
    """
    parsed_arguments = Parameters()

    help_formatter = argparse.RawDescriptionHelpFormatter
    parser = argparse.ArgumentParser(description=desc,
                                     formatter_class=help_formatter)
    parser.add_argument("jobdir", help='Directory containing ' +
                                       CHMJobCreator.CONFIG_FILE_NAME +
                                       ' file')

    bmerge = CHMJobCreator.MERGE_CONFIG_BATCHED_TASKS_FILE_NAME
    batchchm = CHMJobCreator.CONFIG_BATCHED_TASKS_FILE_NAME
    parser.add_argument(SUBMIT_FLAG, action="store_true",
                        help='Rewrite {batchchm}'
                             'and {batchmerge} files with any'
                             'jobs that need to still be'
                             'processed. WARNING: Do NOT add this'
                             'flag if tasks are still running,'
                             'since key configuration files '
                             'will be '
                             'rewritten.'.format(batchchm=batchchm,
                                                 batchmerge=bmerge))

    parser.add_argument(DETAILED_FLAG, action="store_true",
                        help='Output detailed summary '
                             'information for job')
    parser.add_argument("--skipchm", action="store_true",
                        help='Skips examination of CHM jobs. This will'
                             'mean stats on CHM jobs will be invalid')
    parser.add_argument("--log", dest="loglevel", choices=['DEBUG',
                        'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help="Set the logging level (default WARNING)",
                        default='WARNING')
    parser.add_argument('--version', action='version',
                        version=('%(prog)s ' + chmutil.__version__))

    return parser.parse_args(args, namespace=parsed_arguments)


def _get_chmconfig(jobdir):
    """Gets chmconfig
    """
    cfac = CHMConfigFromConfigFactory(os.path.abspath(jobdir))
    return cfac.get_chmconfig(skip_loading_mergeconfig=False)


def _get_incompleted_chm_task_list(chmconfig):
    """Gets incompleted chm tasks
    """
    chm_checker = CHMTaskChecker(chmconfig)
    return chm_checker.get_incomplete_tasks_list()


def _get_incompleted_merge_task_list(mergeconfig):
    """Gets incompleted merge tasks as list
    """
    merge_checker = MergeTaskChecker(mergeconfig)
    return merge_checker.get_incomplete_tasks_list()


def _submit_chm_tasks(batcher, config_file, task_list,
                      cluster):
    """submit CHM tasks
    """
    num_tasks = batcher.write_batched_config(config_file,
                                             task_list)
    sys.stdout.write('Run this:\n\n ' +
                     cluster.get_chm_submit_command(num_tasks) +
                     '\n\n')
    return 0


def _submit_merge_tasks(batcher, config_file, task_list,
                        cluster):
    """submit CHM tasks
    """
    num_tasks = batcher.write_batched_config(config_file,
                                             task_list)
    sys.stdout.write('Run this:\n\n ' +
                     cluster.get_merge_submit_command(num_tasks) +
                     '\n\n')
    return 0


def _submit(chmconfig, chm_task_list, merge_task_list):
    """Generates new configuration files and outputs commands
       to submit incomplete CHM and merge tasks
    """
    cfac = ClusterFactory()
    clust = cfac.get_cluster_by_name(chmconfig.get_cluster())

    if clust is None:
        logger.error('Cluster not supported: ' + chmconfig.get_cluster())
        return 2

    clust.set_chmconfig(chmconfig)

    num_chm_tasks = len(chm_task_list)
    if num_chm_tasks > 0:
        batcher = BatchedTasksListGenerator(chmconfig.
                                            get_number_tasks_per_node())
        logger.info('Found ' + str(num_chm_tasks) +
                    ' CHM tasks that need submission')
        chm_con_file = chmconfig.get_batchedjob_config_file_path()
        logger.info('Batched config file path: ' + chm_con_file)
        return _submit_chm_tasks(batcher, chm_con_file, chm_task_list, clust)

    num_merge_tasks = len(merge_task_list)
    if num_merge_tasks > 0:
        # TODO modify code to write these out even on incomplete job
        # TODO with any merge jobs that CAN be safely run
        batcher = BatchedTasksListGenerator(chmconfig.
                                            get_number_merge_tasks_per_node())
        logger.info('Found ' + str(num_merge_tasks) +
                    ' Merge tasks that need submission')
        mer_con_file = chmconfig.get_batched_mergejob_config_file_path()
        logger.info('Batched config file path: ' + mer_con_file)
        return _submit_merge_tasks(batcher, mer_con_file, merge_task_list,
                                   clust)

    sys.stdout.write('\nAll jobs completed. Have a nice day!\n\n')
    return 0


def _check_chm_job(theargs):
    """Runs all jobs for task
    """
    sys.stdout.write('\nAnalyzing job. This may take a minute...\n\n')

    chmconfig = _get_chmconfig(theargs.jobdir)
    if theargs.skipchm is False:
        chm_task_list = _get_incompleted_chm_task_list(chmconfig.get_config())
    else:
        logger.info("--skipchm set to True. Skipping examination of CHM jobs.")
        chm_task_list = []

    merge_task_list = _get_incompleted_merge_task_list(chmconfig.
                                                       get_merge_config())

    tsf = TaskSummaryFactory(chmconfig, chm_incomplete_tasks=chm_task_list,
                             merge_incomplete_tasks=merge_task_list)
    ts = tsf.get_task_summary()

    sys.stdout.write(ts.get_summary() + '\n')

    if theargs.detailed is True:
        logger.info(DETAILED_FLAG + ' set')
        sys.stdout.write('\nUnfortunately detailed report has '
                         'not yet been implemented\n\n')

    if theargs.submit is True:
        logger.info(SUBMIT_FLAG + ' set')
        return _submit(chmconfig, chm_task_list, merge_task_list)
    return 0


def main(arglist):
    """Main function
    :param arglist: Should be set to sys.argv which is list of arguments
                    passed on commandline including script being run as arg 0
    :returns: exit code. 0 is success otherwise failure
    """
    desc = """
              Version {version}

              Examines a CHM job generated by createchmjob.py to see
              if any tasks still need to be run. When running in
              default mode this tool ONLY outputs a job summary.

              TO update configuration to submit processing be sure to
              add {submit} flag.

              NOTE: CHM requires TWO phases of processing.

              For more information on these phases please run:

              createchmjob.py --help

              for more information.

              This tool examines BOTH phases of processing.

              For the FIRST phase this tool examines the
              <jobdir>/{run_dir}/{tiles}/<image dirs> directories and
              verifies image files exist for every tile listed in the
              <jobdir>/{chmconfig} config file.

              For the SECOND phase, this tool examines the
              <jobdir>/{run_dir}/{probmaps} directory
              and verifies existance of final probability maps for
              each input image.

              NOTE: It is assumed no active tasks are running on this CHM job.

              Example usage default:

              checkchmjob.py /foo/somechmjob

              chmutil version: {version}
              Tiles: 500x500 with 20x20 overlap
              Disable histogram equalization in CHM: True
              Jobs: 50 tiles per job, 1 job per node
              Trained CHM model: /foo/somemodel
              CHM binary: /foo/chm.img

              CHM tasks: 4% complete (960 of 23,456 completed)
              Merge tasks: 0% complete (0 of 1,234 completed)
              """.format(version=chmutil.__version__,
                         run_dir=CHMJobCreator.RUN_DIR,
                         chmconfig=CHMJobCreator.CONFIG_FILE_NAME,
                         batchchm=CHMJobCreator.CONFIG_BATCHED_TASKS_FILE_NAME,
                         batchmerge=CHMJobCreator.
                         MERGE_CONFIG_BATCHED_TASKS_FILE_NAME,
                         probmaps=CHMJobCreator.PROBMAPS_DIR,
                         tiles=CHMJobCreator.TILES_DIR,
                         submit=SUBMIT_FLAG,
                         detailed=DETAILED_FLAG)

    theargs = _parse_arguments(desc, arglist[1:])
    theargs.program = arglist[0]
    theargs.version = chmutil.__version__
    core.setup_logging(logger, loglevel=theargs.loglevel)
    try:
        return _check_chm_job(theargs)
    finally:
        logging.shutdown()


if __name__ == '__main__':  # pragma: no cover
    sys.exit(main(sys.argv))
