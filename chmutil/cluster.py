# -*- coding: utf-8 -*-


import os
import stat
import logging
import shutil
import configparser
from configparser import NoOptionError

from chmutil.core import CHMJobCreator

logger = logging.getLogger(__name__)


class InvalidConfigFileError(Exception):
    """Raised if config file path is invalid
    """
    pass


class InvalidTaskListError(Exception):
    """Raised if invalid task list is used
    """
    pass


class TaskStats(object):
    """Container object that holds information about
       runtime performance of a set of tasks
    """
    def __init__(self):
        """Constructor
        """
        self._completed_task_count = 0
        self._total_task_count = 0

    def set_completed_task_count(self, count):
        """sets number of completed tasks
        :param count: number of completed tasks
        """
        self._completed_task_count = count

    def get_completed_task_count(self):
        """gets number of completed tasks
        :returns: number of completed tasks
        """
        return self._completed_task_count

    def set_total_task_count(self, count):
        """sets total number of tasks
        :param count: total number of tasks
        """
        self._completed_task_count = count

    def get_total_task_count(self):
        """returns total tasks count
        """
        return self._total_task_count


class TaskSummary(object):
    """Summary of a CHM job
    """

    def __init__(self, chmconfig, chm_task_stats=None,
                 merge_task_stats=None):
        """Constructor
        """
        self._chmconfig = chmconfig
        self._chm_task_summary = self.\
            _get_summary_from_task_stats(chm_task_stats)
        self._merge_task_summary = self.\
            _get_summary_from_task_stats(merge_task_stats)

    def _get_summary_from_task_stats(self, task_stats):
        """Creates a summary string from `task_stats` object
        :param task_stats: TaskStats object
        :returns For valid `TaskStats` object this method will return a
                 string of form #% complete (# of # completed) otherwise
                 a string containing NA will be returned
        """
        if task_stats is None:
            return 'NA'

        total = task_stats.get_total_task_count()
        if total <= 0:
            return 'Total number of tasks is >= 0'

        completed = task_stats.get_completed_task_count()
        pc_complete_str = '{:.2%}'.format(completed/total)
        completed_str = '{:,}'.format(completed)
        total_str = '{:,}'.format(total)
        return (pc_complete_str + ' complete (' + completed_str + ' of ' +
                total_str + ' completed)')

    def get_summary(self):
        """Gets the summary of CHM job in human readable form
        """
        val = ('chmutil version: ' + self._chmconfig.get_version() + '\n' +
               'Tiles: ' + self._chmconfig.get_tile_size() + ' with ' +
               self._chmconfig.get_overlap_size() + ' overlap\n' +
               'Disable histogram equalization in CHM: ' +
               str(self._chmconfig.get_disable_histogram_eq_val()) + '\n' +
               'Tasks: ' + str(self._chmconfig.get_number_tiles_per_task()) +
               ' tiles per task, ' + str(self._chmconfig.get_jobs_per_node()) +
               ' tasks(s) per node\nTrained CHM model: ' +
               self._chmconfig.get_model() + '\nCHM binary: ' +
               self._chmconfig.get_chm_binary() + '\n\n' + 'CHM tasks: ' +
               self._chm_task_summary + '\nMerge tasks: ' +
               self._merge_task_summary + '\n')


class TaskSummaryFactory(object):
    """Examines CHM Job and creates JobSummary object
       which contains summary information about the
       job.
    """
    pass


class CHMTaskChecker(object):
    """Checks and returns incomplete CHM Jobs
    """
    def __init__(self, config):
        self._config = config

    def get_incomplete_tasks_list(self):
        """gets list of incomplete jobs
        """
        config = self._config
        task_list = []

        try:
            jobdir = config.get(CHMJobCreator.CONFIG_DEFAULT,
                                CHMJobCreator.JOB_DIR)
        except NoOptionError:
            logger.exception('No ' + CHMJobCreator.JOB_DIR +
                             ' in configuration')
            jobdir = None

        for s in config.sections():
            out_file = config.get(s, CHMJobCreator.CONFIG_OUTPUT_IMAGE)
            if not out_file.startswith('/') and jobdir is not None:
                out_file = os.path.join(jobdir, CHMJobCreator.RUN_DIR,
                                        out_file)
            if not os.path.isfile(out_file):
                task_list.append(s)

        logger.info('Found ' + str(len(task_list)) + ' of ' +
                    str(len(config.sections())) + ' to be incomplete tasks')
        return task_list


class MergeTaskChecker(object):
    """Checks and returns incomplete Merge Jobs
    """
    def __init__(self, config):
        """Constructor
        :param config: Should be `configparser.ConfigParser` object
                       loaded from Merge task configuration file
                       as obtained from `CHMConfig.get_merge_config()
        """
        self._config = config

    def get_incomplete_tasks_list(self):
        """gets list of incomplete jobs
        """
        config = self._config
        task_list = []

        try:
            jobdir = config.get(CHMJobCreator.CONFIG_DEFAULT,
                                CHMJobCreator.JOB_DIR)
        except NoOptionError:
            logger.exception('No ' + CHMJobCreator.JOB_DIR +
                             ' in configuration')
            jobdir = None

        for s in config.sections():
            out_file = config.get(s, CHMJobCreator.MERGE_OUTPUT_IMAGE)
            if not out_file.startswith('/') and jobdir is not None:
                out_file = os.path.join(jobdir, CHMJobCreator.RUN_DIR,
                                        out_file)
            if not os.path.isfile(out_file):
                task_list.append(s)

        logger.info('Found ' + str(len(task_list)) + ' of ' +
                    str(len(config.sections())) + ' to be incomplete tasks')
        return task_list


class BatchedTasksListGenerator(object):
    """Creates Batched Jobs List file used by chmrunner.py
    """
    OLD_SUFFIX = '.old'

    def __init__(self, tasks_per_node):
        """Constructor
        """
        self._tasks_per_node = int(tasks_per_node)

    def _write_batched_task_config(self, bconfig, configfile):
        """Writes out batched job config
        """

        if os.path.isfile(configfile):
            logger.debug('Previous batched job config file found. '
                         'Appending .old suffix')
            shutil.move(configfile, configfile +
                        BatchedTasksListGenerator.OLD_SUFFIX)

        logger.debug('Writing batched job config file to ' + configfile)
        f = open(configfile, 'w')
        bconfig.write(f)
        f.flush()
        f.close()

    def write_batched_config(self, configfile, task_list):
        """Examines chm jobs list and looks for
        incomplete jobs. The incomplete jobs are written
        into `CHMJobCreator.CONFIG_BATCHED_JOBS_FILE_NAME` batched by number
        of jobs per node set in `CHMJobCreator.CONFIG_FILE_NAME`
        :param configfile: file path to write configuration file to
        :raises InvalidConfigFileError: if configfile parameter is None
        :raises InvalidTaskListError: if task_list parameter is None
        :returns: Number of jobs that need to be run
        """
        if configfile is None:
            raise InvalidConfigFileError('configfile passed in cannot be null')

        if task_list is None:
            raise InvalidTaskListError('task list cannot be None')

        if len(task_list) is 0:
            logger.debug('All tasks complete')
            return 0

        bconfig = configparser.ConfigParser()

        total = len(task_list)
        task_counter = 1
        for j in range(0, total, self._tasks_per_node):
            bconfig.add_section(str(task_counter))
            bconfig.set(str(task_counter), CHMJobCreator.BCONFIG_TASK_ID,
                        ','.join(task_list[j:j+self._tasks_per_node]))
            task_counter += 1

        self._write_batched_task_config(bconfig, configfile)
        return task_counter-1


class RocceCluster(object):
    """Generates submit script for CHM job on Rocce cluster
    """
    CLUSTER = 'rocce'
    SUBMIT_SCRIPT_NAME = 'runjobs.' + CLUSTER
    MERGE_SUBMIT_SCRIPT_NAME = 'runmerge.' + CLUSTER
    DEFAULT_JOBS_PER_NODE = 1

    def __init__(self, chmconfig):
        """Constructor
        :param chmconfig: CHMConfig object for the job
        """
        self._chmconfig = chmconfig

    def set_chmconfig(self, chmconfig):
        """Sets CHMConfig object
        :param chmconfig: CHMConfig object
        """
        self._chmconfig = chmconfig

    def get_suggested_tasks_per_node(self, jobs_per_node):
        """Returns suggested tasks per node for cluster
        :returns: 1 as int
        """
        if jobs_per_node is None:
            logger.debug('Using default since tasks per node is None')
            return RocceCluster.DEFAULT_JOBS_PER_NODE

        if jobs_per_node <= 0:
            logger.debug('Using default since tasks per node is 0 or less')
            return RocceCluster.DEFAULT_JOBS_PER_NODE
        try:
            return int(jobs_per_node)
        except ValueError:
            logger.debug('Using default since tasks per int conversion failed')
            return RocceCluster.DEFAULT_JOBS_PER_NODE

    def get_cluster(self):
        """Returns cluster name which is rocce
        :returns: name of cluster as string in this case rocce
        """
        return RocceCluster.CLUSTER

    def _get_submit_script_path(self):
        """Gets path to submit script
        """
        if self._chmconfig is None:
            return RocceCluster.SUBMIT_SCRIPT_NAME

        return os.path.join(self._chmconfig.get_out_dir(),
                            RocceCluster.SUBMIT_SCRIPT_NAME)

    def _get_merge_submit_script_path(self):
        """Gets path to submit script
        """
        if self._chmconfig is None:
            return RocceCluster.MERGE_SUBMIT_SCRIPT_NAME

        return os.path.join(self._chmconfig.get_out_dir(),
                            RocceCluster.
                            MERGE_SUBMIT_SCRIPT_NAME)

    def _get_chm_runner_path(self):
        """gets path to chmrunner.py

        :return: path to chmrunner.py
        """
        if self._chmconfig is None:
            return CHMJobCreator.CHMRUNNER
        return os.path.join(self._chmconfig.get_script_bin(),
                            CHMJobCreator.CHMRUNNER)

    def _get_merge_runner_path(self):
        """gets path to mergetilerunner.py
        """
        if self._chmconfig is None:
            return CHMJobCreator.MERGERUNNER
        return os.path.join(self._chmconfig.get_script_bin(),
                            CHMJobCreator.MERGERUNNER)

    def generate_submit_script(self):
        """Creates submit script and instructions for invocation
        :returns: path to submit script
        """
        script = self._get_submit_script_path()
        out_dir = self._chmconfig.get_out_dir()
        max_mem = str(self._chmconfig.get_max_chm_memory_in_gb())

        stdout_path = os.path.join(self._chmconfig.get_stdout_dir(),
                                   '$JOB_ID.$TASK_ID.out')
        return self._write_submit_script(script, out_dir, stdout_path,
                                         self._chmconfig.get_job_name(),
                                         self._chmconfig.get_walltime(),
                                         self._get_chm_runner_path(),
                                         ',h_vmem=' + max_mem + 'G',
                                         self._chmconfig.get_shared_tmp_dir())

    def generate_merge_submit_script(self):
        """Creates merge submit script and instructions for invocation
        :returns: path to submit script
        """
        script = self._get_merge_submit_script_path()
        out_dir = self._chmconfig.get_out_dir()
        max_mem = str(self._chmconfig.get_max_merge_memory_in_gb())
        stdout_path = os.path.join(self._chmconfig.get_merge_stdout_dir(),
                                   '$JOB_ID.$TASK_ID.out')
        return self._write_submit_script(script, out_dir, stdout_path,
                                         self._chmconfig.get_mergejob_name(),
                                         self._chmconfig.get_merge_walltime(),
                                         self._get_merge_runner_path(),
                                         ',h_vmem=' + max_mem + 'G',
                                         self._chmconfig.get_shared_tmp_dir())

    def _write_submit_script(self, script, working_dir, stdout_path, job_name,
                             walltime, run_script_path,
                             resource_reqs, tmp_dir):
        """Generates submit script content suitable for rocce cluster
        :param working_dir: Working directory
        :param stdout_path: Standard out file path for jobs.
                            ie ./$JOB_ID.$TASKID
        :param job_name: Job name ie foojob
        :param walltime: Maximum time job is allowed to run ie 12:00:00
        :param run_script_path: full path to run script
        :return: string of submit job
        """
        f = open(script, 'w')
        f.write('#!/bin/sh\n')
        f.write('#\n#$ -V\n#$ -S /bin/sh\n#$ -notify\n')
        f.write('#$ -wd ' + working_dir + '\n')
        f.write('#$ -o ' + stdout_path + '\n')
        f.write('#$ -j y\n#$ -N ' + job_name + '\n')
        f.write('#$ -l h_rt=' + walltime
                + resource_reqs + '\n')
        f.write('#$ -q all.q\n#$ -m n\n\n')
        f.write('echo "HOST: $HOSTNAME"\n')
        f.write('echo "DATE: `date`"\n\n')
        f.write('echo "JOBID: $JOB_ID"\n')
        f.write('echo "TASKID: $SGE_TASK_ID"\n')
        f.write('/usr/bin/time -p ' + run_script_path +
                ' $SGE_TASK_ID ' + working_dir + ' --scratchdir ' +
                tmp_dir + ' --log DEBUG\n')
        f.write('\nexitcode=$?\n')
        f.write('echo "' + os.path.basename(run_script_path) +
                ' exited with code: $exitcode"\n')
        f.write('exit $exitcode\n')
        f.flush()
        f.close()
        os.chmod(script, stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH)
        return script

    def get_runchmjob_submit_command(self):
        """Returns checkchmjob.py command the user should run
        :returns: string containing checkchmjob.py the user should invoke
        """
        runchm =  os.path.join(self._chmconfig.get_script_bin(),
                               CHMJobCreator.CHECKCHMJOB)
        return runchm + ' "' + self._chmconfig.get_out_dir()

    def get_chm_submit_command(self, number_jobs):
        """Returns submit command user should invoke
           to run jobs on scheduler
        """
        val = ('cd "' + self._chmconfig.get_out_dir() + '";' +
               'qsub -t 1-' + str(number_jobs) + ' ' +
               RocceCluster.SUBMIT_SCRIPT_NAME)
        return val

    def get_merge_submit_command(self, number_jobs):
        """Returns submit command user should invoke
           to run jobs on scheduler
        """
        val = ('cd "' + self._chmconfig.get_out_dir() + '";' +
               'qsub -t 1-' + str(number_jobs) + ' ' +
               RocceCluster.MERGE_SUBMIT_SCRIPT_NAME)
        return val


class ClusterFactory(object):
    """Factory that produces cluster objects based on cluster name
    """
    # WARNING
    VALID_CLUSTERS = [RocceCluster.CLUSTER]

    def __init__(self):
        """Constructor
        """
        pass

    def get_cluster_by_name(self, name):
        """Creates Cluster object based on value of `name` passed in
        :param name: string containing name of cluster
        :returns: Cluster object for that cluster or None if no match
        """
        if name is None:
            logger.error('name passed in is None')
            return None

        lc_cluster = name.lower()

        if lc_cluster == RocceCluster.CLUSTER:
            logger.debug('returning RocceCluster')
            return RocceCluster(None)

        logger.error('No cluster class supporting ' + lc_cluster + ' found')
        return None
