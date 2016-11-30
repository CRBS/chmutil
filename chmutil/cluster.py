# -*- coding: utf-8 -*-


import os
import stat
import logging
import shutil
import configparser

from chmutil.core import CHMJobCreator

logger = logging.getLogger(__name__)


class InvalidConfigFileError(Exception):
    """Raised if config file path is invalid
    """
    pass


class CHMJobChecker(object):
    """Checks and returns incomplete CHM Jobs
    """
    def __init__(self, config):
        self._config = config

    def get_incomplete_jobs_list(self):
        """gets list of incomplete jobs
        """
        config = self._config
        job_list = []

        for s in config.sections():
            out_file = config.get(s, CHMJobCreator.CONFIG_OUTPUT_IMAGE)
            if not os.path.isfile(out_file):
                job_list.append(s)

        logger.info('Found ' + str(len(job_list)) + ' of ' +
                    str(len(config.sections())) + ' to be incomplete jobs')
        return job_list


class MergeJobChecker(object):
    """Checks and returns incomplete Merge Jobs
    """
    def __init__(self, config):
        self._config = config

    def get_incomplete_jobs_list(self):
        """gets list of incomplete jobs
        """
        config = self._config
        job_list = []

        for s in config.sections():
            out_file = config.get(s, CHMJobCreator.MERGE_OUTPUT_IMAGE)
            if not os.path.isfile(out_file):
                job_list.append(s)

        logger.info('Found ' + str(len(job_list)) + ' of ' +
                    str(len(config.sections())) + ' to be incomplete jobs')
        return job_list


class BatchedJobsListGenerator(object):
    """Creates Batched Jobs List file used by chmrunner.py
    """
    OLD_SUFFIX = '.old'

    def __init__(self, job_checker, jobs_per_node):
        """Constructor
        """
        self._jobchecker = job_checker
        self._jobs_per_node = int(jobs_per_node)

    def _write_batched_job_config(self, bconfig, configfile):
        """Writes out batched job config
        """

        if os.path.isfile(configfile):
            logger.debug('Previous batched job config file found. '
                         'Appending .old suffix')
            shutil.move(configfile, configfile +
                        BatchedJobsListGenerator.OLD_SUFFIX)

        logger.debug('Writing batched job config file to ' + configfile)
        f = open(configfile, 'w')
        bconfig.write(f)
        f.flush()
        f.close()

    def generate_batched_jobs_list(self, configfile):
        """Examines chm jobs list and looks for
        incomplete jobs. The incomplete jobs are written
        into `CHMJobCreator.CONFIG_BATCHED_JOBS_FILE_NAME` batched by number
        of jobs per node set in `CHMJobCreator.CONFIG_FILE_NAME`
        :returns: Number of jobs that need to be run
        """
        if configfile is None:
            raise InvalidConfigFileError('configfile passed in cannot be null')

        job_list = self._jobchecker.get_incomplete_jobs_list()
        if len(job_list) is 0:
            logger.debug('All jobs complete')
            return 0

        bconfig = configparser.ConfigParser()

        total = len(job_list)
        job_counter = 1
        for j in range(0, total, self._jobs_per_node):
            bconfig.add_section(str(job_counter))
            bconfig.set(str(job_counter), CHMJobCreator.BCONFIG_TASK_ID,
                        ','.join(job_list[j:j+self._jobs_per_node]))
            job_counter += 1

        self._write_batched_job_config(bconfig, configfile)
        return job_counter-1


class RocceCluster(object):
    """Generates submit script for CHM job on Rocce cluster
    """
    CLUSTER = 'rocce'
    SUBMIT_SCRIPT_NAME = 'runjobs.' + CLUSTER
    MERGE_SUBMIT_SCRIPT_NAME = 'runmerge' + CLUSTER
    CHMRUNNER = 'chmrunner.py'
    MERGERUNNER = 'mergetilerunner.py'
    RUNCHMJOB = 'runchmjob.py'
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

    def get_suggested_jobs_per_node(self, jobs_per_node):
        """Returns suggested jobs per node for cluster
        :returns: 1 as int
        """
        if jobs_per_node is None:
            logger.debug('Using default since jobs per node is None')
            return RocceCluster.DEFAULT_JOBS_PER_NODE

        if jobs_per_node <= 0:
            logger.debug('Using default since jobs per node is 0 or less')
            return RocceCluster.DEFAULT_JOBS_PER_NODE
        try:
            return int(jobs_per_node)
        except ValueError:
            logger.debug('Using default since jobs per int conversion failed')
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
            return RocceCluster.CHMRUNNER
        return os.path.join(self._chmconfig.get_script_bin(),
                            RocceCluster.CHMRUNNER)

    def _get_merge_runner_path(self):
        """gets path to mergetilerunner.py
        """
        if self._chmconfig is None:
            return RocceCluster.MERGERUNNER
        return os.path.join(self._chmconfig.get_script_bin(),
                            RocceCluster.MERGERUNNER)

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
        """Returns runchmjob.py command the user should run
        :returns: string containing runchmjob.py the user should invoke
        """
        runchm =  os.path.join(self._chmconfig.get_script_bin(),
                               RocceCluster.RUNCHMJOB)
        val = (runchm + ' "' + self._chmconfig.get_out_dir() +
               '" --cluster ' + self.get_cluster())
        return val

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
