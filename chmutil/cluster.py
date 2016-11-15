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


class RocceSubmitScriptGenerator(object):
    """Generates submit script for CHM job on Rocce cluster
    """
    SUBMIT_SCRIPT_NAME = 'runjobs.rocce'
    MERGE_SUBMIT_SCRIPT_NAME = 'runmerge.rocce'
    CHMRUNNER = 'chmrunner.py'
    MERGERUNNER = 'mergetilerunner.py'

    def __init__(self, chmconfig):
        """Constructor
        :param chmconfig: CHMConfig object for the job
        """
        self._chmconfig = chmconfig

    def _get_submit_script_path(self):
        """Gets path to submit script
        """
        if self._chmconfig is None:
            return RocceSubmitScriptGenerator.SUBMIT_SCRIPT_NAME

        return os.path.join(self._chmconfig.get_out_dir(),
                            RocceSubmitScriptGenerator.SUBMIT_SCRIPT_NAME)

    def _get_merge_submit_script_path(self):
        """Gets path to submit script
        """
        if self._chmconfig is None:
            return RocceSubmitScriptGenerator.MERGE_SUBMIT_SCRIPT_NAME

        return os.path.join(self._chmconfig.get_out_dir(),
                            RocceSubmitScriptGenerator.
                            MERGE_SUBMIT_SCRIPT_NAME)

    def _get_chm_runner_path(self):
        """gets path to chmrunner.py

        :return: path to chmrunner.py
        """
        if self._chmconfig is None:
            return RocceSubmitScriptGenerator.CHMRUNNER
        return os.path.join(self._chmconfig.get_script_bin(),
                            RocceSubmitScriptGenerator.CHMRUNNER)

    def _get_merge_runner_path(self):
        """gets path to mergetilerunner.py
        """
        if self._chmconfig is None:
            return RocceSubmitScriptGenerator.MERGERUNNER
        return os.path.join(self._chmconfig.get_script_bin(),
                            RocceSubmitScriptGenerator.MERGERUNNER)

    def generate_submit_script(self):
        """Creates submit script and instructions for invocation
        :returns: path to submit script
        """
        script = self._get_submit_script_path()
        out_dir = self._chmconfig.get_out_dir()
        f = open(script, 'w')
        f.write('#!/bin/sh\n')
        f.write('#\n#$ -V\n#$ -S /bin/sh\n')
        f.write('#$ -wd ' + out_dir + '\n')
        f.write('#$ -o ' + os.path.join(self._chmconfig.get_stdout_dir(),
                                        '$JOB_ID.$TASK_ID.out') + '\n')
        f.write('#$ -j y\n#$ -N ' + self._chmconfig.get_job_name() + '\n')
        f.write('#$ -l h_rt=' + self._chmconfig.get_walltime()
                + ',h_vmem=5G,h=\'!compute-0-20\'\n')
        f.write('#$ -q all.q\n#$ -m n\n\n')
        f.write('echo "HOST: $HOSTNAME"\n')
        f.write('echo "DATE: `date`"\n\n')
        f.write('/usr/bin/time -p ' + self._get_chm_runner_path() +
                ' $SGE_TASK_ID ' + out_dir + ' --scratchdir ' +
                self._chmconfig.get_shared_tmp_dir() + ' --log DEBUG\n')
        f.flush()
        f.close()
        os.chmod(script, stat.S_IRWXU)
        return script

    def generate_merge_submit_script(self):
        """Creates merge submit script and instructions for invocation
        :returns: path to submit script
        """
        script = self._get_merge_submit_script_path()
        out_dir = self._chmconfig.get_out_dir()
        f = open(script, 'w')
        f.write('#!/bin/sh\n')
        f.write('#\n#$ -V\n#$ -S /bin/sh\n')
        f.write('#$ -wd ' + out_dir + '\n')
        f.write('#$ -o ' + os.path.join(self._chmconfig.get_merge_stdout_dir(),
                                        '$JOB_ID.$TASK_ID.out') + '\n')
        f.write('#$ -j y\n#$ -N ' + self._chmconfig.get_mergejob_name() + '\n')
        f.write('#$ -l h_rt=' + self._chmconfig.get_walltime()
                + ',h_vmem=5G,h=\'!compute-0-20\'\n')
        f.write('#$ -q all.q\n#$ -m n\n\n')
        f.write('echo "HOST: $HOSTNAME"\n')
        f.write('echo "DATE: `date`"\n\n')
        f.write('/usr/bin/time -p ' + self._get_merge_runner_path() +
                ' $SGE_TASK_ID ' + out_dir + ' --scratchdir ' +
                self._chmconfig.get_shared_tmp_dir() + ' --log DEBUG\n')
        f.flush()
        f.close()
        os.chmod(script, stat.S_IRWXU)
        return script
