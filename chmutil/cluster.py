# -*- coding: utf-8 -*-


import os
import sys
import stat
import logging
import shutil
import configparser
from configparser import NoOptionError

from chmutil.core import CHMJobCreator
from chmutil.image import ImageStatsSummary
from chmutil.image import ImageStatsFromDirectoryFactory

logger = logging.getLogger(__name__)


class InvalidConfigFileError(Exception):
    """Raised if config file path is invalid
    """
    pass


class InvalidTaskListError(Exception):
    """Raised if invalid task list is used
    """
    pass


class InvalidScriptNameError(Exception):
    """Raised if invalid script name is used"""
    pass


class InvalidWorkingDirError(Exception):
    """Raised if invalid working directory is used"""
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
        self._total_walltime = 0
        self._total_usertime = 0
        self._total_tasks_with_cputimes = 0
        self._total_memorykb = 0
        self._max_memorykb = 0

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
        self._total_task_count = count

    def get_total_task_count(self):
        """returns total tasks count
        """
        return self._total_task_count

    def set_total_cpu_walltime(self, total_walltime):
        """Sets total walltime"""
        self._total_walltime = total_walltime

    def get_total_cpu_walltime(self):
        """Gets total walltime
        """
        return self._total_walltime

    def set_total_cpu_usertime(self, total_usertime):
        """Sets total usertime"""
        self._total_usertime = total_usertime

    def get_total_cpu_usertime(self):
        """Gets total usertime
        """
        return self._total_usertime

    def set_total_tasks_with_cputimes(self, count):
        """Sets number of tasks used to calculate
           cpu times
        """
        self._total_tasks_with_cputimes = count

    def get_total_tasks_with_cputimes(self):
        """Gets number of tasks used to calculate
           cpu times
        """
        return self._total_tasks_with_cputimes

    def set_max_memory_in_kb(self, max_memorykb):
        """Sets max memory for jobs in kilobytes
        """
        self._max_memorykb = max_memorykb

    def get_max_memory_in_kb(self):
        """Gets max memory for jobs in kilobytes
        """
        return self._max_memorykb

    def set_total_memory_in_kb(self, total_memorykb):
        """Sets total memory in kilobytes
        """
        self._total_memorykb = total_memorykb

    def get_total_memory_in_kb(self):
        """Gets total memory in kilobytes
        """
        return self._total_memorykb


class TaskSummary(object):
    """Summary of a CHM job
    """

    GIGA_PREFIX = 'giga'
    MEGA_PREFIX = 'mega'
    MEGA_VAL = 1000000
    GIGA_VAL = 1000000000

    def __init__(self, chmconfig, chm_task_stats=None,
                 merge_task_stats=None,
                 image_stats_summary=None):
        """Constructor
        """
        self._chmconfig = chmconfig
        self._chm_task_stats = chm_task_stats
        self._merge_task_stats = merge_task_stats
        self._chm_task_summary = self.\
            _get_summary_from_task_stats(self._chm_task_stats)
        self._chm_compute_summary = self.\
            _get_compute_summary_from_task_stats('CHM',
                                                 self._chm_task_stats)
        self._merge_task_summary = self.\
            _get_summary_from_task_stats(self._merge_task_stats)
        self._merge_task_compute_summary = self.\
            _get_compute_summary_from_task_stats('Merge',
                                                 self._merge_task_stats)
        self._image_summary = self.\
            _get_input_image_summary(image_stats_summary)

    def get_chm_task_stats(self):
        """Returns `TaskStats` for CHM tasks
        """
        return self._chm_task_stats

    def get_merge_task_stats(self):
        """Returns `TaskStats` for merge tasks
        """
        return self._merge_task_stats

    def _convert_number_to_string(self, val):
        """Converts `val` to string with thousands separator (ie
           1233 becomes 1,233) for versions of python > 2.6
           otherwise this method just converts the number to string
        :param val: number to convert
        :returns: number converted to string with thousands separator
        """
        if sys.version_info[0] == 2 and sys.version_info[1] <= 6:
            return str(val)
        else:
            return '{0:,}'.format(val)

    def _convert_float_to_string(self, val):
        """Converts float `val` to string with thousands separator (ie
           1233 becomes 1,233.0) for versions of python > 2.6
           otherwise this method just converts the number to string
        :param val: number to convert
        :returns: number converted to string with thousands separator
        """
        if sys.version_info[0] == 2 and sys.version_info[1] <= 6:
            return str(val)
        else:
            return '{0:,.1f}'.format(val)

    def _convert_years_to_string(self, val):
        """Takes `val` which is assumed to be a float and returns
           string of float with only one value to right of decimal
           ie: 23423.23423 becomes 23423.2
        :param val: value to convert
        :returns: string converted value
        """
        return '{0:.1f}'.format(val)

    def _convert_float_to_string_with_unitprefix(self, val):
        """Converts number to string and appends unitprefix
        :param val: value to convert
        :returns: string of form <val as str> prefix
                  where prefix is none for values less then
                  10-e6, mega for values between
                  10-e6 and 10-e9, giga above 10-e9"""
        if val < TaskSummary.MEGA_VAL:
            return self._convert_number_to_string(val) + ' '

        if val < TaskSummary.GIGA_VAL:
            div_val = float(val)/TaskSummary.MEGA_VAL
            return (self._convert_float_to_string(div_val) + ' ' +
                    TaskSummary.MEGA_PREFIX)
        div_val = float(val)/TaskSummary.GIGA_VAL
        return (self._convert_float_to_string(div_val) + ' ' +
                TaskSummary.GIGA_PREFIX)

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
            return 'Total number of tasks is <= 0'

        completed = task_stats.get_completed_task_count()
        pc_complete_str = '{0:.0%}'.format((float(completed)/float(total)))

        # if any jobs are not complete set percent to 99%.
        # This is a fix to deal with
        if pc_complete_str == '100%' and completed < total:
            pc_complete_str = '99%'

        completed_str = self._convert_number_to_string(completed)
        total_str = self._convert_number_to_string(total)

        return (pc_complete_str + ' complete (' + completed_str + ' of ' +
                total_str + ' completed)')

    def _get_runtime_in_hours_per_task(self, task_stats):
        """Calculates average runtime in hours based on information
           in task_stats passed in
        :param task_stats: TaskStats object containing raw stats
        :returns: average runtime in hours as float
        """
        num_tasks_with_cputimes = task_stats.get_total_tasks_with_cputimes()
        # get average runtime
        walltime = task_stats.get_total_cpu_walltime()
        avg_seconds_per_task = walltime / num_tasks_with_cputimes
        avg_hours_per_task = avg_seconds_per_task / 3600.0
        return self._convert_float_to_string(avg_hours_per_task)

    def _get_compute_summary_from_task_stats(self, prefix, task_stats):
        """Creates a summary string from `task_stats` object
        :param prefix: Prefix string to place at start of each line in summary
        :param task_stats: `TaskStats` object to summarize
        :returns: For valid `TaskStats` object this method will return a
                  string of form:
          <prefix> task runtime: # hours per task (#gb ram per task)
          <prefix> task CPU consumption so far: # CPU hours (~# CPU years)
          <prefix> task estimated remaining compute: # CPU hours (~# CPU years)
          <prefix> task estimated days to complete: # days
                  otherwise if task_stats is None this is output:

                   Unable to calculate CPU consumption for <prefix>

                  if no stats are available on computed jobs (no jobs finished)
                  then this is returned:

                  No completed task information for <prefix>
        """
        if task_stats is None:
            logger.warning('task_stats passed into '
                           '_get_compute_summary_from_task_stats is None')
            return ''

        num_tasks_with_cputimes = task_stats.get_total_tasks_with_cputimes()
        if num_tasks_with_cputimes <= 0:
            logger.info('No tasks with cpu times found')
            return ''

        # get average runtime
        avg_hours_str = self._get_runtime_in_hours_per_task(task_stats)

        # get average memory
        total_mem_kb = task_stats.get_total_memory_in_kb()
        avg_mem_gb = total_mem_kb / num_tasks_with_cputimes / 1000000.0
        mem_str = self._convert_number_to_string(avg_mem_gb)

        # get total compute consumed so far
        usertime = task_stats.get_total_cpu_usertime()
        total_user_hours = usertime / 3600.0
        total_user_years = total_user_hours / 24 / 365
        avg_compute_seconds_per_task = usertime / num_tasks_with_cputimes
        avg_compute_hours_per_task = avg_compute_seconds_per_task / 3600.0
        user_hours_str = self._convert_float_to_string(total_user_hours)
        user_years_str = self._convert_years_to_string(total_user_years)

        # get estimated remaining compute
        total = task_stats.get_total_task_count()
        completed = task_stats.get_completed_task_count()
        remaining = total - completed
        remain_years_str = 'NA'
        remain_hours_str = 'NA'

        if remaining > 0:
            remain_compute_hours = avg_compute_hours_per_task * remaining
            remain_compute_years = remain_compute_hours / 24 / 365
            remain_hours_str = self.\
                _convert_float_to_string(remain_compute_hours)
            remain_years_str = self.\
                _convert_years_to_string(remain_compute_years)

        p_str = str(prefix)
        return ('\n' + p_str + ' runtime: ' + avg_hours_str +
                ' hours per task (' + mem_str + 'GB ram)\n' +
                p_str + ' CPU consumption so far: ' + user_hours_str +
                ' CPU hours (~' + user_years_str + ' years)\n' +
                p_str + ' estimated remaining compute: ' + remain_hours_str +
                ' CPU hours (~' + remain_years_str + ' years)\n')

    def _get_image_dimensions_from_dict(self, image_dim_dict):
        """Given a dictionary containing keys that are WxH tuples
           and values that are the count of images with those
           dimensions. This method returns a human readable string
           of the image dimensions, if multiple dimensions are seen
           then the most common one is output with this line appended
           *Image dimensions differ"""
        if image_dim_dict is None:
            return 'No image dimension data found'

        if len(image_dim_dict.keys()) is 0:
            return 'Image dimension data empty'

        if len(image_dim_dict.keys()) is 1:
            imtuple = list(image_dim_dict.keys())[0]
            w_str = self._convert_number_to_string(imtuple[0])
            h_str = self._convert_number_to_string(imtuple[1])
            return w_str + ' x ' + h_str

        max_images = -1
        max_key = None
        for k in image_dim_dict.keys():
            if max_key is None or image_dim_dict[k] >= max_images:
                max_key = k
                max_images = image_dim_dict[max_key]

        imstr = self._convert_number_to_string(max_images)
        w_str = self._convert_number_to_string(max_key[0])
        h_str = self._convert_number_to_string(max_key[1])
        return (w_str + ' x ' + h_str + ' *Only ' + imstr +
                ' images have this dimension')

    def _get_input_image_summary(self, image_stats_summary):
        """Gets summary about input images from image_stats_summary object
        :param image_stats_summary: ImageStatsSummary object to get stats from
        :returns: string of form
                  Number input images: X,XXX (X PREFIXbytes)
                  Dimensions of images: XX,XXX X XX,XXXXX (X PREFIXpixels)

                  With *Image dimensions differ added to second line if any
                  of the images have different dimensions
        """
        if image_stats_summary is None:
            logger.info('ImageStatsSummary is None, not outputting stats')
            return ''

        rawcnt = image_stats_summary.get_image_count()
        strcnt = self._convert_number_to_string(rawcnt)

        rawsize = image_stats_summary.get_total_size_of_images_in_bytes()
        sizestr = self._convert_float_to_string_with_unitprefix(rawsize)

        dim_dict = image_stats_summary.get_image_dimensions_as_dict()
        dim_str = self._get_image_dimensions_from_dict(dim_dict)
        return ('Number input images: ' + strcnt + ' (' + sizestr +
                'bytes)\nDimensions of images: ' + dim_str + '\n\n')

    def get_summary(self):
        """Gets the summary of CHM job in human readable form
        """
        if self._chmconfig is None:
            logger.warning('CHMConfig is None in TaskSummary so '
                           'skipping output of job details')
            return ('CHM tasks: ' +
                    self._chm_task_summary + '\nMerge tasks: ' +
                    self._merge_task_summary + '\n')

        return ('chmutil version: ' + self._chmconfig.get_version() + '\n' +
                'Tiles: ' + self._chmconfig.get_tile_size() + ' with ' +
                self._chmconfig.get_overlap_size() + ' overlap\n' +
                'Disable histogram equalization in CHM: ' +
                str(self._chmconfig.get_disable_histogram_eq_val()) + '\n' +
                'Tasks: ' + str(self._chmconfig.get_number_tiles_per_task()) +
                ' tiles per task, ' +
                str(self._chmconfig.get_number_tasks_per_node()) +
                ' tasks(s) per node\nTrained CHM model: ' +
                self._chmconfig.get_model() + '\nCHM binary: ' +
                self._chmconfig.get_chm_binary() + '\n\n' +
                self._image_summary +
                'CHM tasks: ' +
                self._chm_task_summary +
                self._chm_compute_summary +
                '\nMerge tasks: ' +
                self._merge_task_summary +
                self._merge_task_compute_summary + '\n')


class TaskSummaryFactory(object):
    """Examines CHM Job and creates JobSummary object
       which contains summary information about the
       job.
    """
    USERTIME_STR = 'User time (seconds):'
    ELAPSTIME_STR = 'Elapsed (wall clock) time (h:mm:ss or m:ss): '
    MAXMEM_STR = 'Maximum resident set size (kbytes): '
    REAL_STR = 'real '
    USER_STR = 'user '

    def __init__(self, chmconfig, chm_incomplete_tasks=None,
                 merge_incomplete_tasks=None,
                 output_compute=False):
        """Constructor
           :param chmconfig: Should be a `CHMConfig` object loaded with a
                             valid CHM job
           :param chm_incomplete_tasks: list of incomplete chm tasks
           :param merge_incomplete_tasks: list of incomplete merge tasks
        """
        self._chmconfig = chmconfig
        self._chm_incomplete_tasks = chm_incomplete_tasks
        self._merge_incomplete_tasks = merge_incomplete_tasks
        self._output_compute = output_compute

    def _get_files_in_directory_generator(self, path):
        """Generator that gets files in directory"""

        if path is None:
            logger.error('Path is None, returning nothing')
            return

        # first time we encounter a file yield it
        if os.path.isfile(path):
            yield path

        # second time we encounter a file just return
        if os.path.isfile(path):
            return

        if not os.path.isdir(path):
            return

        logger.debug(path + ' is a directory looking for files within')
        for entry in os.listdir(path):
            fullpath = os.path.join(path, entry)
            if os.path.isfile(fullpath):
                yield fullpath
            if os.path.isdir(fullpath):
                for aentry in self._get_files_in_directory_generator(fullpath):
                    yield aentry

    def _get_compute_hours_consumed(self, dirpath):
        """Looks at output from chm tasks to determine how much
        compute was consumed
        :param dirpath: directory containing task output files
        returns: array of tuples
                 [(user time in seconds,walltime in seconds,max memory in kb)]
        """
        res = []
        for taskfile in self._get_files_in_directory_generator(dirpath):
            logger.debug('Examining ' + taskfile)
            f = open(taskfile, 'r')
            usertime = 0
            max_memory = 0
            walltime = 0
            for line in f:
                if TaskSummaryFactory.USERTIME_STR in line:
                    usertime = float(line[line.index(': ')+1:].rstrip())
                if TaskSummaryFactory.MAXMEM_STR in line:
                    max_memory = int(line[line.index('): ')+2:].rstrip())
                if TaskSummaryFactory.ELAPSTIME_STR in line:
                    walltime_raw = line[line.index('): ')+2:].rstrip()
                    split_time = walltime_raw.split(':')
                    if len(split_time) == 3:
                        walltime += float(split_time[0]) * 3600
                        walltime += float(split_time[1]) * 60
                        walltime += float(split_time[2])
                    if len(split_time) == 2:
                        walltime += float(split_time[0]) * 60
                        walltime += float(split_time[1])

                if line.startswith(TaskSummaryFactory.REAL_STR):

                    walltime = float(line[len(TaskSummaryFactory.
                                              REAL_STR):].rstrip())

                if line.startswith(TaskSummaryFactory.USER_STR):
                    usertime = float(line[len(TaskSummaryFactory.
                                              USER_STR):].rstrip())
            if walltime > 0:
                res.append((usertime, walltime, max_memory))
            else:
                logger.debug('Parsed file had 0 walltime so skipping : ' +
                             taskfile)
        return res

    def _update_chm_task_stats_with_compute(self, taskstats, runtimes_list):
        """Updates if needed `TaskStats` passed in as chmts
        with compute usage if requested via `output_compute` flag
        in constructor
        :returns TaskStats: updated with compute stats if needed
        """
        if self._output_compute is False or runtimes_list is None:
            return taskstats

        if taskstats is None:
            logger.error('TaskStats is None, skipping update of compute times')
            return None

        num_tasks = len(runtimes_list)
        logger.debug('Found ' + str(num_tasks) + ' with compute times')
        sum_usertime = 0
        sum_walltime = 0
        max_memory = 0
        sum_memory = 0
        for t in runtimes_list:
            sum_usertime += t[0]
            sum_walltime += t[1]
            sum_memory += t[2]
            if t[2] > max_memory:
                max_memory = t[2]

        taskstats.set_total_tasks_with_cputimes(num_tasks)
        taskstats.set_total_cpu_usertime(sum_usertime)
        taskstats.set_total_cpu_walltime(sum_walltime)
        taskstats.set_max_memory_in_kb(max_memory)
        taskstats.set_total_memory_in_kb(sum_memory)

        return taskstats

    def _get_chm_task_stats(self):
        """Gets `TaskStats` for CHM tasks
        """
        total_chm_tasks = 0
        if self._chmconfig is not None:
            con = self._chmconfig.get_config()
            if con is not None:
                total_chm_tasks = len(self._chmconfig.get_config().sections())
                logger.debug('Total task count: ' + str(total_chm_tasks))

        completed_chm_tasks = 0
        if self._chm_incomplete_tasks is not None:
            num_incomplete_tasks = len(self._chm_incomplete_tasks)
            completed_chm_tasks = total_chm_tasks - num_incomplete_tasks

        chmts = TaskStats()
        chmts.set_completed_task_count(completed_chm_tasks)
        chmts.set_total_task_count(total_chm_tasks)

        runtimes_list = []
        try:
            stdout_dir = self._chmconfig.get_stdout_dir()
            logger.debug('Examining ' + stdout_dir + ' for log files to' +
                         'calculate compute times')

            runtimes_list = self._get_compute_hours_consumed(stdout_dir)
        except AttributeError:
            logger.error('Unable to get output directory from config'
                         'skipping examining of compute hours consumed')

        chmts = self._update_chm_task_stats_with_compute(chmts, runtimes_list)
        return chmts

    def _get_merge_task_stats(self):
        """Gets `TaskStats` for merge tasks
        """
        total_merge_tasks = 0
        if self._chmconfig.get_merge_config() is not None:
            total_merge_tasks = len(self._chmconfig.get_merge_config().
                                    sections())

        completed_merge_tasks = 0
        if self._merge_incomplete_tasks is not None:
            num_incomplete_tasks = len(self._merge_incomplete_tasks)
            completed_merge_tasks = total_merge_tasks - num_incomplete_tasks

        mergets = TaskStats()
        mergets.set_completed_task_count(completed_merge_tasks)
        mergets.set_total_task_count(total_merge_tasks)

        stdout_dir = self._chmconfig.get_merge_stdout_dir()
        runtimes_list = self._get_compute_hours_consumed(stdout_dir)
        mergets = self._update_chm_task_stats_with_compute(mergets,
                                                           runtimes_list)

        return mergets

    def _get_image_stats_summary(self):
        """Generates ImageStatsSummary object
        """
        if self._output_compute is None or self._output_compute is False:
            logger.debug('Skipping analysis of input image data')
            return None

        imgdir = self._chmconfig.get_images()
        if not os.path.isdir(imgdir):
            logger.error('Input image path not a directory')
            return ImageStatsSummary()

        fac = ImageStatsFromDirectoryFactory(imgdir)
        isum = ImageStatsSummary()
        for iis in fac.get_input_image_stats():
            isum.add_image_stats(iis)
        return isum

    def get_task_summary(self):
        """Gets `TaskSummary` for CHM job defined in constructor
           :returns: TaskSummary object
        """
        return TaskSummary(self._chmconfig,
                           chm_task_stats=self._get_chm_task_stats(),
                           merge_task_stats=self._get_merge_task_stats(),
                           image_stats_summary=self._get_image_stats_summary())


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
            logger.debug('Checking if image file exists: ' + out_file)
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
            logger.debug('Checking if image file exists: ' + out_file)
            if not os.path.isfile(out_file):
                task_list.append(s)

        logger.info('Found ' + str(len(task_list)) + ' of ' +
                    str(len(config.sections())) + ' to be incomplete tasks')
        return task_list


class CanMergeTaskBeRun(object):
    """Given a merge taskid instances of this class
       check to see if all tiles needed to perform
       the merge have been created by CHM task(s)
    """
    def __init__(self, chmconfig, incomplete_chm_tasks):
        """Constructor
        """
        self._chmconfig = chmconfig
        self._incomplete_chm_tasks = incomplete_chm_tasks

    def _build_lookup_table_mapping_merge_task_to_chm_task_ids(self):
        """Walks through CHM task configuration and builds a
           hash table where key is merge task id and value is
           a list of chm task ids
        """
    def can_task_be_run(self, taskid):
        """Checks if `taskid` merge task can be run
           :returns: tuple of (True|False, Reason|None) where
                     True in first element means yes it can be
                     run and False no. Second element will be None
                     or contain a string with reason job cannot be
                     run
        """
        raise NotImplementedError('Not implemented silly')


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


class Cluster(object):
    """Base class for all Cluster objects
    """
    def __init__(self, chmconfig):
        """Constructor
           :param chmconfig: CHMConfig object for the job
        """
        self._chmconfig = chmconfig
        self._cluster = 'notset'
        self._submit_script_name = 'notset'
        self._merge_submit_script_name = 'notset'
        self._default_jobs_per_node = 1
        self._default_merge_tasks_per_node = 1

    def set_chmconfig(self, chmconfig):
        """Sets CHMConfig object
        :param chmconfig: CHMConfig object
        """
        self._chmconfig = chmconfig

    def get_suggested_tasks_per_node(self, tasks_per_node):
        """Returns suggested tasks per node for cluster
        :returns: 1 as int
        """
        if tasks_per_node is None:
            logger.debug('Using default since tasks per node is None')
            return self._default_jobs_per_node

        try:
            jpn = int(tasks_per_node)
            if jpn <= 0:
                logger.debug('Using default since '
                             'tasks per node is 0 or less')
                return self._default_jobs_per_node
            return jpn
        except ValueError:
            logger.debug('Using default since tasks per int '
                         'conversion failed')
            return self._default_jobs_per_node

    def get_suggested_merge_tasks_per_node(self, tasks_per_node):
        """Returns suggested tasks per node for cluster
        :returns: 1 as int
        """
        if tasks_per_node is None:
            logger.debug('Using default since merge tasks per node is None')
            return self._default_merge_tasks_per_node

        try:
            jpn = int(tasks_per_node)
            if jpn <= 0:
                logger.debug('Using default since merge '
                             'tasks per node is 0 or less')
                return self._default_merge_tasks_per_node
            return jpn
        except ValueError:
            logger.debug('Using default since merge '
                         'tasks per int conversion failed')
            return self._default_merge_tasks_per_node

    def get_checkchmjob_command(self):
        """Returns checkchmjob.py command the user should run
        :returns: string containing checkchmjob.py the user should invoke
        """
        runchm = os.path.join(self._chmconfig.get_script_bin(),
                              CHMJobCreator.CHECKCHMJOB)
        return runchm + ' "' + self._chmconfig.get_out_dir() + '" --submit'

    def get_cluster(self):
        """Returns cluster name which is rocce
        :returns: name of cluster as string in this case rocce
        """
        return self._cluster

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

    def _get_submit_script_path(self):
        """Gets path to submit script
        """
        if self._chmconfig is None:
            return self._submit_script_name

        return os.path.join(self._chmconfig.get_out_dir(),
                            self._submit_script_name)

    def _get_merge_submit_script_path(self):
        """Gets path to submit script
        """
        if self._chmconfig is None:
            return self._merge_submit_script_name

        return os.path.join(self._chmconfig.get_out_dir(),
                            self._merge_submit_script_name)


class RocceCluster(Cluster):
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
        super(RocceCluster, self).__init__(chmconfig)

        self._cluster = RocceCluster.CLUSTER
        self._submit_script_name = RocceCluster.SUBMIT_SCRIPT_NAME
        self._merge_submit_script_name = RocceCluster.MERGE_SUBMIT_SCRIPT_NAME
        self._default_jobs_per_node = RocceCluster.DEFAULT_JOBS_PER_NODE
        self._default_merge_tasks_per_node = RocceCluster.DEFAULT_JOBS_PER_NODE

    def get_chm_submit_command(self, number_jobs):
        """Returns submit command user should invoke
           to run jobs on scheduler
        """
        val = ('cd "' + self._chmconfig.get_out_dir() + '";' +
               'qsub -t 1-' + str(number_jobs) + ' ' +
               self._submit_script_name)
        return val

    def get_merge_submit_command(self, number_jobs):
        """Returns submit command user should invoke
           to run jobs on scheduler
        """
        val = ('cd "' + self._chmconfig.get_out_dir() + '";' +
               'qsub -t 1-' + str(number_jobs) + ' ' +
               self._merge_submit_script_name)
        return val

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
                                         ',h_vmem=' + max_mem + 'G' +
                                         ',virtual_free=' + max_mem + 'G',
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
        f.write('/usr/bin/time -v ' + run_script_path +
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


class GordonCluster(Cluster):
    """Generates submit script for CHM job on Gordon cluster
    """
    CLUSTER = 'gordon'
    SUBMIT_SCRIPT_NAME = 'runjobs.' + CLUSTER
    MERGE_SUBMIT_SCRIPT_NAME = 'runmerge.' + CLUSTER
    DEFAULT_JOBS_PER_NODE = 8
    MERGE_TASKS_PER_NODE = 6
    MAX_TASKS_PER_ARRAY_JOB = 1000
    WARNING_MESSAGE = ('\n# Gordon is limited to ' +
                       str(MAX_TASKS_PER_ARRAY_JOB) +
                       ' per qsub call. Once these jobs are complete ' +
                       'checkchmjob.py will ' +
                       'need to be run again\n\n')

    def __init__(self, chmconfig):
        """Constructor
        :param chmconfig: CHMConfig object for the job
        """
        super(GordonCluster, self).__init__(chmconfig)

        self._cluster = GordonCluster.CLUSTER
        self._submit_script_name = GordonCluster.SUBMIT_SCRIPT_NAME
        self._merge_submit_script_name = GordonCluster.MERGE_SUBMIT_SCRIPT_NAME
        self._default_jobs_per_node = GordonCluster.DEFAULT_JOBS_PER_NODE
        self._default_merge_tasks_per_node = GordonCluster.MERGE_TASKS_PER_NODE

    def _get_adjusted_number_of_tasks_and_warning(self, number_tasks):
        """Gordon only allows 1,000 tasks per array job.
        If an array job is launced in excess of this, an error is output.
        To bypass this issue, this method returns a tuple with a warning
        message and an adjusted number of tasks.
        :param number_tasks: number of tasks that need to be run
        :return: tuple (number of tasks, warning text for user)
        """
        if number_tasks > GordonCluster.MAX_TASKS_PER_ARRAY_JOB:
            return (GordonCluster.MAX_TASKS_PER_ARRAY_JOB,
                    GordonCluster.WARNING_MESSAGE)
        return number_tasks, ''

    def get_chm_submit_command(self, number_jobs):
        """Returns submit command user should invoke
           to run jobs on scheduler. Do
        """
        (number_tasks, warn_msg) = self.\
            _get_adjusted_number_of_tasks_and_warning(number_jobs)

        self.generate_submit_script(number_tasks=number_tasks)
        val = (warn_msg + 'cd "' + self._chmconfig.get_out_dir() + '";' +
               'qsub ' +
               GordonCluster.SUBMIT_SCRIPT_NAME)
        return val

    def get_merge_submit_command(self, number_jobs):
        """Returns submit command user should invoke
           to run jobs on scheduler
        """
        (number_tasks, warn_msg) = self. \
            _get_adjusted_number_of_tasks_and_warning(number_jobs)
        self.generate_merge_submit_script(number_tasks=number_tasks)
        val = (warn_msg + 'cd "' + self._chmconfig.get_out_dir() + '";' +
               'qsub ' +
               GordonCluster.MERGE_SUBMIT_SCRIPT_NAME)
        return val

    def _get_standard_out_filename(self):
        """Gets standard out file name for jobs
        """
        return '$PBS_JOBID.$PBS_ARRAYID.out'

    def generate_submit_script(self, number_tasks=1):
        """Creates submit script and instructions for invocation
        :returns: path to submit script
        """
        script = self._get_submit_script_path()
        out_dir = self._chmconfig.get_out_dir()

        stdout_path = os.path.join(self._chmconfig.get_stdout_dir(),
                                   self._get_standard_out_filename())
        return self._write_submit_script(script, out_dir, stdout_path,
                                         self._chmconfig.get_job_name(),
                                         self._chmconfig.get_walltime(),
                                         self._get_chm_runner_path(),
                                         self._chmconfig.get_account(),
                                         self._chmconfig.get_shared_tmp_dir(),
                                         number_tasks)

    def generate_merge_submit_script(self, number_tasks=1):
        """Creates merge submit script and instructions for invocation
        :returns: path to submit script
        """
        script = self._get_merge_submit_script_path()
        out_dir = self._chmconfig.get_out_dir()
        stdout_path = os.path.join(self._chmconfig.get_merge_stdout_dir(),
                                   self._get_standard_out_filename())
        return self._write_submit_script(script, out_dir, stdout_path,
                                         self._chmconfig.get_mergejob_name(),
                                         self._chmconfig.get_merge_walltime(),
                                         self._get_merge_runner_path(),
                                         self._chmconfig.get_account(),
                                         self._chmconfig.get_shared_tmp_dir(),
                                         number_tasks)

    def _write_submit_script(self, script, working_dir, stdout_path, job_name,
                             walltime, run_script_path,
                             account, tmp_dir, number_tasks):
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
        f.write('#!/bin/sh\n#\n')
        f.write('#PBS -m n\n')
        f.write('#PBS -V\n')
        f.write('#PBS -A ' + account + '\n')
        f.write('#PBS -w ' + working_dir + '\n')
        f.write('#PBS -q normal\n')
        f.write('#PBS -l nodes=1:ppn=16:native:noflash\n')
        f.write('#PBS -o ' + stdout_path + '\n')
        f.write('#PBS -j oe\n')
        f.write('#PBS -t 1-' + str(number_tasks) + '\n')
        f.write('#PBS -N ' + job_name + '\n')
        f.write('#PBS -l walltime=' + walltime + '\n\n')

        f.write('echo "HOST: $HOSTNAME"\n')
        f.write('echo "DATE: `date`"\n\n')
        f.write('echo "JOBID: $PBS_JOBID"\n')
        f.write('echo "TASKID: $PBS_ARRAYID"\n\n')
        f.write('module load singularity/2.2\n\n')
        f.write('/usr/bin/time -v ' + run_script_path +
                ' $PBS_ARRAYID ' + working_dir + ' --scratchdir ' +
                tmp_dir + ' --log DEBUG\n')
        f.write('\nexitcode=$?\n')
        f.write('echo "' + os.path.basename(run_script_path) +
                ' exited with code: $exitcode"\n')
        f.write('exit $exitcode\n')
        f.flush()
        f.close()
        os.chmod(script, stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH)
        return script


# -----------------------
class CometCluster(Cluster):
    """Generates submit script for CHM job on Comet cluster
    """
    CLUSTER = 'comet'
    SUBMIT_SCRIPT_NAME = 'runjobs.' + CLUSTER
    MERGE_SUBMIT_SCRIPT_NAME = 'runmerge.' + CLUSTER
    DEFAULT_JOBS_PER_NODE = 16
    MERGE_TASKS_PER_NODE = 10

    def __init__(self, chmconfig):
        """Constructor
        :param chmconfig: CHMConfig object for the job
        """
        super(CometCluster, self).__init__(chmconfig)

        self._cluster = CometCluster.CLUSTER
        self._submit_script_name = CometCluster.SUBMIT_SCRIPT_NAME
        self._merge_submit_script_name = CometCluster.MERGE_SUBMIT_SCRIPT_NAME
        self._default_jobs_per_node = CometCluster.DEFAULT_JOBS_PER_NODE
        self._default_merge_tasks_per_node = CometCluster.MERGE_TASKS_PER_NODE

    def get_chm_submit_command(self, number_jobs):
        """Returns submit command user should invoke
           to run jobs on scheduler. Do
        """
        val = ('cd "' + self._chmconfig.get_out_dir() + '";' +
               'sbatch -a 1-' + str(number_jobs) + ' ' +
               CometCluster.SUBMIT_SCRIPT_NAME)
        return val

    def get_merge_submit_command(self, number_jobs):
        """Returns submit command user should invoke
           to run jobs on scheduler
        """
        val = ('cd "' + self._chmconfig.get_out_dir() + '";' +
               'sbatch -a 1-' + str(number_jobs) + ' ' +
               CometCluster.MERGE_SUBMIT_SCRIPT_NAME)
        return val

    def _get_standard_out_filename(self):
        """Gets standard out file name for jobs
        """
        return '%A.%a.out'

    def generate_submit_script(self):
        """Creates submit script and instructions for invocation
        :returns: path to submit script
        """
        script = self._get_submit_script_path()
        out_dir = self._chmconfig.get_out_dir()

        stdout_path = os.path.join(self._chmconfig.get_stdout_dir(),
                                   self._get_standard_out_filename())
        return self._write_submit_script(script, out_dir, stdout_path,
                                         self._chmconfig.get_job_name(),
                                         self._chmconfig.get_walltime(),
                                         self._get_chm_runner_path(),
                                         self._chmconfig.get_account(),
                                         self._chmconfig.get_shared_tmp_dir())

    def generate_merge_submit_script(self):
        """Creates merge submit script and instructions for invocation
        :returns: path to submit script
        """
        script = self._get_merge_submit_script_path()
        out_dir = self._chmconfig.get_out_dir()
        stdout_path = os.path.join(self._chmconfig.get_merge_stdout_dir(),
                                   self._get_standard_out_filename())
        return self._write_submit_script(script, out_dir, stdout_path,
                                         self._chmconfig.get_mergejob_name(),
                                         self._chmconfig.get_merge_walltime(),
                                         self._get_merge_runner_path(),
                                         self._chmconfig.get_account(),
                                         self._chmconfig.get_shared_tmp_dir())

    def _write_submit_script(self, script, working_dir, stdout_path, job_name,
                             walltime, run_script_path,
                             account, tmp_dir):
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
        f.write('#!/bin/sh\n#\n')
        f.write('#SBATCH --nodes=1\n')
        f.write('#SBATCH -A ' + account + '\n')
        f.write('#SBATCH -D ' + working_dir + '\n')
        f.write('#SBATCH -p compute\n')
        f.write('#SBATCH --export=SLURM_UMASK=0022\n')
        f.write('#SBATCH -o ' + stdout_path + '\n')
        f.write('#SBATCH -J ' + job_name + '\n')
        f.write('#SBATCH -t ' + walltime + '\n\n')

        f.write('echo "HOST: $HOSTNAME"\n')
        f.write('echo "DATE: `date`"\n\n')
        f.write('echo "JOBID: $SLURM_ARRAY_JOB_ID"\n')
        f.write('echo "TASKID: $SLURM_ARRAY_TASK_ID"\n\n')
        f.write('module load singularity/2.2\n\n')
        f.write('/usr/bin/time -v ' + run_script_path +
                ' $SLURM_ARRAY_TASK_ID ' + working_dir + ' --scratchdir ' +
                tmp_dir + ' --log DEBUG\n')
        f.write('\nexitcode=$?\n')
        f.write('echo "' + os.path.basename(run_script_path) +
                ' exited with code: $exitcode"\n')
        f.write('exit $exitcode\n')
        f.flush()
        f.close()
        os.chmod(script, stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH)
        return script


class ClusterFactory(object):
    """Factory that produces cluster objects based on cluster name
    """
    # WARNING
    VALID_CLUSTERS = [RocceCluster.CLUSTER,
                      GordonCluster.CLUSTER,
                      CometCluster.CLUSTER]

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

        if lc_cluster == GordonCluster.CLUSTER:
            logger.debug('returning GordonCluster')
            return GordonCluster(None)

        if lc_cluster == CometCluster.CLUSTER:
            logger.debug('returning CometCluster')
            return CometCluster(None)

        logger.error('No cluster class supporting ' + lc_cluster + ' found')
        return None


class SchedulerFactory(object):
    """Factory that produces Scheduler objects
    """
    COMET = 'comet'
    ROCCE = 'rocce'
    GORDON = 'gordon'

    def __init__(self):
        """Constructor"""
        pass

    def get_scheduler_by_cluster_name(self, clustername):
        """Gets Scheduler object by `clustername`
        :param clustername: name of cluster
        :returns: Scheduler object appropriate for cluster or None if none
                  found
        """
        if clustername is None:
            logger.error('clustername passed in is None')
            return None

        lc_cluster = clustername.lower()

        if lc_cluster == SchedulerFactory.ROCCE:
            logger.debug('Returning SGEScheduler for ' +
                         SchedulerFactory.ROCCE)
            return SGEScheduler(SchedulerFactory.ROCCE, queue='all.q')

        if lc_cluster == SchedulerFactory.GORDON:
            logger.debug('Returning PBSScheduler for ' +
                         SchedulerFactory.GORDON)
            return PBSScheduler(SchedulerFactory.GORDON, queue='normal',
                                load_singularity_cmd='module load '
                                                     'singularity/2.2\n')

        if lc_cluster == SchedulerFactory.COMET:
            logger.debug('Returning SLURMScheduler for ' +
                         SchedulerFactory.COMET)

            return SLURMScheduler(SchedulerFactory.COMET, queue='compute',
                                  load_singularity_cmd='module load '
                                                       'singularity/2.2\n')

        logger.error('No cluster class supporting ' + lc_cluster + ' found')
        return None


class Scheduler(object):
    """Base class for various schedulers
    """
    OUT_SUFFIX = '.out'

    def __init__(self, clustername,
                 queue=None,
                 account=None,
                 jobid_for_filepath=None,
                 jobid_for_arrayjob=None,
                 jobid=None,
                 taskid_for_filepath=None,
                 taskid=None,
                 submitcmd=None,
                 arrayflag=None,
                 load_singularity_cmd=None):
        """Constructor
        """
        self._clustername = clustername
        self._account = account
        self._queue = queue
        self._jobid_for_filepath_variable = jobid_for_filepath
        self._jobid_for_arrayjob_variable = jobid_for_arrayjob
        self._jobid_variable = jobid
        self._taskid_for_filepath_variable = taskid_for_filepath
        self._taskid_variable = taskid
        self._submitcmd = submitcmd
        self._arrayflag = arrayflag
        self._load_singularity_cmd = load_singularity_cmd

    def get_clustername(self):
        """Gets name"""
        return self._clustername

    def set_account(self, account):
        """Sets account to bill hours consumed on cluster
        """
        self._account = account

    def get_jobid_for_arrayjob_variable(self):
        """Gets jobid variable and will be replaced by
           scheduler for running array jobs
        """
        return self._jobid_for_arrayjob_variable

    def get_jobid_variable(self):
        """Gets jobid variable that will be replaced by
           scheduler for running array jobs
        """
        return self._jobid_variable

    def get_taskid_variable(self):
        """Gets taskid variable and will be replaced by
           scheduler for running array jobs
        """
        return self._taskid_variable

    def get_job_out_file_name(self):
        """Gets single job output filename <JOBID>.OUT_SUFFIX
        """
        return (str(self._jobid_for_filepath_variable) +
                Scheduler.OUT_SUFFIX)

    def get_array_job_out_file_name(self):
        """gets array job file name based on configuration passed in
        constructor
        :returns: String in format of <JOBID>.<TASKID>.OUT_SUFFIX with
                  values in <> set to
                  appropriate values for scheduler
        """
        return (str(self._jobid_for_filepath_variable) + '.' +
                str(self._taskid_for_filepath_variable) +
                Scheduler.OUT_SUFFIX)

    def _make_script_executable(self, script):
        """Sets execute all permission on script path passed in
        :param script: path to script
        """
        if os.path.exists(script):
            logger.debug('Making ' + script + ' executable')
            os.chmod(script, stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH)
        else:
            logger.error(script + ' does not exist, skipping permission '
                                  'change')

    def _generate_submit_command(self, script, working_dir, number_tasks):
        """Gets command to submit job
        """

        array_args = ' '
        if number_tasks is not None:
            if self._arrayflag is not None:
                array_args = (' ' + self._arrayflag +
                              ' 1-' + str(number_tasks) + ' ')

        if self._submitcmd is None:
            submitcmd = ' '
        else:
            submitcmd = self._submitcmd

        return ('To submit run: cd ' + working_dir + '; ' +
                submitcmd + array_args + script)

    def _get_script_header(self, working_dir, stdout_path, job_name,
                           walltime, required_mem_gb=None,
                           number_tasks=None):
        """Generates commands to put at top of submit script
        :param script: Full path to script to write out
        :param working_dir: Working directory
        :param stdout_path: Standard out file path for jobs.
                            ie ./$JOB_ID.$TASKID
        :param job_name: Job name ie foojob
        :param walltime: Maximum time job is allowed to run ie 12:00:00
        :param account: Account to bill processing to
        :param required_mem_gb: Ram in Gb required for job
        :param number_tasks: number of tasks in job
        :return: always returns empty string.
        """
        return ''

    def write_submit_script(self, scriptname, working_dir, stdout_path,
                            job_name, walltime, cmds, required_mem_gb=None,
                            number_tasks=None):
        """Generates submit script for a cluster
        :param script: Full path to script to write out
        :param working_dir: Working directory
        :param stdout_path: Standard out file path for jobs.
                            ie ./$JOB_ID.$TASKID
        :param job_name: Job name ie foojob
        :param walltime: Maximum time job is allowed to run ie 12:00:00
        :param cmds: Actual commands being run
        :param resource_reqs: Additional resource requirements for job
        :param number_tasks: number of tasks in job
        :raises IOError: If this method is unable to write the
                         submit script to a file
        :return: tuple (how to submit, path to submit script written)
        """
        if scriptname is None:
            raise InvalidScriptNameError('Script name cannot be None')

        if working_dir is None:
            raise InvalidWorkingDirError('Working dir cannot be None')
        script = os.path.join(working_dir, scriptname)
        f = open(script, 'w')
        f.write(self._get_script_header(working_dir, stdout_path,
                                        job_name, walltime,
                                        required_mem_gb=required_mem_gb,
                                        number_tasks=number_tasks))

        if self._load_singularity_cmd is not None:
            logger.debug('Load singularity command is not None '
                         'adding to submit script')
            f.write(self._load_singularity_cmd)

        f.write(cmds)
        f.flush()
        f.close()
        self._make_script_executable(script)
        return (self._generate_submit_command(script, working_dir,
                                              number_tasks),
                script)


class SLURMScheduler(Scheduler):
    """SLURM Scheduler
    """

    def __init__(self, clustername,
                 queue=None,
                 account=None,
                 load_singularity_cmd=None):
        super(SLURMScheduler,
              self).__init__(clustername,
                             queue=queue,
                             account=account,
                             jobid_for_filepath='%A',
                             jobid_for_arrayjob='$SLURM_ARRAY_JOB_ID',
                             jobid='$SLURM_JOB_ID',
                             taskid_for_filepath='%a',
                             taskid='$SLURM_ARRAY_TASK_ID',
                             submitcmd='sbatch',
                             arrayflag='-a',
                             load_singularity_cmd=load_singularity_cmd)

    def _get_script_header(self, working_dir, stdout_path, job_name,
                           walltime, required_mem_gb=None,
                           number_tasks=None):
        """Generates commands to put at top of submit script
        :param working_dir: Working directory
        :param stdout_path: Standard out file path for jobs.
                            ie ./$JOB_ID.$TASKID
        :param job_name: Job name ie foojob
        :param walltime: Maximum time job is allowed to run ie 12:00:00
        :param account: Account to bill processing to
        :param required_mem_gb: Ram in Gb required for job
        :param number_tasks: number of tasks in job
        :return: string container commands to put at top of submit script
        """
        res = '#!/bin/sh\n#\n#SBATCH --nodes=1\n'

        if self._account is not None:
            res += '#SBATCH -A ' + self._account + '\n'
        else:
            logger.debug('Account is None, not setting')

        if working_dir is not None:
            res += '#SBATCH -D ' + working_dir + '\n'

        if self._queue is not None:
            res += '#SBATCH -p ' + self._queue + '\n'
        else:
            logger.debug('Queue is None, not setting')

        res += '#SBATCH --export=SLURM_UMASK=0022\n'
        if stdout_path is not None:
            res += '#SBATCH -o ' + stdout_path + '\n'
        if job_name is not None:
            res += '#SBATCH -J ' + job_name + '\n'

        if walltime is not None:
            res += '#SBATCH -t ' + walltime + '\n'

        res += 'echo "HOST: $HOSTNAME"\n'
        res += 'echo "DATE: `date`"\n'
        res += 'echo "JOBID: $SLURM_JOB_ID"\n\n'

        return res


class SGEScheduler(Scheduler):
    """SGE Scheduler
    """

    def __init__(self, clustername, queue=None, account=None,
                 load_singularity_cmd=None):
        super(SGEScheduler,
              self).__init__(clustername,
                             queue=queue,
                             account=account,
                             jobid_for_filepath='$JOB_ID',
                             jobid_for_arrayjob='$JOB_ID',
                             jobid='$JOB_ID',
                             taskid_for_filepath='$TASK_ID',
                             taskid='$SGE_TASK_ID',
                             submitcmd='qsub',
                             arrayflag='-t',
                             load_singularity_cmd=load_singularity_cmd)

    def _get_script_header(self, working_dir, stdout_path, job_name,
                           walltime, required_mem_gb=None,
                           number_tasks=None):
        """Generates commands to put at top of submit script
        :param working_dir: Working directory
        :param stdout_path: Standard out file path for jobs.
                            ie ./$JOB_ID.$TASKID
        :param job_name: Job name ie foojob
        :param walltime: Maximum time job is allowed to run ie 12:00:00
        :param account: Account to bill processing to
        :param required_mem_gb: Ram in Gb required for job
        :param number_tasks: number of tasks in job
        :return: string container commands to put at top of submit script
        """
        res = '#!/bin/sh\n#$ -V\n#$ -S /bin/sh\n#$ -notify\n'

        if working_dir is not None:
            res += '#$ -wd ' + working_dir + '\n'

        if self._queue is not None:
            res += '#$ -q ' + self._queue + '\n'
        else:
            logger.debug('Queue is None, not setting')

        if stdout_path is not None:
            res += '#$ -j y\n#$ -o ' + stdout_path + '\n'

        if job_name is not None:
            res += '#$ -N ' + job_name + '\n'

        if walltime is None:
            comma_delim = ''
            walltime_reqs = ''
        else:
            comma_delim = ','
            walltime_reqs = 'h_rt=' + walltime

        if required_mem_gb is not None:
            resource_reqs = (comma_delim + 'h_vmem=' + str(required_mem_gb) +
                             'G,virtual_free=' + str(required_mem_gb) + 'G')
        else:
            resource_reqs = ''

        if walltime_reqs is not '' or resource_reqs is not '':
            res += '#$ -l ' + walltime_reqs + resource_reqs

        res += '\n\n'
        res += 'echo "HOST: $HOSTNAME"\n'
        res += 'echo "DATE: `date`"\n'
        res += 'echo "JOBID: $JOB_ID"\n\n'

        return res


class PBSScheduler(Scheduler):
    """PBS Scheduler
    """

    def __init__(self, clustername, queue=None, account=None,
                 load_singularity_cmd=None):
        super(PBSScheduler,
              self).__init__(clustername,
                             queue=queue,
                             account=account,
                             jobid_for_filepath='$PBS_JOBID',
                             jobid_for_arrayjob='$PBS_JOBID',
                             jobid='$PBS_JOBID',
                             taskid_for_filepath='$PBS_ARRAYID',
                             taskid='$PBS_ARRAYID',
                             submitcmd='qsub',
                             arrayflag=None,
                             load_singularity_cmd=load_singularity_cmd)

    def _get_script_header(self, working_dir, stdout_path, job_name,
                           walltime, required_mem_gb=None,
                           number_tasks=None):
        """Generates commands to put at top of submit script
        :param working_dir: Working directory
        :param stdout_path: Standard out file path for jobs.
                            ie ./$JOB_ID.$TASKID
        :param job_name: Job name ie foojob
        :param walltime: Maximum time job is allowed to run ie 12:00:00
        :param account: Account to bill processing to
        :param required_mem_gb: Ram in Gb required for job
        :param number_tasks: number of tasks in job
        :return: string container commands to put at top of submit script
        """
        res = '#!/bin/sh\n#\n#PBS -V\n#PBS -m n\n'

        if working_dir is not None:
            res += '#PBS -wd ' + working_dir + '\n'

        if self._queue is not None:
            res += '#PBS -q ' + self._queue + '\n'
        else:
            logger.debug('Queue is None, not setting')
        if self._account is not None:
            res += '#PBS -A ' + self._account + '\n'

        res += '#PBS -l nodes=1:ppn=16:native:noflash\n'

        if number_tasks is not None:
            res += ('#PBS -t 1-' + str(number_tasks) + '\n')

        if stdout_path is not None:
            res += '#PBS -j oe\n#PBS -o ' + stdout_path + '\n'

        if job_name is not None:
            res += '#PBS -N ' + job_name + '\n'

        if walltime is not None:
            res += '#PBS -l walltime=' + walltime + '\n'

        res += 'echo "HOST: $HOSTNAME"\n'
        res += 'echo "DATE: `date`"\n'
        res += 'echo "JOBID: $PBS_JOBID"\n\n'

        return res
