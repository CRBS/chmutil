# -*- coding: utf-8 -*-


import os
import datetime
import logging
import configparser
from configparser import NoOptionError
import shlex
import subprocess
import time
from PIL import Image

import chmutil

logger = logging.getLogger(__name__)


class OverlapTooLargeForTileSizeError(Exception):
    """Raised when overlap used is to large for overlap
    """
    pass


class LoadConfigError(Exception):
    """Raised when there is an error reading the CHM job config
    """
    pass


class InvalidJobDirError(Exception):
    """Raised when invalid job directory is passed in or used
    """
    pass


class InvalidImageDirError(Exception):
    """Raised when invalid image directory is used
    """
    pass


class InvalidCommaDelimitedStringError(Exception):
    """Raised when box is unable to parse a string"""
    pass


class Parameters(object):
    """Placeholder class for parameters
    """
    pass


class SingularityAbortError(Exception):
    """Raised if Singularity aborted"""
    pass


def setup_logging(thelogger,
                  log_format='%(asctime)-15s %(levelname)s %(name)s '
                             '%(message)s',
                  loglevel='WARNING'):
    """Sets up logging
    """
    if loglevel == 'DEBUG':
        numericloglevel = logging.DEBUG
    if loglevel == 'INFO':
        numericloglevel = logging.INFO
    if loglevel == 'WARNING':
        numericloglevel = logging.WARNING
    if loglevel == 'ERROR':
        numericloglevel = logging.ERROR
    if loglevel == 'CRITICAL':
        numericloglevel = logging.CRITICAL

    thelogger.setLevel(numericloglevel)
    logging.basicConfig(format=log_format)
    logging.getLogger('chmutil.core').setLevel(numericloglevel)
    logging.getLogger('chmutil.cluster').setLevel(numericloglevel)
    logging.getLogger('chmutil.image').setLevel(numericloglevel)


def add_standard_parameters(parser):
    """Adds some common flags to `ArgumentParser` passed in
    :param parser: ArgumentParser object
    :returns: ArgumentParser with common fields added
    """
    parser.add_argument("--scratchdir", help='Scratch Directory '
                                             '(default /tmp)',
                        default='/tmp')
    parser.add_argument("--log", dest="loglevel", choices=['DEBUG',
                        'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help="Set the logging level (default WARNING)",
                        default='WARNING')
    parser.add_argument('--version', action='version',
                        version=('%(prog)s ' + chmutil.__version__))
    return


def run_external_command(cmd_to_run, tmp_dir,
                         polling_sleep_time=1):
    """Runs command passed in
    :param cmd_to_run: command with arguments to run set as a string
    :param tmp_dir: temporary directory where stdout.txt and stderr.txt
                    files can be temporarily written
    :param polling_sleep_time: time in seconds to sleep between checks for
                               command completion.
    :returns: tuple containing (exit code, stdout, stderr)
    """
    if cmd_to_run is None:
        return 256, '', 'Command must be set'

    if tmp_dir is None:
        return 255, '', 'Tmpdir must be set'

    if not os.path.isdir(tmp_dir):
        return 254, '', 'Tmpdir must be a directory'

    logger.info("Running command " + cmd_to_run)
    tmp_stdout = os.path.join(tmp_dir, 'stdout.txt')
    stdout_f = open(tmp_stdout, 'w')
    tmp_stderr = os.path.join(tmp_dir, 'stderr.txt')
    stderr_f = open(tmp_stderr, 'w')
    p = subprocess.Popen(shlex.split(cmd_to_run),
                         stdout=stdout_f,
                         stderr=stderr_f)

    logger.debug('Waiting for process to complete')
    p.poll()
    logger.debug('Polling returned None, looping until'
                 ' p.returncode is not None')
    while p.returncode is None:
        time.sleep(polling_sleep_time)
        p.poll()

    logger.debug('Got a return code that is not None: ' +
                 str(p.returncode))
    stdout_f.flush()
    stdout_f.close()
    stderr_f.flush()
    stderr_f.close()
    logger.debug('Writing stderr and stdout to ' + tmp_dir)
    fo = open(tmp_stdout, 'r')
    out = fo.read()
    fo.close()
    fe = open(tmp_stderr, 'r')
    err = fe.read()
    fe.close()

    return p.returncode, out, err


def wait_for_children_to_exit(process_list):
    """Waits for children processes in process_list to finish
    """
    exit_code = 0

    if process_list is None:
        logger.info('None passed into wait_for_children_to_exit()')
        return exit_code

    running_procs = len(process_list)
    while running_procs > 0:
        logger.info('Still waiting on ' + str(running_procs) + ' processes')
        logger.debug('Waiting on pid: ' + str(process_list[0]))
        try:
            ecode = os.waitpid(process_list[0], 0)[1]
            if ecode > 255:
                ecode = ecode >> 8
            logger.info('Process ' + str(process_list[0]) +
                        ' exited with code: ' + str(ecode))
            exit_code += ecode
        except OSError:
            logger.exception('Caught exception, but it might not '
                             'be a big deal')
        time.sleep(1)
        del process_list[0]
        running_procs = len(process_list)
    return exit_code


def get_image_path_list(image_dir, suffix,
                        keysortfunc=None):
    """Gets list of images with suffix from dir
    :param image_dir: Path to directory with images
    :param suffix: Only include files ending with suffix.
                   code uses str().endswidth for checking.
                   If `suffix` is None then all files match
    :raises InvalidImageDirError: if `image_dir` is None or not
                                  a directory
    :raises OSError: if there is an error invoking os.listdir on `image_dir`
    :returns: list of file paths
    """
    if image_dir is None:
        raise InvalidImageDirError('image_dir is None')

    if not os.path.isdir(image_dir):
        raise InvalidImageDirError('image_dir must be a directory')

    if suffix is None:
        adjusted_suffix = ''
    else:
        adjusted_suffix = suffix

    img_list = []
    for entry in os.listdir(image_dir):
        fp = os.path.join(image_dir, entry)
        if not os.path.isfile(fp):
            logger.debug(entry + ' is not a file. skipping')
            continue
        if entry.endswith(adjusted_suffix):
            img_list.append(fp)

    if keysortfunc is not None:
        logger.debug('Sort function passed in sorting data')
        img_list.sort(key=keysortfunc)

    return img_list


def get_longest_sequence_of_numbers_in_string(val):
    """Given a string of characters return the
       longest string of numbers in that string as an int.
       Before search os.path.basename is applied to val
       to remove the path.

       Example:
       >> get_longest_sequene_of_numbers_in_string('bin1-3view-final.0254.png')
       >> 254

       :param val: string to examine
       :returns int: int of longest sequence of numbers in string.
                     If tie, then first encountered number is used.
                     If none or val is None return 0
    """
    if val is None:
        return 0

    max_digit_sequence = ''
    cur_val = ''
    for element in os.path.basename(val):
        if element.isdigit():
            cur_val += element
            continue

        if cur_val != '':
            if len(cur_val) > len(max_digit_sequence):
                max_digit_sequence = cur_val
        cur_val = ''

    if len(cur_val) > len(max_digit_sequence):
        max_digit_sequence = cur_val

    if max_digit_sequence == '':
        return 0

    return int(max_digit_sequence)


def get_first_sequence_of_numbers_in_string(val):
    """Given a string of characters return the
       first string of numbers in that string as an int.

       Example:
       >> get_first_sequence_of_numbers_in_string('bin1-3view-final.0254.png')
       >> 1

       :param val: string to examine
       :returns int: int of first sequence of numbers in string.
                     If none or val is None return 0
    """
    if val is None:
        return 0

    cur_val = ''
    for element in val:
        if element.isdigit():
            cur_val += element
            continue

        if cur_val != '':
            break

    if cur_val == '':
        return 0

    return int(cur_val)


class CHMJobCreator(object):
    """Creates CHM Job to run on cluster
    """
    JOB_DIR = 'jobdir'
    CONFIG_FILE_NAME = 'base.chm.tasks.list'
    CONFIG_BATCHED_TASKS_FILE_NAME = 'batched.chm.tasks.list'
    MERGE_CONFIG_FILE_NAME = 'base.merge.tasks.list'
    MERGE_CONFIG_BATCHED_TASKS_FILE_NAME = 'batched.merge.tasks.list'
    MERGE_INPUT_IMAGE_DIR = 'inputimagedir'
    MERGE_OUTPUT_IMAGE = 'outputimage'
    MERGE_OUTPUT_OVERLAY_IMAGE = 'overlayoutputimage'
    MERGE_MERGETILES_BIN = 'mergetilesbin'
    MERGE_TASKS_PER_NODE = 'mergetaskspernode'
    RUN_DIR = 'chmrun'
    STDOUT_DIR = 'stdout'
    TILES_DIR = 'tiles'
    MERGE_STDOUT_DIR = 'mergestdout'
    PROBMAPS_DIR = 'probmaps'
    OVERLAYMAPS_DIR = 'overlaymaps'
    TMP_DIR = 'tmp'
    CONFIG_DEFAULT = 'DEFAULT'
    CONFIG_CHM_BIN = 'chmbin'
    CONFIG_INPUT_IMAGE = 'inputimage'
    CONFIG_ARGS = 'args'
    CONFIG_OUTPUT_IMAGE = 'outputimage'
    CONFIG_IMAGES = 'images'
    CONFIG_MODEL = 'model'
    BCONFIG_TASK_ID = 'taskids'
    CONFIG_TILES_PER_TASK = 'tilespertask'
    CONFIG_TILE_SIZE = 'tilesize'
    CONFIG_OVERLAP_SIZE = 'overlapsize'
    CONFIG_DISABLE_HISTEQ_IMAGES = 'disablehisteqimages'
    CONFIG_TASKS_PER_NODE = 'taskspernode'
    CONFIG_ACCOUNT = 'account'
    CHMUTIL_VERSION = 'chmutilversion'
    CONFIG_CLUSTER = 'cluster'
    CHMRUNNER = 'chmrunner.py'
    MERGERUNNER = 'mergetilerunner.py'
    CHECKCHMJOB = 'checkchmjob.py'
    README_TXT_FILE = 'readme.txt'
    README_BODY = """chmutil job to run CHM jobs on cluster of computers
===========================================================

Chmutil version: {version}
Date: {date}

Contents of this directory were created by invocation of createchmjob.py with
the following command line:

{commandline}

To check job status invoke the following cluster specific command:

On Gordon and Rocce:

qstat -t -u "$USER"

On Comet:

squeue -u "$USER"

If all jobs have completed invoke the following to see if any more tasks
need to be run

checkjobstatus.py {jobdir}

If there are more tasks to run be sure to rerun above command with
'--submit' flag to update the batched.* configuration files and the
runjobs.CLUSTER/runmerge.CLUSTER files like so:

checkjobstatus.py {jobdir} --submit


For more help please visit wiki here:

https://github.com/CRBS/chmutil/wiki

For bugs/issues feel free to open a ticket here:

https://github.com/CRBS/chmutil/issues


Below is a description of data in this directory
================================================

base.chm.tasks.list
  -- Main configuration file for CHM job. Created when createchmjob.py is
     run and contains CHM tasks to run.

batched.chm.tasks.list
  -- Batched CHM task configuration file. This defines how the CHM
     tasks in base.chm.tasks.list are batched on individual compute nodes
     in the cluster. Created when checkchmjob.py --submitted is run.

base.merge.tasks.list
  -- Configuration of merge tasks. Created when createchmjob.py is run.

batched.merge.tasks.list
  -- Batched merge task configuration file. This defines how the merge
     tasks in base.merge.tasks.list  are batched on individual compute
     nodes in the cluster. Created when checkchmjob.py --submitted is run.

chmrun/
  -- Base directory where all job output is written. This directory will
     always be named this.

chmrun/mergestdout/
  -- Directory containing output from merge tasks. Merge tasks are directed
     to write to this path via runmerge.CLUSTER queue submit script file.

chmrun/overlaymaps/
  -- Directory containing output images that are an overlay of the merged
     probability maps on top of the original input images. These images are
     created when the merge tasks are run.

chmrun/overlaymaps/
  -- Directory containing output merged probability maps. These images are
     created when the merge tasks are run.

chmrun/stdout/
  -- Directory containing output from CHM tasks. CHM tasks are directed to
     write to this path via runjobs.CLUSTER queue submit script file.

chmrun/tiles/
  -- Directory containing directories for each input image. Within each of
     this input image directories the CHM tasks will write partial
     probability maps which will be merged by merge tasks in the SECOND
     phase of processing.

chmrun/tmp/
  -- Directory containing temp standard out and standard error files for
     chmrunner.py and mergetilerunner.py script. Both of these scripts will
     create a sub directory with format TASKID.UUID/ and under that directory
     will be a stderr.txt and a stdout.txt containing output from the actual
     merge and CHM tasks. Once the task completes this directory and files
     will be removed.

runjobs.CLUSTER
  -- CHM submit script file where CLUSTER will be set to gordon, comet, rocce.
     This file contains necessary flags for submission to the scheduler.

runmerge.CLUSTER
  -- Merge submit script file where CLUSTER will be set to gordon, comet,
     rocce. This file contains necessary flags for submission to the
     scheduler.
"""

    def __init__(self, chmopts):
        """Constructor
        """
        self._chmopts = chmopts

    def _create_config(self):
        """Creates configparser object and populates it with CHMOpts data
        :returns: configparser config object filled with CHMOpts data
        """
        config = configparser.ConfigParser()
        config.set('', CHMJobCreator.CHMUTIL_VERSION,
                   self._chmopts.get_version())
        config.set('', CHMJobCreator.JOB_DIR,
                   self._chmopts.get_out_dir())
        config.set('', CHMJobCreator.CONFIG_IMAGES,
                   self._chmopts.get_images())
        config.set('', CHMJobCreator.CONFIG_CHM_BIN,
                   self._chmopts.get_chm_binary())
        config.set('', CHMJobCreator.CONFIG_MODEL, self._chmopts.get_model())
        config.set('', CHMJobCreator.CONFIG_TILES_PER_TASK,
                   str(self._chmopts.get_number_tiles_per_task()))
        config.set('', CHMJobCreator.CONFIG_TILE_SIZE,
                   str(self._chmopts.get_tile_size()))
        config.set('', CHMJobCreator.CONFIG_OVERLAP_SIZE,
                   str(self._chmopts.get_overlap_size()))
        config.set('', CHMJobCreator.CONFIG_DISABLE_HISTEQ_IMAGES,
                   str(self._chmopts.get_disable_histogram_eq_val()))
        config.set('', CHMJobCreator.CONFIG_TASKS_PER_NODE,
                   str(self._chmopts.get_number_tasks_per_node()))
        config.set('', CHMJobCreator.CONFIG_ACCOUNT,
                   str(self._chmopts.get_account()))
        config.set('', CHMJobCreator.CONFIG_CLUSTER,
                   str(self._chmopts.get_cluster()))
        return config

    def _write_config(self, config):
        """Writes `config` to file
        :param config: configparser config object
        :returns: Path to written configuration file
        """
        cfile = os.path.join(self._chmopts.get_out_dir(),
                             CHMJobCreator.CONFIG_FILE_NAME)
        logger.debug('Writing config to : ' + cfile)
        f = open(cfile, 'w')
        config.write(f)
        f.flush()
        f.close()
        return cfile

    def _write_readme(self, config):
        """Writes out readme.txt file
        """
        readme = os.path.join(self._chmopts.get_out_dir(),
                              CHMJobCreator.README_TXT_FILE)

        rawargs = self._chmopts.get_raw_args()
        if rawargs is None:
            rawargs = 'Unknown'
        else:
            rawargs = 'cd ' + os.getcwd() + ';' + rawargs

        f = open(readme, 'w')
        f.write(CHMJobCreator.README_BODY.format(
            version=self._chmopts.get_version(),
            jobdir=self._chmopts.get_out_dir(),
            date=str(datetime.datetime.today()),
            commandline=rawargs))
        f.flush()
        f.close()

    def _create_merge_config(self):
        """Creates configparser object and populates it with CHMConfig data
        needed for merge job
        :returns: configparser config object filled with some data
        """
        config = configparser.ConfigParser()
        config.set('', CHMJobCreator.MERGE_MERGETILES_BIN,
                   os.path.join(self._chmopts.get_script_bin(),
                                'mergetiles.py'))
        config.set('', CHMJobCreator.CONFIG_IMAGES,
                   self._chmopts.get_images())
        config.set('', CHMJobCreator.JOB_DIR,
                   self._chmopts.get_out_dir())
        config.set('', CHMJobCreator.MERGE_TASKS_PER_NODE,
                   str(self._chmopts.get_number_merge_tasks_per_node()))
        config.set('', CHMJobCreator.CONFIG_CLUSTER,
                   str(self._chmopts.get_cluster()))
        return config

    def _write_merge_config(self, config):
        """Writes `config` to file to merge config
        :param config: configparser config object
        :returns: Path to written configuration file
        """
        cfile = os.path.join(self._chmopts.get_out_dir(),
                             CHMJobCreator.MERGE_CONFIG_FILE_NAME)
        logger.debug('Writing config to : ' + cfile)
        f = open(cfile, 'w')
        config.write(f)
        f.flush()
        f.close()
        return cfile

    def _create_output_image_dir(self, imagestats, run_dir):
        """Creates directory where CHM output images will be written
        :param imagestats: ImageStats for image to create directory for
        :param rundir: Base run directory for CHM job
        :returns: tuple (output image directory, filename of image
        form `imagestats`)
        """
        i_name = os.path.basename(imagestats.get_file_path())
        i_dir = os.path.join(run_dir, CHMJobCreator.TILES_DIR, i_name)
        if os.path.isdir(i_dir) is False:
            logger.debug('Creating image dir ' + i_dir)
            os.makedirs(i_dir, mode=0o775)
        return i_name

    def _create_run_dir(self):
        """Creates CHM job run directory
        :returns: Path to run directory
        """
        run_dir = os.path.join(self._chmopts.get_out_dir(),
                               CHMJobCreator.RUN_DIR)
        if os.path.isdir(run_dir) is False:
            logger.debug('Creating run dir ' + run_dir)
            os.makedirs(run_dir, mode=0o775)
            os.makedirs(os.path.join(run_dir, CHMJobCreator.STDOUT_DIR),
                        mode=0o775)
            os.makedirs(os.path.join(run_dir, CHMJobCreator.MERGE_STDOUT_DIR),
                        mode=0o775)
            os.makedirs(os.path.join(run_dir, CHMJobCreator.TMP_DIR),
                        mode=0o775)
            os.makedirs(os.path.join(run_dir, CHMJobCreator.PROBMAPS_DIR),
                        mode=0o775)
            os.makedirs(os.path.join(run_dir, CHMJobCreator.OVERLAYMAPS_DIR),
                        mode=0o775)
            os.makedirs(os.path.join(run_dir, CHMJobCreator.TILES_DIR),
                        mode=0o775)

        return run_dir

    def _add_task_for_image_to_config(self, config, counter_as_str,
                                      i_name, img_cntr, theargs):
        """Adds job to config object
        :param config: configparser config object to add job to
        :param counter_as_str: Counter used in string form
        :param imagestats: `ImageStats` for job
        :param i_name: Name of image
        :param img_cntr: Image counter
        :param theargs: args for CHM job
        """
        config.add_section(counter_as_str)
        config.set(counter_as_str, CHMJobCreator.CONFIG_INPUT_IMAGE,
                   i_name)
        config.set(counter_as_str, CHMJobCreator.CONFIG_ARGS,
                   ' '.join(theargs))
        config.set(counter_as_str, CHMJobCreator.CONFIG_OUTPUT_IMAGE,
                   os.path.join(CHMJobCreator.TILES_DIR, i_name,
                                str(img_cntr).zfill(3) + '.' + i_name))

    def _add_mergetask_for_image_to_config(self, config, counter_as_str,
                                           image_name):
        """Adds merge job to config object
        :param config: configparser config object to add merge job to
        :param counter_as_str: Counter used in string form
        :param image_tile_dir: Directory where image tiles of probmaps are put
        :param image_name: name of image the tiles correspond to
        """
        config.add_section(counter_as_str)
        config.set(counter_as_str, CHMJobCreator.MERGE_INPUT_IMAGE_DIR,
                   os.path.join(CHMJobCreator.TILES_DIR, image_name))
        config.set(counter_as_str, CHMJobCreator.MERGE_OUTPUT_IMAGE,
                   os.path.join(CHMJobCreator.PROBMAPS_DIR,
                                image_name))
        config.set(counter_as_str,
                   CHMJobCreator.MERGE_OUTPUT_OVERLAY_IMAGE,
                   os.path.join(CHMJobCreator.OVERLAYMAPS_DIR,
                                image_name))

    def create_job(self):
        """Creates jobs
        """
        arg_gen = CHMArgGenerator(self._chmopts)
        statsfac = ImageStatsFromDirectoryFactory(self._chmopts.get_images(),
                                                  max_image_pixels=self.
                                                  _chmopts.
                                                  get_max_image_pixels())
        imagestats = statsfac.get_input_image_stats()
        config = self._create_config()
        mergeconfig = self._create_merge_config()
        counter = 1
        mergecounter = 1
        run_dir = self._create_run_dir()

        for iis in imagestats:
            i_name = self._create_output_image_dir(iis, run_dir)
            img_cntr = 1
            self._add_mergetask_for_image_to_config(mergeconfig,
                                                    str(mergecounter),
                                                    i_name)
            for a in arg_gen.get_args(iis):
                counter_as_str = str(counter)
                self._add_task_for_image_to_config(config, counter_as_str,
                                                   i_name, img_cntr, a)
                counter += 1
                img_cntr += 1
            mergecounter += 1
        self._write_config(config)
        self._write_merge_config(mergeconfig)
        self._chmopts.set_config(config)
        self._chmopts.set_merge_config(mergeconfig)
        self._write_readme(config)

        return self._chmopts


class CHMConfig(object):
    """Contains options for CHM parameters
    """
    def __init__(self, images, model, outdir,
                 tile_size,
                 overlap_size,
                 number_tiles_per_task=1,
                 tasks_per_node=1,
                 merge_tasks_per_node=1,
                 disablehisteq=True,
                 chmbin='./chm-0.1.0.img',
                 scriptbin='',
                 jobname='chmjob',
                 mergejobname='mergechmjob',
                 walltime='12:00:00',
                 mergewalltime='12:00:00',
                 max_image_pixels=768000000,
                 max_chm_memory_in_gb=10,
                 max_merge_memory_in_gb=10,
                 version='unknown',
                 cluster='rocce',
                 account='',
                 config=None,
                 mergeconfig=None,
                 rawargs=None):
        """Constructor
        """
        self._images = images
        self._model = model
        self._outdir = outdir
        self._tile_size = tile_size
        self._overlap_size = overlap_size
        self._parse_and_set_tile_width_height(tile_size)
        self._parse_and_set_overlap_width_height(overlap_size)
        self._number_tiles_per_task = number_tiles_per_task
        self._tasks_per_node = tasks_per_node
        self._disablehisteq = disablehisteq
        self._chmbin = chmbin
        self._scriptbin = scriptbin
        self._jobname = jobname
        self._mergejobname = mergejobname
        self._mergewalltime = mergewalltime
        self._walltime = walltime
        self._max_image_pixels = max_image_pixels
        self._max_chm_memory_in_gb = max_chm_memory_in_gb
        self._max_merge_memory_in_gb = max_merge_memory_in_gb
        self._account = account
        self._version = version
        self._config = config
        self._mergeconfig = mergeconfig
        self._merge_tasks_per_node = merge_tasks_per_node
        self._cluster = cluster
        self._rawargs = rawargs

    def _extract_width_and_height(self, val):
        """parses WxH value into tuple
        """
        if val is None or val == '':
            return '', ''
        sval = str(val).split('x')

        if len(sval) == 1:
            return int(sval[0]), int(sval[0])
        return int(sval[0]), int(sval[1])

    def _parse_and_set_tile_width_height(self, tile_size):
        """parses out tile width and height
        """
        w, h = self._extract_width_and_height(tile_size)
        self._tile_width = w
        self._tile_height = h
        return

    def _parse_and_set_overlap_width_height(self, o_size):
        """parses out overlap width and height
        """
        w, h = self._extract_width_and_height(o_size)
        self._overlap_width = w
        self._overlap_height = h
        return

    def get_raw_args(self):
        """Gets raw arguments passed to createchmjob.py
        """
        return self._rawargs

    def get_account(self):
        """Gets account to charge processing to
        """
        return self._account

    def get_cluster(self):
        """Gets the cluster the CHM job is running on
        """
        return self._cluster

    def get_max_chm_memory_in_gb(self):
        """Gets maximum memory a CHM job will use
        :return: Maximum memory in gigabytes a chm job will use
        """
        return self._max_chm_memory_in_gb

    def get_max_merge_memory_in_gb(self):
        """Gets maximum memory a merge job will use
        :return: Maximum memory in gigabytes a merge job will use
        """
        return self._max_merge_memory_in_gb

    def get_version(self):
        """Gets version of chmutil
        """
        return self._version

    def get_max_image_pixels(self):
        """Gets maximum image pixels that dictates
        the largest an image can be analyzed without error
        """
        return self._max_image_pixels

    def get_walltime(self):
        """gets job walltime
        """
        return self._walltime

    def get_merge_walltime(self):
        """gets merge job walltime
        """
        return self._mergewalltime

    def get_job_name(self):
        """gets name of job
        """
        return self._jobname

    def get_mergejob_name(self):
        """gets name of merge job
        """
        return self._mergejobname

    def get_script_bin(self):
        """Gets bin directory where chmutil scripts
           reside
        """
        return self._scriptbin

    def set_merge_config(self, config):
        """SEts merge config
        """
        self._mergeconfig = config

    def get_merge_config(self):
        """Gets merge config
        """
        return self._mergeconfig

    def set_config(self, config):
        """Sets config
        """
        self._config = config

    def get_config(self):
        """Gets configparser config if set in constructor
        """
        return self._config

    def get_job_config(self):
        """Gets path to job config file
        """
        if self.get_out_dir() is None:
            return CHMJobCreator.CONFIG_FILE_NAME
        return os.path.join(self.get_out_dir(), CHMJobCreator.CONFIG_FILE_NAME)

    def get_batchedjob_config_file_path(self):
        """Gets path to batched job config
        """
        if self.get_out_dir() is None:
            return CHMJobCreator.CONFIG_BATCHED_TASKS_FILE_NAME
        return os.path.join(self.get_out_dir(),
                            CHMJobCreator.CONFIG_BATCHED_TASKS_FILE_NAME)

    def get_batched_mergejob_config_file_path(self):
        """Gets path to batched merge job config
        """
        if self.get_out_dir() is None:
            return CHMJobCreator.MERGE_CONFIG_BATCHED_TASKS_FILE_NAME
        return os.path.join(self.get_out_dir(),
                            CHMJobCreator.MERGE_CONFIG_BATCHED_TASKS_FILE_NAME)

    def get_disable_histogram_eq_val(self):
        """gets boolean to indicate whether chm should
        perform histogram equalization
        """
        return self._disablehisteq

    def get_tile_size(self):
        """gets raw block size
        """
        return self._tile_size

    def get_overlap_size(self):
        """gets raw overlap size
        """
        return self._overlap_size

    def get_images(self):
        """gets images
        """
        return self._images

    def get_model(self):
        """gets model
        """
        return self._model

    def get_out_dir(self):
        """gets outdir
        """
        return self._outdir

    def get_run_dir(self):
        """gets run dir
        """
        if self.get_out_dir() is None:
            return CHMJobCreator.RUN_DIR

        return os.path.join(self.get_out_dir(),
                            CHMJobCreator.RUN_DIR)

    def get_stdout_dir(self):
        """gets stdout dir
        """
        return os.path.join(self.get_run_dir(), CHMJobCreator.STDOUT_DIR)

    def get_merge_stdout_dir(self):
        """gets merge stdout dir
        """
        return os.path.join(self.get_run_dir(), CHMJobCreator.MERGE_STDOUT_DIR)

    def get_shared_tmp_dir(self):
        """gets shared tmp dir
        """
        return os.path.join(self.get_run_dir(), CHMJobCreator.TMP_DIR)

    def get_tile_width(self):
        """gets tile width
        """
        return self._tile_width

    def get_tile_height(self):
        """gets tile width
        """
        return self._tile_height

    def get_overlap_width(self):
        """gets tile width
        """
        return self._overlap_width

    def get_overlap_height(self):
        """gets tile width
        """
        return self._overlap_height

    def get_number_tiles_per_task(self):
        """returns number of tiles per job
        """
        return self._number_tiles_per_task

    def get_number_tasks_per_node(self):
        """gets desired number tasks per node
        """
        return self._tasks_per_node

    def get_number_merge_tasks_per_node(self):
        """gets desired number merge tasks per node
        """
        return self._merge_tasks_per_node

    def get_chm_binary(self):
        """gets path to chm binary
        """
        return self._chmbin


class CHMConfigFromConfigFactory(object):
    """Creates CHMOpts object from configuration file
    """
    def __init__(self, job_dir):
        """Constructor
        :param job_dir: Directory containing job config file
        :raises InvalidJobDirError: if job_dir passed in is None
        """
        if job_dir is None:
            raise InvalidJobDirError('job directory passed in cannot be null')
        self._job_dir = job_dir

    def _get_config(self, cfile):
        """Gets configpaser.COnfigParser from configuration file
        """
        config = configparser.ConfigParser()

        if not os.path.isfile(cfile):
            raise LoadConfigError(cfile + ' configuration file does not exist')

        config.read(cfile)
        return config

    def _get_chmutil_version(self, config):
        """Attempts to get chmutil version from config
           :returns: version as a string or unknown if not found
        """
        try:
            return config.get(CHMJobCreator.CONFIG_DEFAULT,
                              CHMJobCreator.CHMUTIL_VERSION)
        except NoOptionError:
            return 'unknown'

    def get_chmconfig(self, skip_loading_config=False,
                      skip_loading_mergeconfig=True):
        """Gets CHMOpts from configuration within `job_dir` passed into
        constructor
        :raises LoadConfigError: if no configuration file is found
        :returns: CHMConfig configured from configuration in `job_dir`
                  passed into constructor
        """
        if skip_loading_config is False:
            config = self._get_config(cfile=os.path.join(self._job_dir,
                                                         CHMJobCreator.
                                                         CONFIG_FILE_NAME))
        else:
            logger.debug('Skipping load of job configuration')
            config = None

        default = CHMJobCreator.CONFIG_DEFAULT

        if skip_loading_mergeconfig is False:
            mergecon = self._get_config(os.path.join(self._job_dir,
                                                     CHMJobCreator.
                                                     MERGE_CONFIG_FILE_NAME))
            merge_t_node = mergecon.getint(default,
                                           CHMJobCreator.MERGE_TASKS_PER_NODE)
        else:
            logger.debug('Skipping load of merge job configuration')
            mergecon = None
            merge_t_node = 1

        if config is None:
            logger.debug('Config is None')

            if mergecon is not None:

                cluster = mergecon.get(default,
                                       CHMJobCreator.CONFIG_CLUSTER)
                logger.debug('Setting cluster to ' + str(cluster))
                return CHMConfig(None, None, self._job_dir,
                                 None, None, cluster=cluster,
                                 mergeconfig=mergecon)

            logger.error('Mergeconfig is None')
            return CHMConfig(None, None, self._job_dir,
                             None, None, mergeconfig=mergecon)

        disablehisteq = config.getboolean(default,
                                          CHMJobCreator.
                                          CONFIG_DISABLE_HISTEQ_IMAGES)

        cluster = config.get(default,
                             CHMJobCreator.CONFIG_CLUSTER)

        tiles_per_task = config.get(default,
                                    CHMJobCreator.CONFIG_TILES_PER_TASK)

        account = ''
        if config.has_option(default, CHMJobCreator.CONFIG_ACCOUNT):
            account = config.get(default, CHMJobCreator.CONFIG_ACCOUNT)
            logger.debug('account found in config: ' + str(account))

        opts = CHMConfig(config.get(default, CHMJobCreator.CONFIG_IMAGES),
                         config.get(default, CHMJobCreator.CONFIG_MODEL),
                         self._job_dir,
                         config.get(default, CHMJobCreator.CONFIG_TILE_SIZE),
                         config.get(default, CHMJobCreator.
                                    CONFIG_OVERLAP_SIZE),
                         number_tiles_per_task=tiles_per_task,
                         tasks_per_node=config.get(default, CHMJobCreator.
                                                   CONFIG_TASKS_PER_NODE),
                         disablehisteq=disablehisteq,
                         chmbin=config.get(default, CHMJobCreator.
                                           CONFIG_CHM_BIN),
                         version=self._get_chmutil_version(config),
                         merge_tasks_per_node=merge_t_node,
                         cluster=cluster,
                         config=config,
                         account=account,
                         mergeconfig=mergecon)
        return opts


class ImageStats(object):
    """Contains information about an image to be segmented
    """
    def __init__(self, path, width, height, format):
        """Constructor
        """
        self._path = path
        self._width = width
        self._height = height
        self._format = format

    def get_width(self):
        """Gets width
        """
        return self._width

    def get_height(self):
        """Gets height
        """
        return self._height

    def get_file_path(self):
        """Gets path to image
        """
        return self._path

    def get_format(self):
        """
        :return:
        """
        return self._format


class ImageStatsFromDirectoryFactory(object):
    """Creates ImageStats objects from directory of images
    """

    def __init__(self, directory, max_image_pixels=768000000):
        """Constructor
        """
        self._directory = directory
        logger.debug('Setting MAX_IMAGE_PIXELS to ' + str(max_image_pixels))
        Image.MAX_IMAGE_PIXELS = max_image_pixels

    def get_input_image_stats(self,
                              keysortfunc=None):
        """Gets InputImageStats objects as list
        """
        image_stats_list = []
        if os.path.isfile(self._directory):
            return []
        file_list = get_image_path_list(self._directory,
                                        None)
        for fp in file_list:
            im = None
            try:
                im = Image.open(fp)
                iis = ImageStats(fp, im.size[0],
                                 im.size[1], im.format)
                image_stats_list.append(iis)
            except Exception:
                logger.exception('Skipping file unable to open ' + fp)
            finally:
                try:
                    im.close()
                except Exception:
                    logger.exception('Caught exception attempting '
                                     'to close image')

        return image_stats_list


class CHMArgGenerator(object):
    """Generates tile args consumable by CHM 2.1.367
    """
    def __init__(self, chmopts):
        """Constructor
        """
        self._chmopts = chmopts
        self._t_width_w_over = (self._chmopts.get_tile_width() -
                                (2 * self._chmopts.get_overlap_width()))

        if self._t_width_w_over <= 0:
            raise OverlapTooLargeForTileSizeError('Overlap width too large '
                                                  'for tile')

        self._t_height_w_over = (self._chmopts.get_tile_height() -
                                 (2 * self._chmopts.get_overlap_height()))

        if self._t_height_w_over <= 0:
            raise OverlapTooLargeForTileSizeError('Overlap height too large '
                                                  'for tile')

    def get_args(self, image_stats):
        """Creates a list of tile args
        """
        (tiles_w, tiles_h) = self._get_number_of_tiles_tuple(image_stats)
        tile_list = []
        for c in range(1, int(tiles_w + 1)):
            for r in range(1, int(tiles_h + 1)):
                tile_list.append('-t ' + str(c) + ',' + str(r))
        total = tiles_w * tiles_h
        split_list = []
        t_per_job = self._chmopts.get_number_tiles_per_task()

        for ts in range(0, total, t_per_job):
            split_list.append(tile_list[ts:ts+t_per_job])

        return split_list

    def _get_number_of_tiles_tuple(self, image_stats):
        """Gets number of tiles needed in horizontal and vertical
           directions to analyze an image
        """
        tiles_width = (image_stats.get_width() +
                       self._t_width_w_over - 1) / self._t_width_w_over

        tiles_height = (image_stats.get_height() +
                        self._t_height_w_over - 1) / self._t_height_w_over

        return int(tiles_width), int(tiles_height)


class Box(object):
    """Represents a box used in Pillow image library
    """
    COMMA = ','

    def __init__(self, left=None, upper=None, right=None, lower=None):
        """constructor"""
        self._left = left
        self._upper = upper
        self._right = right
        self._lower = lower

    def are_any_corners_none(self):
        """Checks if any of the corner coordinates are None
        :returns: True if yes otherwise False
        """
        if self._left is None or self._upper is None:
            return True

        if self._right is None or self._lower is None:
            return True

        return False

    def load_from_comma_delimited_string(self, string):
        """Parses comma delimited string to set values of object
        :param string: comma delimited string in this format
                       left,upper,right,lower with each value
                       convertible to an int
        :raises InvalidCommaDelimitedStringError: if splitting does not result
                in 4 elements that are assumed to be the box coordinates
        :raises ValueError: If any of elements cannot be converted to int
        """
        if string is None:
            raise InvalidCommaDelimitedStringError('string is None')

        elements = string.split(Box.COMMA)
        if len(elements) != 4:
            raise InvalidCommaDelimitedStringError('string does not have 4 '
                                                   'elements when parsed: ' +
                                                   string)

        self._left = int(elements[0])
        self._upper = int(elements[1])
        self._right = int(elements[2])
        self._lower = int(elements[3])

    def get_box_as_tuple(self):
        """Gets box as tuple
        :returns: Tuple with 4 values (left, upper, right, lower)
        """
        return (self._left, self._upper,
                self._right, self._lower)

    def get_list_of_tuple_of_corner_coordinates(self):
        """Gets Box as tuple of x,y coordinates
        :returns: list of tuples of corner coordinates in format
                  [(left,upper),(left,lower),(right,upper),(right,lower)]
        """
        if self.are_any_corners_none():
            return None

        return [(self._left, self._upper),
                (self._left, self._lower),
                (self._right, self._upper),
                (self._right, self._lower)]

    def get_box_as_comma_delimited_string(self):
        """Returns object as a string of values delimited by commas
        :returns: string of positions delimited by commas in this order
                  left,upper,right,lower
        """
        return (str(self._left) + Box.COMMA + str(self._upper) + Box.COMMA +
                str(self._right) + Box.COMMA + str(self._lower))

    def is_coordinate_in_box(self, coordinate_tuple):
        """Checks if coordinate is in box
        :param coordinate_tuple: tuple containing 2 int coordinates (x, y)
        :returns: True if yes otherwise False
        :raises ValueError: if values in `coordinate_tuple` are not of type int
        """
        if coordinate_tuple is None:
            return False

        x = int(coordinate_tuple[0])
        y = int(coordinate_tuple[1])
        if self.are_any_corners_none() is True:
            return False

        if self._left <= x <= self._right:
            if self._upper <= y <= self._lower:
                return True

        return False

    def does_box_intersect(self, box):
        """Checks if two Box's intersect
        :param box: Box to check
        :return: True if they intersect, False otherwise
        """

        if self.are_any_corners_none() is True:
            return False

        if box.are_any_corners_none() is True:
            return False

        # check for overlap and case where box passed in
        # could be within this box
        c_list = box.get_list_of_tuple_of_corner_coordinates()
        for c_tup in c_list:
            if self.is_coordinate_in_box(c_tup) is True:
                return True

        # case where this box is smaller and partially or wholly
        # within larger box
        c_list = self.get_list_of_tuple_of_corner_coordinates()
        for c_tup in c_list:
            if box.is_coordinate_in_box(c_tup) is True:
                return True

        return False
