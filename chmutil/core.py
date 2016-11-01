# -*- coding: utf-8 -*-


import os
import logging
import configparser
from PIL import Image

logger = logging.getLogger(__name__)


class OverlapTooLargeForTileSizeError(Exception):
    """Raised when overlap used is to large for overlap
    """
    pass

class CHMJobCreator(object):
    """Creates CHM Job to run on cluster
    """

    CONFIG_FILE_NAME = 'chm.jobs.list'
    RUN_DIR = 'chmrun'
    CONFIG_INPUT_IMAGE = 'inputimage'
    CONFIG_ARGS = 'args'
    CONFIG_OUTPUT_IMAGE = 'outputimage'
    CONFIG_MODEL = 'model'
    CONFIG_TILES_PER_JOB = 'tilesperjob'
    CONFIG_TILE_SIZE = 'tilesize'
    CONFIG_OVERLAP_SIZE = 'overlapsize'
    CONFIG_DISABLE_HISTEQ_IMAGES = 'disablehisteqimages'
    CONFIG_JOBS_PER_NODE = 'jobspernode'

    def __init__(self, chmopts):
        """Constructor
        """
        self._chmopts = chmopts

    def _create_config(self):
        """Creates configparser object and populates it with CHMOpts data
        :returns: configparser config object filled with CHMOpts data
        """
        config = configparser.ConfigParser()
        config.set('', CHMJobCreator.CONFIG_MODEL, self._chmopts.get_model())
        config.set('', CHMJobCreator.CONFIG_TILES_PER_JOB,
                   str(self._chmopts.get_number_tiles_per_job()))
        config.set('', CHMJobCreator.CONFIG_TILE_SIZE,
                   str(self._chmopts.get_tile_size()))
        config.set('', CHMJobCreator.CONFIG_OVERLAP_SIZE,
                   str(self._chmopts.get_overlap_size()))
        config.set('', CHMJobCreator.CONFIG_DISABLE_HISTEQ_IMAGES,
                   str(self._chmopts.get_disable_histogram_eq_val()))
        config.set('', CHMJobCreator.CONFIG_JOBS_PER_NODE,
                   str(self._chmopts.get_number_jobs_per_node()))
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

    def _create_output_image_dir(self, imagestats, run_dir):
        """Creates directory where CHM output images will be written
        :param imagestats: ImageStats for image to create directory for
        :param rundir: Base run directory for CHM job
        :returns: tuple (output image directory, filename of image form `imagestats`)
        """
        i_name = os.path.basename(imagestats.get_file_path())
        i_dir = os.path.join(run_dir, i_name)
        if os.path.isdir(i_dir) is False:
            logger.debug('Creating image dir ' + i_dir)
            os.makedirs(i_dir, mode=0775)
        return i_dir, i_name

    def _create_run_dir(self):
        """Creates CHM job run directory
        :returns: Path to run directory
        """
        run_dir = os.path.join(self._chmopts.get_out_dir(), CHMJobCreator.RUN_DIR)
        if os.path.isdir(run_dir) == False:
            logger.debug('Creating run dir ' + run_dir)
            os.makedirs(run_dir, mode=0775)

        return run_dir

    def _add_job_for_image_to_config(self, config, counter_as_str,
                                     imagestats, i_dir, i_name,
                                     img_cntr, theargs):
        """Adds job to config object
        :param config: configparser config object to add job to
        :param counter_as_str: Counter used in string form
        :param imagestats: `ImageStats` for job
        :param i_dir: Output image directory
        :param i_name: Name of image
        :param img_cntr: Image counter
        :param theargs: args for CHM job
        """
        config.add_section(counter_as_str)
        config.set(counter_as_str, CHMJobCreator.CONFIG_INPUT_IMAGE,
                   imagestats.get_file_path())
        config.set(counter_as_str, CHMJobCreator.CONFIG_ARGS, ' '.join(theargs))
        config.set(counter_as_str, CHMJobCreator.CONFIG_OUTPUT_IMAGE,
                   os.path.join(i_dir, str(img_cntr).zfill(3) + '.' + i_name))

    def create_job(self):
        """Creates jobs
        """
        arg_gen = CHMArgGenerator(self._chmopts)
        statsfac = ImageStatsFromDirectoryFactory(self._chmopts.get_images())
        imagestats = statsfac.get_input_image_stats()
        config = self._create_config()
        counter = 1
        run_dir = self._create_run_dir()

        for iis in imagestats:
            i_dir, i_name = self._create_output_image_dir(iis, run_dir)
            img_cntr = 1
            for a in arg_gen.get_args(iis):
                counter_as_str = str(counter)
                self._add_job_for_image_to_config(config, counter_as_str,
                                                  iis, i_dir, i_name, img_cntr,
                                                  a)
                counter += 1
                img_cntr += 1

        cfile = self._write_config(config)

        job = CHMJob(self._chmopts.get_out_dir(), cfile)
        return job


class CHMOpts(object):
    """Contains options for CHM parameters
    """
    def __init__(self, images, model, outdir,
                 tile_size,
                 overlap_size,
                 number_tiles_per_job=1,
                 jobs_per_node=1,
                 disablehisteq=True):
        """Constructor
        """
        self._images = images
        self._model = model
        self._outdir = outdir
        self._tile_size = tile_size
        self._overlap_size = overlap_size
        self._parse_and_set_tile_width_height(tile_size)
        self._parse_and_set_overlap_width_height(overlap_size)
        self._number_tiles_per_job = number_tiles_per_job
        self._jobs_per_node = jobs_per_node
        self._disablehisteq = disablehisteq

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

    def get_number_tiles_per_job(self):
        """returns number of tiles per job
        """
        return self._number_tiles_per_job

    def get_number_jobs_per_node(self):
        """gets desired number jobs per node
        """
        return self._jobs_per_node


class CHMJob(object):
    """Represents a CHM Cluster Job
    """

    def __init__(self, path, configfile):
        """Constructor
        """
        self._path = path
        self._config = configfile

    def get_dir(self):
        """Gets job directory
        """
        return self._path

    def get_job_config(self):
        """Gets job configuration file
        """
        return self._config


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

    def __init__(self, directory):
        """Constructor
        """
        self._directory = directory

    def get_input_image_stats(self):
        """Gets InputImageStats objects as list
        """
        image_stats_list = []
        if os.path.isfile(self._directory):
            return []

        for entry in os.listdir(self._directory):
            fp = os.path.join(self._directory, entry)
            if os.path.isfile(fp):
                try:
                    im = Image.open(fp)
                    iis = ImageStats(fp, im.size[0],
                                          im.size[1], im.format)
                    image_stats_list.append(iis)
                except Exception as e:
                    logger.exception('Skipping file unable to open ' + fp)

        return image_stats_list


class CHMArgGenerator(object):
    """Generates tile args consumable by CHM 2.1.367
    """
    def __init__(self, chmopts):
        """Constructor
        """
        self._chmopts = chmopts
        self._t_width_w_over = self._chmopts.get_tile_width() -\
                         (2 * self._chmopts.get_overlap_width())

        if self._t_width_w_over <= 0:
            raise OverlapTooLargeForTileSizeError('Overlap width too large '
                                                  'for tile')

        self._t_height_w_over = self._chmopts.get_tile_height() -\
                          (2 * self._chmopts.get_overlap_height())

        if self._t_height_w_over <= 0:
            raise OverlapTooLargeForTileSizeError('Overlap height too large '
                                                  'for tile')

    def get_args(self, image_stats):
        """Creates a list of tile args
        """
        (tiles_w, tiles_h) = self._get_number_of_tiles_tuple(image_stats)
        tile_list = []
        for c in range(1, tiles_w + 1):
            for r in range(1, tiles_h + 1):
                tile_list.append('-t ' + str(c) + ',' + str(r))
        total = tiles_w * tiles_h
        split_list = []
        t_per_job = self._chmopts.get_number_tiles_per_job()
        for ts in range(0, total, t_per_job):
            split_list.append(tile_list[ts:ts+t_per_job])
        return split_list

    def _get_number_of_tiles_tuple(self, image_stats):
        """Gets number of tiles needed in horizontal and vertical
           directions to analyze an image
        """


        tiles_width = (image_stats.get_width() +
                       self._t_width_w_over -1) / self._t_width_w_over

        tiles_height = (image_stats.get_height() +
                        self._t_height_w_over -1) / self._t_height_w_over

        return tiles_width, tiles_height
