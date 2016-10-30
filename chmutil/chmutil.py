# -*- coding: utf-8 -*-


import os
import logging
import configparser
from PIL import Image

logger = logging.getLogger(__name__)


class CHMJobCreator(object):
    """Creates CHM Job to run on cluster
    """

    CONFIG_FILE_NAME = 'chm.jobs.list'

    def __init__(self, images, model, outdir, tilesize=None,
                 overlapsize=None, disablehisteq=False,
                 tilesperjob=1,
                 jobspernode=11):
        """Constructor
        """
        self._images = images
        self._model = model
        self._outdir = outdir
        self._tilesize = tilesize
        self._overlapsize = overlapsize
        self._disablehisteq = disablehisteq
        self._tilesperjob = tilesperjob
        self._jobspernode = jobspernode

    def _create_config(self):
        config = configparser.ConfigParser()
        config.set('', 'model', self._model)
        config.set('', 'tilesperjob', self._tilesperjob)
        return config

    def _write_config(self, config):
        cfile = os.path.join(self._outdir, CHMJobCreator.CONFIG_FILE_NAME)
        f = open(cfile,
                 'w')
        config.write(f)
        f.flush()
        f.close()
        return cfile

    def create_job(self):
        """Creates jobs
        """
        statsfac = InputImageStatsFromDirectoryFactory(self._images)
        imagestats = statsfac.get_input_image_stats()
        config = self._create_config()
        counter = 1
        for iis in imagestats:
            counter_as_str = str(counter)
            config.add_section(counter_as_str)
            config.set(counter_as_str, 'inputimage', iis.get_file_path())
            config.set(counter_as_str, 'args', 'args go here')
            config.set(counter_as_str, 'outputimage',' some output dir')

        cfile = self._write_config(config)

        job = CHMJob(self._outdir, cfile)
        return job


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


class InputImageStats(object):
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

    def get_tile_args(self):
        """
        hi
        :return:
        """
        return "hi"

class InputImageStatsFromDirectoryFactory(object):
    """Creates InputImageStats objects from directory of images
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
                    iis = InputImageStats(im, fp, im.size[0],
                                          im.size[1], im.format)
                    image_stats_list.append(iis)
                except:
                    pass

        return image_stats_list

class TileArgGenerator(object):
    """Generates tile args consumable by CHM 2.1.367
    """
    def __init__(self):
        """Constructor
        """
        pass

    def get_tile_arg(self, input_image_stats,
                     tile_width, tile_height, overlap_width,
                     overlap_height, number_tiles_per_job):
        """Creates a list of tile args
        """
        return ['1,1 1,2 1,3', '3,4 4,5 5,6']




