#! /usr/bin/env python

import sys
import os
import argparse
import logging
import chmutil
import random
import tempfile
import shutil
import configparser
from PIL import Image


from chmutil.core import Parameters
from chmutil import core
from chmutil.core import Box

LOG_FORMAT = "%(asctime)-15s %(levelname)s (%(process)d) %(name)s %(message)s"

IMAGEDIR_KEY = 'imagedir'
# create logger
logger = logging.getLogger('chmutil.createchmimage')


class NoInputImageFoundError(Exception):
    """Raised if input image does not exist
    """
    pass


def _parse_arguments(desc, args):
    """Parses command line arguments using argparse.
    """
    parsed_arguments = Parameters()

    help_formatter = argparse.RawDescriptionHelpFormatter
    parser = argparse.ArgumentParser(description=desc,
                                     formatter_class=help_formatter)
    parser.add_argument("imagedir", help='Base image dir')
    parser.add_argument("numtiles", type=int,
                        help='# of tiles to extract')

    parser.add_argument("output", help='Full path to output mrc file')
    parser.add_argument("--suffix", default='.png',
                        help='Image suffix (default .png)')
    parser.add_argument("--useconfig", help='Instead of generating random'
                                            'tiles use config passed in')
    parser.add_argument("--tilesize", default='512x512',
                        help='NOT IMPLEMENTED Size of tiles in WxH format '
                             '(default 512x512)')
    parser.add_argument("--scratchdir", default='/tmp')
    parser.add_argument("--log", dest="loglevel", choices=['DEBUG',
                        'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help="Set the logging level (default WARNING)",
                        default='WARNING')
    parser.add_argument("--seed", default=None,
                        help='Seed to use for random number generator')
    parser.add_argument('--version', action='version',
                        version=('%(prog)s ' + chmutil.__version__))

    return parser.parse_args(args, namespace=parsed_arguments)


def _pick_tile(img_path, tile_width, tile_height):
    """picks a tile
    """
    img = None
    try:
        img = Image.open(img_path)
        xmax = img.size[0]-tile_width
        ymax = img.size[1]-tile_height

        xpos = random.randint(0, xmax)
        ypos = random.randint(0, ymax)
        logger.info('Rando tile: ' + img_path + ' x=' + str(xpos) +
                    ' y='+str(ypos))
        return Box(left=xpos, upper=ypos, right=xpos + tile_width,
                   lower=ypos + tile_height)
    finally:
        if img is not None:
            img.close()


def _does_tile_intersect_any_other_tiles(tile_tuple_list, new_tile_tuple):
    """check for intersection
    """
    for entry in tile_tuple_list:
        if entry[0] == new_tile_tuple[0]:
            if entry[1].does_box_intersect(new_tile_tuple[1]) is True:
                logger.info('Found intersecting tile')
                return True
    return False


def _pick_random_tiles(img_list, num_tiles, tile_width=512,
                       tile_height=512):
    """Using random generates a list of tuples with image path and tile
    location
    :returns: nested tuple (image path, (left, upper, right, and lower))
    """
    num_images = len(img_list)
    tile_tuple_list = []
    while len(tile_tuple_list) < num_tiles:
        the_img = img_list[random.randint(0, num_images-1)]
        tile_box = _pick_tile(the_img, tile_width, tile_height)
        new_tile_tuple = the_img, tile_box
        if _does_tile_intersect_any_other_tiles(tile_tuple_list,
                                                new_tile_tuple) is False:
            tile_tuple_list.append(new_tile_tuple)

    return tile_tuple_list


def _get_tiles_from_tuple_list(img_list, config_file):
    """Loads list of tuples with image path and tile location
    :returns: nested tuple (image path, (left, upper, right, and lower))
    """
    config = configparser.ConfigParser()
    config.read(config_file)
    tile_tuple_list = []

    tile_dict = {}
    for entry in img_list:
        img_basename = os.path.basename(entry)
        logger.debug('Looking for entries with section name: ' + img_basename)
        if not config.has_section(img_basename):
            continue
        for subentry in config.items(img_basename):
            logger.debug('tile: ' + subentry[0] + ' => ' + subentry[1])
            if subentry[0] == IMAGEDIR_KEY:
                logger.debug('skipping ' + IMAGEDIR_KEY)
                continue
            tile_dict[int(subentry[0])] = (entry, subentry[1])

    sorted_keys = tile_dict.keys()
    sorted_keys.sort()
    for x in sorted_keys:
        tbox = Box()
        tbox.load_from_comma_delimited_string(tile_dict[x][1])
        tile_tuple_list.append((tile_dict[x][0], tbox))

    return tile_tuple_list


def _extract_and_save_tile(temp_dir, entry, counter):
    """extract and save tile
    """
    img = None
    tile = None
    try:
        logger.info('Creating tile from ' + entry[0] +
                    ' with these coords ' +
                    entry[1].get_box_as_comma_delimited_string())
        img = Image.open(entry[0])
        tile = img.crop(entry[1].get_box_as_tuple())
        tif_file = os.path.join(temp_dir, str(counter).zfill(4) + '.tif')
        logger.info('Saving file ' + tif_file)
        tile.save(tif_file, format='TIFF')
    finally:
        if img is not None:
            img.close()
        if tile is not None:
            tile.close()


def _save_tile_tuple_list_as_config_file(tile_tuple_list, config_file):
    """saves tile tuple list as config file
    """
    config = configparser.ConfigParser()
    counter = 1
    for entry in tile_tuple_list:
        counter_as_str = str(counter)
        if counter is 1:
            config.set('', IMAGEDIR_KEY, os.path.dirname(entry[0]))

        img_basename = os.path.basename(entry[0])
        if not config.has_section(img_basename):
            config.add_section(img_basename)
        config.set(img_basename, counter_as_str,
                   entry[1].get_box_as_comma_delimited_string())
        counter += 1

    f = open(config_file, 'w')
    config.write(f)
    f.flush()
    f.close()


def _create_mrc_stack(image_dir, num_tiles, dest_file, theargs):
    """Convert image
    """
    img_list = core.get_image_path_list(image_dir, theargs.suffix)
    if len(img_list) is 0:
        logger.error('No images found in ' + image_dir)
        return 1

    try:
        logger.debug('--useconfig set to ' + str(theargs.useconfig))
        if theargs.useconfig is None:
            parse_config = False
        else:
            parse_config = True
    except AttributeError:
        parse_config = False

    if parse_config is True:
        logger.debug('Using config for tiles')
        tile_tuple_list = _get_tiles_from_tuple_list(img_list,
                                                     theargs.useconfig)
    else:
        logger.debug('Generating random tiles')
        random.seed(theargs.seed)
        tile_tuple_list = _pick_random_tiles(img_list, num_tiles)

    temp_dir = tempfile.mkdtemp(dir=theargs.scratchdir)
    curdir = os.getcwd()

    try:
        counter = 0
        for entry in tile_tuple_list:
            _extract_and_save_tile(temp_dir, entry, counter)
            counter += 1
        logger.info('Changing to ' + temp_dir + 'directory to run newstack')
        os.chdir(temp_dir)

        tif_list = []
        for entry in os.listdir(temp_dir):
            if entry.endswith('.tif'):
                tif_list.append(entry)
        cmd = 'newstack ' + ' '.join(tif_list) + ' "' + dest_file + '"'

        exit, out, err = core.run_external_command(cmd, temp_dir)

        sys.stdout.write(out)
        sys.stderr.write(err)

        _save_tile_tuple_list_as_config_file(tile_tuple_list,
                                             dest_file + '.tile.list.config')
        return exit
    finally:
        os.chdir(curdir)
        shutil.rmtree(temp_dir)


def main(arglist):
    """Main function
    :param arglist: Should be set to sys.argv which is list of arguments
                    passed on commandline including script being run as arg 0
    :returns: exit code. 0 is success otherwise failure
    """
    desc = """
              Version {version}

              WARNING: THIS SCRIPT HAS NOT BEEN TESTED AND MAY NOT WORK

              Creates mrc stack (output) by extracting random
              tiles from images in (imagedir)
              Example Usage:

              createtrainingmrcstack.py ./myimages 5 ./result.foo

              """.format(version=chmutil.__version__)

    sys.stderr.write('\nTHIS PROGRAM IS AN ALPHA IMPLEMENTATION '
                     'AND MAY NOT WORK\n\n')
    theargs = _parse_arguments(desc, arglist[1:])
    theargs.program = arglist[0]
    theargs.version = chmutil.__version__
    core.setup_logging(logger, log_format=LOG_FORMAT,
                       loglevel=theargs.loglevel)
    try:
        sys.stderr.write('\nTHIS PROGRAM IS AN ALPHA IMPLEMENTATION '
                         'AND MAY NOT WORK\n\n')
        return _create_mrc_stack(os.path.abspath(theargs.imagedir),
                                 theargs.numtiles,
                                 os.path.abspath(theargs.output),
                                 theargs)
    finally:
        logging.shutdown()


if __name__ == '__main__':  # pragma: no cover
    sys.exit(main(sys.argv))
