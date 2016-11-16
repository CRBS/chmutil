#! /usr/bin/env python

import sys
import os
import argparse
import logging
import chmutil
from PIL import Image
from PIL import ImageOps
from PIL import ImageFilter

from chmutil.core import Parameters
from chmutil import core

LOG_FORMAT = "%(asctime)-15s %(levelname)s (%(process)d) %(name)s %(message)s"

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
    parser.add_argument("image", help='Base image')
    parser.add_argument("probmap", help='Probability map')

    parser.add_argument("output", help='Output image path, should have .png'
                                       'extension, if not .png will be '
                                       'appended')
    parser.add_argument("--log", dest="loglevel", choices=['DEBUG',
                        'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help="Set the logging level (default WARNING)",
                        default='WARNING')
    parser.add_argument('--version', action='version',
                        version=('%(prog)s ' + chmutil.__version__))

    return parser.parse_args(args, namespace=parsed_arguments)


def _convert_image(image_file, probmap_file, dest_file, theargs):
    """Convert image
    """
    if not os.path.isfile(image_file):
        raise Exception('Image ' + image_file + ' not found')

    if not os.path.isfile(probmap_file):
        raise Exception('Image ' + probmap_file + ' not found')

    logger.debug('Opening file ' + image_file)

    probimg = Image.open(probmap_file)

    # threshold image anything below %30 set to zero, rest to 255
    # also means value of 77 pixel
    probimg = Image.eval(probimg, lambda px: 0 if px < 77 else 255)
    probimg = probimg.convert('RGB')

    pixels = list(probimg.getdata())
    blue_pixels = []
    for r, g, b in pixels:
        blue_pixels.append((0, 0, b))

    blueprobmap = Image.new('RGBA', probimg.size)
    blueprobmap.putdata(blue_pixels)

    img = Image.open(image_file).convert(mode='RGBA')

    res = Image.blend(img, blueprobmap, 0.3)

    if not dest_file.endswith('.png'):
        dest_file += '.png'
    res.save(dest_file, "PNG")


def main(arglist):
    """Main function
    :param arglist: Should be set to sys.argv which is list of arguments
                    passed on commandline including script being run as arg 0
    :returns: exit code. 0 is success otherwise failure
    """
    desc = """
              Version {version}

              Creates new image (output)
              where probability map (probmap) is semi-transparently
              overlayed in blue on top of base image (image)

              Example Usage:

              createprobmapoverlay.py baseimage.png probmap.png overlay.png

              """.format(version=chmutil.__version__)

    theargs = _parse_arguments(desc, arglist[1:])
    theargs.program = arglist[0]
    theargs.version = chmutil.__version__
    core.setup_logging(logger, log_format=LOG_FORMAT,
                       loglevel=theargs.loglevel)
    try:
        return _convert_image(os.path.abspath(theargs.image),
                              os.path.abspath(theargs.probmap),
                                  os.path.abspath(theargs.output),
                                  theargs)
    finally:
        logging.shutdown()


if __name__ == '__main__':
    sys.exit(main(sys.argv))
