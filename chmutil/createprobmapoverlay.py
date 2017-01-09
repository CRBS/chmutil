#! /usr/bin/env python

import sys
import os
import argparse
import logging
import chmutil
from PIL import Image

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
    parser.add_argument("--overlaycolor", type=str,
                        help="Color to use for overlay"
                             "(default blue)",
                        choices=['red', 'blue', 'green', 'yellow',
                                 'cyan', 'magenta', 'purple'],
                        default='blue')
    parser.add_argument("--threshpc", type=int,
                        help='Percent cut off for thresholding with '
                             'valid range 0-100.'
                             ' For example,'
                             'a value of 30 means to set all pixels with '
                             'intensity'
                             'less then 30% of 255 to 0 and the rest to 255',
                        default=30)
    parser.add_argument("--blend", type=float, default=0.3,
                        help='Blend value used to blend probability'
                             'map with base image. (default 0.3')
    parser.add_argument("--log", dest="loglevel", choices=['DEBUG',
                        'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help="Set the logging level (default WARNING)",
                        default='WARNING')
    parser.add_argument('--version', action='version',
                        version=('%(prog)s ' + chmutil.__version__))

    return parser.parse_args(args, namespace=parsed_arguments)


def _get_pixel_coloring_tuple(thecolor):

    logger.debug('Overlay color set to: ' + thecolor)

    if thecolor == 'red':
        return 1, 0, 0

    if thecolor == 'green':
        return 0, 1, 0

    if thecolor == 'yellow':
        return 1, 1, 0

    if thecolor == 'cyan':
        return 0, 1, 1

    if thecolor == 'magenta':
        return 1, 0, 1

    if thecolor == 'purple':
        return 0.5, 0, 0.5

    return 0, 0, 1


def _convert_image(image_file, probmap_file, dest_file, theargs):
    """Convert image
    """
    if not os.path.isfile(image_file):
        raise Exception('Image ' + image_file + ' not found')

    if not os.path.isfile(probmap_file):
        raise Exception('Image ' + probmap_file + ' not found')

    logger.debug('Opening file ' + image_file)

    probimg = Image.open(probmap_file)

    # threshold image anything belowtheargs.threshpc percentage
    #  set to zero, rest to 255
    rawpx = int((float(theargs.threshpc)*0.01)*255)
    probimg = Image.eval(probimg, lambda px: 0 if px < rawpx else 255)
    rgbimg = probimg.convert('RGB')
    probimg.close()


    pixels = list(rgbimg.getdata())
    blue_pixels = []
    ctuple = _get_pixel_coloring_tuple(theargs.overlaycolor)
    for r, g, b in pixels:
        blue_pixels.append((int(r*ctuple[0]), int(g*ctuple[1]),
                            int(b*ctuple[2])))

    blueprobmap = Image.new('RGBA', rgbimg.size)
    blueprobmap.putdata(blue_pixels)

    img = Image.open(image_file).convert(mode='RGBA')

    res = Image.blend(img, blueprobmap, theargs.blend)

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


if __name__ == '__main__':  # pragma: no cover
    sys.exit(main(sys.argv))
