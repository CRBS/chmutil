#! /usr/bin/env python

import sys
import os
import argparse
import logging
import chmutil
from PIL import Image

from chmutil.core import Parameters
from chmutil import core
from chmutil.image import ImageThresholder
from chmutil.image import ColorizeGrayscaleImage

LOG_FORMAT = "%(asctime)-15s %(levelname)s (%(process)d) %(name)s %(message)s"

# create logger
logger = logging.getLogger('chmutil.createprobmapoverlay')


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
                             'less then 30%% of 255 to 0 and the rest to 255',
                        default=30)
    parser.add_argument("--opacity", type=int, default=70,
                        help='Sets level of opacity of overlay. 0 is '
                             'transparent and 255 is opaque. (default 70)')
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


def _get_thresholded_probmap(probmap_file, threshpc):
    """Reads probability map and thresholds it according to
    value of `threshpc`
    :param probmap_file:
    :param threshpc: threshold percent ie 30 means to threshold
           all pixels below 30% max intensity to 0 and the rest
           to 255
    :return: Pillow Image that is grayscale thresholded to be
             values of 0 or 255
    """
    probimg = None
    try:
        probimg = Image.open(probmap_file).convert(mode='L')

        # threshold image anything belowtheargs.threshpc percentage
        #  set to zero, rest to 255
        logger.info('Thresholding probability map')
        thresh = ImageThresholder(threshold_percent=int(threshpc))
        return thresh.threshold_image(probimg)
    finally:
        if probimg is not None:
            probimg.close()


def _convert_image(image_file, probmap_file, dest_file, theargs):
    """Convert image
    """
    if not os.path.isfile(image_file):
        raise NoInputImageFoundError('Image ' + image_file + ' not found')

    if not os.path.isfile(probmap_file):
        raise NoInputImageFoundError('Image ' + probmap_file + ' not found')

    thresh_image = _get_thresholded_probmap(probmap_file,
                                            theargs.threshpc)

    logger.info('Colorizing probability map')
    colortuple = _get_pixel_coloring_tuple(theargs.overlaycolor)
    colorizer = ColorizeGrayscaleImage(color=colortuple,
                                       opacity=theargs.opacity)

    col_img = colorizer.colorize_image(thresh_image)
    thresh_image.close()

    logger.info('Loading base image')
    img = Image.open(image_file).convert(mode='RGBA')

    logger.info('Combining base image with probability map')
    res = Image.alpha_composite(img, col_img)

    if not dest_file.endswith('.png'):
        dest_file += '.png'
    res.save(dest_file, "PNG")
    return 0


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
