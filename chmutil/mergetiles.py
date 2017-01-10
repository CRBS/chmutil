#! /usr/bin/env python

import sys
import os
import argparse
import logging
import chmutil
from PIL import Image
from chmutil.core import Parameters
from chmutil import core
from chmutil.image import SimpleImageMerger

LOG_FORMAT = "%(asctime)-15s %(levelname)s (%(process)d) %(name)s %(message)s"

# create logger
logger = logging.getLogger('chmutil.mergetiles')


def _parse_arguments(desc, args):
    """Parses command line arguments using argparse.
    """
    parsed_arguments = Parameters()

    help_formatter = argparse.RawDescriptionHelpFormatter
    parser = argparse.ArgumentParser(description=desc,
                                     formatter_class=help_formatter)
    parser.add_argument("imagedir", help='Directory containing image tiles'
                                         'from CHM')
    parser.add_argument("output", help='Output image path, should have '
                                       'same extension as input')
    parser.add_argument("--maxpixels", type=int, default=768000000,
                        help='Sets maximum number of pixels in Image library'
                             'MAX_IMAGE_PIXELS default(768000000)')
    parser.add_argument("--suffix", default='png',
                        help='Only attempt to merge image files with'
                             'this suffix. (Default png)')
    parser.add_argument("--log", dest="loglevel", choices=['DEBUG',
                        'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help="Set the logging level (default WARNING)",
                        default='WARNING')
    parser.add_argument('--version', action='version',
                        version=('%(prog)s ' + chmutil.__version__))

    return parser.parse_args(args, namespace=parsed_arguments)


def _merge_image_tiles(img_dir, dest_file, suffix):
    """Merges image tiles
    """
    logger.info('Merging images in ' + img_dir)
    sim = SimpleImageMerger()
    im_list = core.get_image_path_list(img_dir, suffix)
    merged = sim.merge_images(im_list)

    if merged is None:
        logger.error('No images were merged')
        return 1

    logger.info('Writing results to ' + dest_file)
    merged.save(dest_file)
    return 0


def main(arglist):
    """Main function
    :param arglist: Should be set to sys.argv which is list of arguments
                    passed on commandline including script being run as arg 0
    :returns: exit code. 0 is success otherwise failure
    """
    desc = """
              Version {version}

              Merges set of image tiles in <imagedir> directory
              writing out a single merged image to <output> file


              Example Usage:

              mergetiles.py ./histeqimages merged_img.png

              """.format(version=chmutil.__version__)

    theargs = _parse_arguments(desc, arglist[1:])
    theargs.program = arglist[0]
    theargs.version = chmutil.__version__
    core.setup_logging(logger, log_format=LOG_FORMAT,
                       loglevel=theargs.loglevel)
    try:
        logger.debug('Setting Image.MAX_IMAGE_PIXELS to ' +
                     str(theargs.maxpixels))
        Image.MAX_IMAGE_PIXELS = theargs.maxpixels

        return _merge_image_tiles(os.path.abspath(theargs.imagedir),
                                  os.path.abspath(theargs.output),
                                  theargs.suffix)
    except Exception:
        logger.exception('Caught exception')
        return 2
    finally:
        logging.shutdown()


if __name__ == '__main__':  # pragma: no cover
    sys.exit(main(sys.argv))
