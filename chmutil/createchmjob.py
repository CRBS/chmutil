#! /usr/bin/env python

import sys
import argparse
import logging
import os
import chmutil

from chmutil.core import CHMJobCreator
from chmutil.core import CHMConfig
from chmutil.cluster import RocceSubmitScriptGenerator
from chmutil.cluster import BatchedJobsListGenerator


LOG_FORMAT = "%(asctime)-15s %(levelname)s %(name)s %(message)s"

# create logger
logger = logging.getLogger('chmutil.createchmjob')



class Parameters(object):
    """Placeholder class for parameters
    """
    pass


def _setup_logging(theargs):
    """hi
    """
    theargs.logformat = LOG_FORMAT
    theargs.numericloglevel = logging.NOTSET
    if theargs.loglevel == 'DEBUG':
        theargs.numericloglevel = logging.DEBUG
    if theargs.loglevel == 'INFO':
        theargs.numericloglevel = logging.INFO
    if theargs.loglevel == 'WARNING':
        theargs.numericloglevel = logging.WARNING
    if theargs.loglevel == 'ERROR':
        theargs.numericloglevel = logging.ERROR
    if theargs.loglevel == 'CRITICAL':
        theargs.numericloglevel = logging.CRITICAL

    logger.setLevel(theargs.numericloglevel)
    logging.basicConfig(format=theargs.logformat)

    logging.getLogger('chmutil.createchmjob').setLevel(theargs.numericloglevel)
    logging.getLogger('chmutil.core').setLevel(theargs.numericloglevel)


def _parse_arguments(desc, args):
    """Parses command line arguments using argparse.
    """
    parsed_arguments = Parameters()

    help_formatter = argparse.RawDescriptionHelpFormatter
    parser = argparse.ArgumentParser(description=desc,
                                     formatter_class=help_formatter)
    parser.add_argument("images", help='Directory of images')
    parser.add_argument("model", help='Directory containing trained model')
    parser.add_argument("outdir", help='Output directory')
    parser.add_argument("--log", dest="loglevel", choices=['DEBUG',
                        'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help="Set the logging level (default WARNING)",
                        default='WARNING')
    parser.add_argument("--chmbin", help='Full path to chm binary',
                        default='./chm-0.1.0.img')
    parser.add_argument('--tilesize',
                        default='',
                        help='Sets size of tiles to use when running chm in '
                             'tile mode.  If set value should be WxH format '
                             'or widthXheight aka 200x100 would mean each tile'
                             ' is 200 pixels wide and 100 pixels wide. '
                             '(default empty string meaning no tiling)')
    parser.add_argument('--overlapsize',
                        default='0x0',
                        help='Sets overlap of tiles to use when running chm in '
                             'tile mode. If set value should be in WxH format'
                             ' aka 200x100 would mean overlap 200 pixels in '
                             ' X or width direction and 100 pixels in Y or'
                             ' height direction (default 0x0)')

    parser.add_argument('--disablechmhisteq', action='store_true',
                        help='If set tells CHM NOT to do internal histogram '
                             'equalization')

    parser.add_argument('--tilesperjob', default='50',
                        help='Number of tiles to run per job. Lower numbers '
                             'mean each job runs faster, but results in '
                             'more jobs. (default 50)')

    parser.add_argument('--jobspernode', default='11',
                        help='Number of jobs to run concurrently on a single'
                             'compute node. For 500x500 tiles, Gordon '
                             'should be set to 11, '
                             'Comet should be set to 21. (default 11)')
    parser.add_argument('--cluster', default='rocce',
                        help='Sets which cluster to generate job script for'
                             ' (default rocce)')
    parser.add_argument('--version', action='version',
                        version=('%(prog)s ' + chmutil.__version__))

    return parser.parse_args(args, namespace=parsed_arguments)

def _create_chm_job(theargs):
    """Creates CHM Job
    :param theargs: list of arguments obtained from _parse_arguments()
    :returns: exit code for program. 0 success otherwise failure
    """
    try:
        opts = CHMConfig(os.path.abspath(theargs.images),
                         os.path.abspath(theargs.model),
                         os.path.abspath(theargs.outdir),
                         theargs.tilesize,
                         theargs.overlapsize,
                         disablehisteq=theargs.disablechmhisteq,
                         number_tiles_per_job=int(theargs.tilesperjob),
                         jobs_per_node=int(theargs.jobspernode),
                         chmbin=os.path.abspath(theargs.chmbin))

        creator = CHMJobCreator(opts)
        opts = creator.create_job()

        # TODO create separate classes to generate submit script
        bgen = BatchedJobsListGenerator(opts)
        bgen.generate_batched_jobs_list()

        gen = None
        if theargs.cluster == 'rocce':
            gen = RocceSubmitScriptGenerator(opts)

        if gen is not None:
            script, info = gen.generate_submit_script()
            sys.stdout.write('\n' + info + '\n')


        return 0
    except Exception:
        logger.exception("Error caught exception")
        return 2


def main(arglist):
    """Main function
    :param arglist: Should be set to sys.argv which is list of arguments
                    passed on commandline including script being run as arg 0
    :returns: exit code. 0 is success otherwise failure
    """
    desc = """
              Version {version}

              Creates job scripts to run CHM on images in <images> directory
              using model specified in <model> directory. The generated scripts
              are put in <outdir>


              Example Usage:

              createchmjob.py ./images ./model ./mychmjob

              """.format(version=chmutil.__version__)

    theargs = _parse_arguments(desc, arglist[1:])
    theargs.program = arglist[0]
    theargs.version = chmutil.__version__
    _setup_logging(theargs)
    try:
        return _create_chm_job(theargs)
    finally:
        logging.shutdown()


if __name__ == '__main__':
    sys.exit(main(sys.argv))
