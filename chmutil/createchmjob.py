#! /usr/bin/env python

import sys
import argparse
import logging
import os
import chmutil

from chmutil.core import CHMJobCreator
from chmutil.core import CHMConfig
from chmutil.core import Parameters
from chmutil.cluster import ClusterFactory
from chmutil import core

# create logger
logger = logging.getLogger('chmutil.createchmjob')


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
                        default='512x512',
                        help='Sets size of tiles to use when running chm in '
                             'tile mode.  If set value should be WxH format '
                             'or widthXheight aka 200x100 would mean each tile'
                             ' is 200 pixels wide and 100 pixels wide. '
                             '(default 512x512)')
    parser.add_argument('--overlapsize',
                        default='0x0',
                        help='Sets overlap of tiles to use when running chm '
                             'in tile mode. If set value should be in WxH '
                             'format aka 200x100 would mean overlap 200 '
                             'pixels in  X or width direction and 100 '
                             'pixels in Y or height direction (default 0x0)')

    parser.add_argument('--disablechmhisteq', action='store_true',
                        help='If set tells CHM NOT to do internal histogram '
                             'equalization')

    parser.add_argument('--tilespertask', '--jobspertask', dest='tilespertask',
                        default='50', type=int,
                        help='Number of tiles to run per task. Lower numbers '
                             'mean each task runs faster, but results in '
                             'more task. (default 50)')
    parser.add_argument('--taskspernode', '--jobspernode', default=0, type=int,
                        help='Overrides number of CHM tasks to run '
                             'concurrently '
                             'on a single compute node. (default is 0 '
                             'which tells script to set this value to a '
                             'number appropriate for cluster set '
                             'in --cluster option)')
    parser.add_argument('--mergetaskspernode', default=0, type=int,
                        help='Overrides number of merge tasks to run '
                             'concurrently on a single compute node. '
                             '(default 0 which tellls script to se this value '
                             'to a number appropriate for cluster set in '
                             '--cluster option)')
    parser.add_argument('--cluster', default='rocce',
                        choices=ClusterFactory.VALID_CLUSTERS,
                        help='Sets which cluster to generate job script for'
                             ' (default rocce)')
    parser.add_argument('--account', default='',
                        help='Sets account to charge processing to. Needed'
                             'for jobs run on Gordon & Comet (default \'\')')
    parser.add_argument('--jobname', default='chmjob',
                        help='Name for job given to scheduler, must not '
                             'contain any non alphanumeric characters and '
                             'must start with letter a-z')
    parser.add_argument('--walltime', default='12:00:00',
                        help='Sets walltime for job in HH:MM:SS format '
                             'default(12:00:00) ')
    parser.add_argument('--version', action='version',
                        version=('%(prog)s ' + chmutil.__version__))

    return parser.parse_args(args, namespace=parsed_arguments)


def _create_chm_job(theargs):
    """Creates CHM Job
    :param theargs: list of arguments obtained from _parse_arguments()
    :returns: exit code for program. 0 success otherwise failure
    """
    try:
        cluster_fac = ClusterFactory()
        cluster = cluster_fac.get_cluster_by_name(theargs.cluster)
        taskspernode = cluster.\
            get_suggested_tasks_per_node(theargs.taskspernode)
        mergetaskspernode = cluster.\
            get_suggested_merge_tasks_per_node(theargs.mergetaskspernode)
        con = CHMConfig(os.path.abspath(theargs.images),
                        os.path.abspath(theargs.model),
                        os.path.abspath(theargs.outdir),
                        theargs.tilesize,
                        theargs.overlapsize,
                        disablehisteq=theargs.disablechmhisteq,
                        number_tiles_per_task=theargs.tilespertask,
                        tasks_per_node=taskspernode,
                        chmbin=os.path.abspath(theargs.chmbin),
                        scriptbin=os.path.dirname(theargs.program),
                        walltime=theargs.walltime,
                        jobname=theargs.jobname,
                        account=theargs.account,
                        mergejobname='merge' + theargs.jobname,
                        merge_tasks_per_node=mergetaskspernode,
                        version=chmutil.__version__,
                        cluster=theargs.cluster,
                        rawargs=theargs.rawargs)

        creator = CHMJobCreator(con)
        creator.create_job()
        cluster.set_chmconfig(con)
        cluster.generate_submit_script()
        cluster.generate_merge_submit_script()
        sys.stdout.write('Run this to submit job\n' +
                         cluster.get_checkchmjob_command() + '\n')
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
              are put in <outdir>.

              CHM requires TWO phases of processing.

              In the FIRST phase CHM tasks are run which work on tiles of each
              image in the input. This is done to parallelize the processing
              as well as reduce the memory footprint of CHM which gets huge on
              tiles larger then 1000x1000. For example tiles of 500x500 easily
              use 4 to 6 gigabytes of ram. These tiles are stored on the
              filesystem under <outdir>/{rundir}/{tiles}/<image.png>
              directories described below.

              In the SECOND phase merge tasks are run which combine the tiles
              into what are known as probability maps. Probability maps are
              simply greyscale 8-bit images (values 0-255)
              of the same size as in the input images where the intensity of
              the pixel correlates to the probability that it belongs to the
              feature trained for in the trained model. The probability maps
              are stored in <outdir>/{rundir}/{probmaps} directory described
              below.

              Here is a breakdown of the following directories
              created under the <outdir>:

              {config}
                 -- Configuration containing CHM tasks.

                    At the top of this file are some are options common
                    to all CHM tasks denoted by the header [DEFAULT]

                    Following the [DEFAULT] section are options for each
                    CHM task which are delimited by a [#] where # is a number
                    starting at 1.

                    Example default:

                    [DEFAULT]
                    chmutilversion = {version}
                    images = /home/foo/images
                    chmbin = /foo/chm.img
                    model = /home/foo/trainedmodel
                    tilesperjob = 50
                    tilesize = 512x512
                    overlapsize = 0x0
                    disablehisteqimages = False
                    jobspernode = 1
                    cluster = rocce

                    Example task:

                    [1]
                    inputimage = someimage.png
                    args = -t 1,1 -t 1,2 -t 1,3
                    outputimage = someimage.png/001.someimage.png


              {mergeconfig}
                 -- Configuration containing merge tasks

              runjobs.<cluster>
                  -- Cluster submit script

              {rundir}/
                  -- Directory containing output of job

              {rundir}/{tiles}
                  -- Directory containing a directory for every
                     image where the intermediate tile images can
                     be written

              {rundir}/{stdout}
                  -- Directory containing output from CHM tasks

              {rundir}/{mergestdout}
                 -- Directory containing output from merge tasks

              {rundir}/{tmp}
                 -- Directory used to hold temporary CHM outputs

              {rundir}/{probmaps}
                 -- Directory containing finished probability map images
                    generated from merge phase

              {rundir}/{overlaymaps}
                 -- Directory containing overlay images where input images
                    are overlayed with probability maps. This is generated in
                    merge phase

              {rundir}/<image.png>
                 -- Directories containing image tiles from individual
                    CHM tasks


              Example Usage:

              createchmjob.py ./images ./model ./mychmjob

              Once job is created invoke checkchmjob.py for job submission.
              """.format(version=chmutil.__version__,
                         config=CHMJobCreator.CONFIG_FILE_NAME,
                         mergeconfig=CHMJobCreator.MERGE_CONFIG_FILE_NAME,
                         rundir=CHMJobCreator.RUN_DIR,
                         stdout=CHMJobCreator.STDOUT_DIR,
                         mergestdout=CHMJobCreator.MERGE_STDOUT_DIR,
                         tmp=CHMJobCreator.TMP_DIR,
                         probmaps=CHMJobCreator.PROBMAPS_DIR,
                         overlaymaps=CHMJobCreator.OVERLAYMAPS_DIR,
                         tiles=CHMJobCreator.TILES_DIR)

    theargs = _parse_arguments(desc, arglist[1:])
    theargs.program = arglist[0]
    theargs.version = chmutil.__version__
    core.setup_logging(logger, loglevel=theargs.loglevel)
    try:
        theargs.rawargs = ' '.join(arglist)
        return _create_chm_job(theargs)
    finally:
        logging.shutdown()


if __name__ == '__main__':  # pragma: no cover
    sys.exit(main(sys.argv))
