import sys
import argparse
import logging
import os
import datetime
import chmutil
import stat

from chmutil.core import Parameters
from chmutil.cluster import ClusterFactory
from chmutil import core

# create logger
logger = logging.getLogger('chmutil.createchmtrainjob')

MODEL_DIR = 'model'
TMP_DIR = 'tmp'
STDOUT_DIR = 'stdout'
README_FILE = 'readme.txt'
RUNTRAIN = 'runtrain'
RUNTRAIN_COMET = RUNTRAIN + '.comet'
RUNTRAIN_GORDON = RUNTRAIN + '.gordon'
RUNTRAIN_ROCCE = RUNTRAIN + '.rocce'

README_BODY = """chmutil job to generate CHM trained model
===========================================================

Chmutil version: {version}
Date: {date}

Contents of this directory were created by invocation of
createchmtrainjob.py with the following command line:

{commandline}

To check job status invoke the following cluster specific command:

On Gordon and Rocce:

qstat -t -u "$USER"

On Comet:

squeue -u "$USER"

For more help please visit wiki here:

https://github.com/CRBS/chmutil/wiki

For bugs/issues feel free to open a ticket here:

https://github.com/CRBS/chmutil/issues


Below is a description of data in this directory
================================================

{readme}
    -- Readme file containing arguments passed to
       this script and definitions of files and
       directories

{runtrain}.<cluster>
    -- Cluster submit script

{stdout}/
    -- Directory containing output from CHM train task

{tmp}/
    -- Directory used to hold temporary CHM train outputs

{model}/
    -- Directory containing trained model

"""


class InvalidOutDirError(Exception):
    """Invalid output directory
    """
    pass


class UnsupportedClusterError(Exception):
    """Invalid Cluster
    """
    pass


def _parse_arguments(desc, args):
    """Parses command line arguments using argparse.
    """
    parsed_arguments = Parameters()

    help_formatter = argparse.RawDescriptionHelpFormatter
    parser = argparse.ArgumentParser(description=desc,
                                     formatter_class=help_formatter)
    parser.add_argument("images", help='Directory of images')
    parser.add_argument("labels", help='Directory containing label images')
    parser.add_argument("outdir", help='Output directory')
    parser.add_argument("--log", dest="loglevel", choices=['DEBUG',
                        'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help="Set the logging level (default WARNING)",
                        default='WARNING')
    parser.add_argument("--chmbin", help='Full path to chm binary',
                        default='./chm-0.1.0.img')
    parser.add_argument('--stage', type=int, default=2,
                        help='The number of stages of training to perform. '
                             'Must be >=2')
    parser.add_argument('--level', type=int, default=4,
                        help='The number of levels of training to perform. '
                             'Must be >=4')
    parser.add_argument('--cluster', default='rocce',
                        choices=ClusterFactory.VALID_CLUSTERS,
                        help='Sets which cluster to generate job script for'
                             ' (default rocce)')
    parser.add_argument('--account', default='',
                        help='Sets account to charge processing to. Needed'
                             'for jobs run on Gordon & Comet (default \'\')')
    parser.add_argument('--jobname', default='chmtrainjob',
                        help='Name for job given to scheduler, must not '
                             'contain any non alphanumeric characters and '
                             'must start with letter a-z')
    parser.add_argument('--walltime', default='24:00:00',
                        help='Sets walltime for job in HH:MM:SS format '
                             'default(24:00:00) ')
    parser.add_argument('--maxmem', default=90,
                        help='Sets maximum memory in gigabytes job needs to '
                             'run (default 90)')
    parser.add_argument('--version', action='version',
                        version=('%(prog)s ' + chmutil.__version__))

    return parser.parse_args(args, namespace=parsed_arguments)


def _create_submit_script(theargs):
    """Creates runtrain.CLUSTER file using values in theargs
    parameter to figure out what CLUSTER the job is for as
    well as what parameters to pass to CHM train
    :param theargs: parameters from _parse_arguments() function
    :return: human readable string describing how to submit job
             which will also be appended to the readme.txt file
             for the job.
    """
    cf = ClusterFactory()
    clust = cf.get_cluster_by_name(theargs.cluster)
    if clust is None:
        raise UnsupportedClusterError(theargs.cluster +
                                      ' is not a known cluster')

    cmd, script = clust.generate_train_submit_script(theargs.outdir,
                                                     theargs.images,
                                                     theargs.labels,
                                                     theargs.stage,
                                                     theargs.level,
                                                     theargs.walltime,
                                                     theargs.chmbin,
                                                     account=theargs.account,
                                                     job_name=theargs.jobname,
                                                     max_mem=theargs.maxmem)

    f = open(os.path.join(theargs.outdir, README_FILE), 'a')
    f.write('\n\n' + cmd)
    f.flush()
    f.close()
    return cmd


def _create_directories_and_readme(outdir, rawargs):
    """Creates job directories and readme.txt file
    :param outdir: Full path to output directory
    :param rawargs: Arguments passed to this commandline script
    """

    # create directory if it does not exist
    if os.path.exists(outdir):
        if not os.path.isdir(outdir):
            raise InvalidOutDirError(outdir + ' exists, but is not a '
                                              'directory')
    else:
        logger.info('Creating directory: ' + outdir)
        os.makedirs(outdir, mode=0o755)

    # create stdout, tmp, model directories
    os.makedirs(os.path.join(outdir, STDOUT_DIR), mode=0o755)
    os.makedirs(os.path.join(outdir, TMP_DIR), mode=0o755)
    os.makedirs(os.path.join(outdir, MODEL_DIR), mode=0o755)

    readme = README_BODY.format(version=chmutil.__version__,
                                stdout=STDOUT_DIR,
                                tmp=TMP_DIR,
                                model=MODEL_DIR,
                                commandline=rawargs,
                                date=str(datetime.datetime.today()),
                                readme=README_FILE,
                                runtrain=RUNTRAIN)
    f = open(os.path.join(outdir, README_FILE), 'w')
    f.write(readme)
    f.flush()
    f.close()
    return


def _create_chmtrain_job(theargs):
    """Creates CHM Job
    :param theargs: list of arguments obtained from _parse_arguments()
    :returns: exit code for program. 0 success otherwise failure
    """
    _create_directories_and_readme(theargs.outdir, theargs.rawargs)
    submit_cmd = _create_submit_script(theargs)
    sys.stdout.write(submit_cmd)
    return 0


def main(arglist):
    """Main function
    :param arglist: Should be set to sys.argv which is list of arguments
                    passed on commandline including script being run as arg 0
    :returns: exit code. 0 is success otherwise failure
    """
    desc = """
              Version {version}

              Creates script to run CHM train on images in <images> directory
              and labels in <labels> directory. Putting trained
              model under <outdir>/{model}/
              The generated script is put in <outdir>.

              Here is a breakdown of the following directories and files
              created under the <outdir>:

              {readme}
                  -- Readme file containing arguments passed to
                     this script and definitions of files and
                     directories

              {runtrain}.<cluster>
                  -- Cluster submit script

              {stdout}/
                  -- Directory containing output from CHM train task

              {tmp}/
                 -- Directory used to hold temporary CHM train outputs

              {model}/
                 -- Directory containing trained model


              Example Usage:

              createchmtrainjob.py ./images ./labels ./run --cluster rocce

              """.format(version=chmutil.__version__,
                         stdout=STDOUT_DIR,
                         tmp=TMP_DIR,
                         model=MODEL_DIR,
                         readme=README_FILE,
                         runtrain=RUNTRAIN)

    theargs = _parse_arguments(desc, arglist[1:])
    theargs.program = arglist[0]
    theargs.version = chmutil.__version__
    core.setup_logging(logger, loglevel=theargs.loglevel)
    try:
        theargs.rawargs = ' '.join(arglist)
        return _create_chmtrain_job(theargs)
    finally:
        logging.shutdown()


if __name__ == '__main__':  # pragma: no cover
    sys.exit(main(sys.argv))
