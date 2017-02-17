#! /usr/bin/env python

import sys
import argparse
import logging
import os
import datetime
import chmutil

from chmutil.core import Parameters
from chmutil.cluster import SchedulerFactory
from chmutil import core

# create logger
logger = logging.getLogger('chmutil.createchmtrainjob')

MODEL_DIR = 'model'
TMP_DIR = 'tmp'
IMAGES_DIR = 'images'
LABELS_DIR = 'labels'
STDOUT_DIR = 'stdout'
README_FILE = 'readme.txt'
RUNTRAIN = 'runtrain.'

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

{runtrain}<cluster>
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

class IMODConversionError(Exception):
    """Problems converting IMOD files to training data format
    """
    pass


def _parse_arguments(desc, args):
    """Parses command line arguments using argparse.
    """
    parsed_arguments = Parameters()

    help_formatter = argparse.RawDescriptionHelpFormatter
    parser = argparse.ArgumentParser(description=desc,
                                     formatter_class=help_formatter)
    parser.add_argument("images", help='Directory of images or mrc file,'
                                       'mrc file requires IMOD to'
                                       'be installed and mod file to be'
                                       'passed into <labels>')
    parser.add_argument("labels", help='Directory containing label images or '
                                       'mod file. mod file can only be passed'
                                       'if mrc file is passed into <images>'
                                       'and IMOD is installed on system')
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
                             'Must be >=1')
    parser.add_argument('--cluster', default='rocce',
                        choices=[SchedulerFactory.ROCCE,
                                 SchedulerFactory.COMET,
                                 SchedulerFactory.GORDON],
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
    parser.add_argument('--maxmem', default=90, type=int,
                        help='Sets maximum memory in gigabytes job needs to '
                             'run (default 90)')
    parser.add_argument('--imodbindir', default='',
                        help='Sets bin directory for IMOD commands. '
                             'Only needed if mrc and mod files are'
                             'passed into <images> and <labels> '
                             '(default \'\')')
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
    sf = SchedulerFactory()
    sched = sf.get_scheduler_by_cluster_name(theargs.cluster)
    if sched is None:
        raise UnsupportedClusterError(theargs.cluster +
                                      ' is not known')

    sched.set_account(theargs.account)
    tmpdir = os.path.abspath(os.path.join(theargs.outdir, TMP_DIR))
    modeldir = os.path.abspath(os.path.join(theargs.outdir, MODEL_DIR))
    cmd = ('/usr/bin/time -v ' + os.path.abspath(theargs.chmbin) +
           ' train ' + os.path.abspath(theargs.images) + ' ' +
           os.path.abspath(theargs.labels) +
           ' -S ' + str(theargs.stage) + ' -L ' + str(theargs.level) +
           ' -m ' + tmpdir +
           '\nexitcode=$?\n' +
           'mv -f ' + tmpdir + ' ' + modeldir + '\n'
           'echo "' + os.path.basename(theargs.chmbin) +
           ' exited with code: $exitcode"\n'
           'exit $exitcode\n')
    stdout_path = os.path.abspath(os.path.join(theargs.outdir, STDOUT_DIR,
                                  sched.get_job_out_file_name()))
    ucmd, script = sched.write_submit_script(RUNTRAIN +
                                             sched.get_clustername(),
                                             os.path.abspath(theargs.outdir),
                                             stdout_path,
                                             theargs.jobname,
                                             theargs.walltime,
                                             cmd,
                                             required_mem_gb=theargs.maxmem)

    f = open(os.path.join(theargs.outdir, README_FILE), 'a')
    f.write('\n\n' + ucmd)
    f.flush()
    f.close()
    return ucmd


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
    stdoutdir = os.path.join(outdir, STDOUT_DIR)
    if not os.path.isdir(stdoutdir):
        os.makedirs(stdoutdir, mode=0o755)

    tmpdir = os.path.join(outdir, TMP_DIR)
    if not os.path.isdir(tmpdir):
        os.makedirs(tmpdir, mode=0o755)

    cd_with_rawargs = 'cd ' + os.getcwd() + ';' + rawargs

    readme = README_BODY.format(version=chmutil.__version__,
                                stdout=STDOUT_DIR,
                                tmp=TMP_DIR,
                                model=MODEL_DIR,
                                commandline=cd_with_rawargs,
                                date=str(datetime.datetime.today()),
                                readme=README_FILE,
                                runtrain=RUNTRAIN)
    f = open(os.path.join(outdir, README_FILE), 'w')
    f.write(readme)
    f.flush()
    f.close()
    return


def _convert_mod_mrc_files(theargs):
    """Check if images and labels are mrc and mod files, if yes
    use IMOD to convert them to images and labels
    :param theargs: Parameters from _parse_arguments function
    :raises OSError: If there is an error creating images or labels directory
                     under theargs.outdir
    :raises IMODConversionError: If either input is invalid or invocations to
                                 mrc2tif or imodmop fail
    :returns: tuple (updated theargs.images value, updated theargs.labels value)
    """
    if os.path.isdir(theargs.images):
        logger.debug('Images path is a directory, no conversion needed')
        return theargs.images, theargs.labels

    if not os.path.isfile(theargs.images):
        raise IMODConversionError(theargs.images +
                                  ' is not a file, cannot convert')

    tmp_dir = os.path.join(theargs.outdir, TMP_DIR)

    images_dir = os.path.join(theargs.outdir, IMAGES_DIR)
    logger.info(theargs.images + ' is a file, assume its an mrc and'
                ' convert it')

    logger.debug('Creating directory ' + images_dir)

    os.makedirs(images_dir, mode=0o755)

    mrc2tif = os.path.join(theargs.imodbindir, 'mrc2tif')

    logger.debug('Running ' + mrc2tif)
    imageoutpath = os.path.join(images_dir, 'x')
    ecode, out, err = core.run_external_command((mrc2tif + ' -p ' +
                                                theargs.images + ' ' +
                                                imageoutpath), tmp_dir)
    logger.debug('Output from ' + mrc2tif + ': ' + str(out) + ':' + str(err))

    if ecode is not 0:
        raise IMODConversionError('Non zero exit code from mrc2tif: ' +
                                  str(ecode) + ' : ' + str(out) + ' : ' +
                                  str(err))

    if not os.path.isfile(theargs.labels):
        raise IMODConversionError(theargs.labels +
                                  ' is not a file, cannot convert')

    labels_dir = os.path.join(theargs.outdir, LABELS_DIR)
    tmp_mrc = os.path.join(theargs.outdir, TMP_DIR, 'tmp.mrc')
    logger.info(theargs.images + ' is a file, assume its a mod '
                'file and convert it')
    logger.debug('Creating directory ' + labels_dir)
    os.makedirs(labels_dir, mode=0o755)
    imodmop = os.path.join(theargs.imodbindir, 'imodmop')
    logger.debug('Running ' + imodmop)
    ecode, out, err = core.run_external_command((imodmop + ' -mask 1 ' +
                                                theargs.labels + ' ' +
                                                theargs.images + ' ' +
                                                tmp_mrc), tmp_dir)
    logger.info('Output from imodmop: ' + str(out) + ':' + str(err))

    if ecode is not 0:
        raise IMODConversionError('Non zero exit code from imodmop: ' +
                                  str(ecode) + ' : ' + str(out) + ' : ' +
                                  str(err))

    ecode, out, err = core.run_external_command((mrc2tif + ' -p ' +
                                                tmp_mrc + ' ' +
                                                os.path.join(labels_dir,
                                                             'x')), tmp_dir)
    logger.debug('Output from mrc2tif: ' + str(out) + ':' + str(err))

    if ecode is not 0:
        raise IMODConversionError('Non zero exit code from mrc2tif: ' +
                                  str(ecode) + ' : ' + str(out) + ' : ' +
                                  str(err))
    return images_dir, labels_dir


def main(arglist):
    """Main function
    :param arglist: Should be set to sys.argv which is list of arguments
                    passed on commandline including script being run as arg 0
    :returns: exit code. 0 is success otherwise failure
    """
    desc = """
              Version {version}

              Creates script to run CHM train on training data located
              in <images> and <labels> directory. Putting the trained
              model under <outdir>/{model}/

              The generated script is put in <outdir>.

              Here is a breakdown of the following directories and files
              created under the <outdir>:

              {readme}
                  -- Readme file containing arguments passed to
                     this script and definitions of files and
                     directories

              {runtrain}<cluster>
                  -- Cluster submit script

              {stdout}/
                  -- Directory containing output from CHM train task

              {tmp}/
                 -- Directory used to hold temporary CHM train outputs.
                    Once processing is complete this directory is
                    renamed to {model}/

              {model}/
                 -- Directory appears after proccessing completes and
                    contained trained model.


              Example Usage:

              createchmtrainjob.py can take data in two formats, default
              and IMOD extraction mode.


              DEFAULT MODE:
              In default mode the training images are already put into two
              directories. Here is example usage of that mode:

              createchmtrainjob.py ./images ./labels ./run --cluster rocce


              IMOD EXTRACTION MODE:

              If IMOD is installed this script can extract properly structured
              data from .mrc and .mod files. Instead of passing directories to
              <images> and <labels> just pass an mrc and mod file respectively.

              Example:

              createchmtrainjob.py images.mrc labels.mod ./run --cluster rocce

              In the above mode the images.mrc must contain the training images
              and the labels.mod must have the contours for that images.mrc
              file. createchmtrainjob.py will take all the images from
              images.mrc and put them in <outdir>/{images} directory and
              extract all labels from labels.mod and put them in
              <outdir>/{labels}/ directory giving them the name x.###.png.
              createchmtrainjob.py does this by invoking mrc2tif -p on the
              images.mrc and by invoking imodmop -mask 1 on the labels.mod
              file followed by an mrc2tif -p on the mrc file created by
              imodmop.
              """.format(version=chmutil.__version__,
                         stdout=STDOUT_DIR,
                         tmp=TMP_DIR,
                         model=MODEL_DIR,
                         readme=README_FILE,
                         runtrain=RUNTRAIN,
                         images=IMAGES_DIR,
                         labels=LABELS_DIR)

    theargs = _parse_arguments(desc, arglist[1:])
    theargs.program = arglist[0]
    theargs.version = chmutil.__version__
    core.setup_logging(logger, loglevel=theargs.loglevel)
    try:
        theargs.rawargs = ' '.join(arglist)
        _create_directories_and_readme(theargs.outdir, theargs.rawargs)
        theargs.images, theargs.labels = _convert_mod_mrc_files(theargs)
        submit_cmd = _create_submit_script(theargs)
        sys.stdout.write('\n' + submit_cmd + '\n\n')
        return 0
    finally:
        logging.shutdown()


if __name__ == '__main__':  # pragma: no cover
    sys.exit(main(sys.argv))
