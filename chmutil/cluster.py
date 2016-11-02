# -*- coding: utf-8 -*-


import os
import stat
import logging
import configparser
from chmutil.core import CHMConfig
from chmutil.core import CHMJobCreator

logger = logging.getLogger(__name__)


class RocceSubmitScriptGenerator(object):
    """Generates submit script for CHM job on Rocce cluster
    """
    SUBMIT_SCRIPT_NAME = 'runjobs.rocce'

    def __init__(self, chmconfig):
        """Constructor
        :param chmconfig: CHMConfig object for the job
        """
        self._chmconfig = chmconfig

    def _get_submit_script_path(self):
        """Gets path to submit script
        """
        return os.path.join(self._chmconfig.get_out_dir(),
                     RocceSubmitScriptGenerator.SUBMIT_SCRIPT_NAME)

    def _get_instructions(self):
        """get instructions
        """
        return 'blah 'blah'

    def generate_submit_script(self):
        """Creates submit script and instructions for invocation
        :returns: tuple (path to submit script, instructions for submit)
        """
        script = self._get_submit_script_path()
        out_dir = self._chmconfig.get_out_dir()
        f = open(script, 'w')
        f.write('#!/bin/sh\n')
        f.write('#\n#$ -V\n#$ -S /bin/sh\n')
        f.write('#$ -wd ' + out_dir + '\n')
        f.write('#$ -o ' + os.path.join(self._chmconfig.get_stdout_dir(),
                                        '$JOB_ID.$TASK_ID.out') + '\n')
        f.write('#$ -j y\n#$ -N chmjob\n')
        f.write('#$ -l h_rt=12:00:00,h_vmem=5G,h=\'!compute-0-20\'\n')
        f.write('#$ -q all.q\n#$ -m n\n\n')
        f.write('echo "HOST: $HOSTNAME"\n')
        f.write('echo "DATE: `date`"\n\n')
        f.write('/usr/bin/time -p ' + self._chmconfig.get_chm_binary() +
                ' $SGE_TASK_ID ' + out_dir + ' --scratchdir ' +
                self._chmconfig.get_shared_tmp_dir() + ' --log DEBUG\n')
        f.flush()
        f.close()
        os.chmod(script, stat.S_IRWXU)
        return script, self._get_instructions()


