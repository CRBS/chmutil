#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_chmutil
----------------------------------

Tests for `chmutil` module.
"""

import tempfile
import shutil
import os
import unittest
import stat

from chmutil.cluster import Scheduler
from chmutil.cluster import SLURMScheduler
from chmutil.cluster import SGEScheduler
from chmutil.cluster import PBSScheduler
from chmutil.cluster import SchedulerFactory
from chmutil.cluster import InvalidScriptNameError
from chmutil.cluster import InvalidWorkingDirError


class TestScheduler(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_getters(self):
        sched = Scheduler(None)
        self.assertEqual(sched.get_clustername(), None)
        self.assertEqual(sched.get_jobid_for_arrayjob_variable(), None)
        self.assertEqual(sched.get_jobid_variable(), None)
        self.assertEqual(sched.get_taskid_variable(), None)
        self.assertEqual(sched.get_job_out_file_name(), 'None' +
                         Scheduler.OUT_SUFFIX)
        self.assertEqual(sched.get_array_job_out_file_name(), 'None.None' +
                         Scheduler.OUT_SUFFIX)
        self.assertEqual(sched._get_script_header(None, None, None, None), '')

        sched = Scheduler('foo', queue='queue', account='account',
                          jobid_for_filepath='jobidfilepath',
                          jobid_for_arrayjob='arrayjobid',
                          jobid='jobid',
                          taskid_for_filepath='filepathtaskid',
                          taskid='taskid',
                          submitcmd='submit',
                          arrayflag='aflag',
                          load_singularity_cmd='cmd')
        self.assertEqual(sched.get_clustername(), 'foo')
        self.assertEqual(sched.get_jobid_for_arrayjob_variable(),
                         'arrayjobid')
        self.assertEqual(sched.get_jobid_variable(), 'jobid')
        self.assertEqual(sched.get_taskid_variable(), 'taskid')
        self.assertEqual(sched.get_job_out_file_name(),
                         'jobidfilepath' +
                         Scheduler.OUT_SUFFIX)
        self.assertEqual(sched.get_array_job_out_file_name(),
                         'jobidfilepath.filepathtaskid' +
                         Scheduler.OUT_SUFFIX)
        self.assertEqual(sched._get_script_header(None, None, None, None), '')
        sched.set_account('hi')

    def test_make_script_executable(self):
        temp_dir = tempfile.mkdtemp()
        try:
            tfile = os.path.join(temp_dir, 'foo.txt')
            open(tfile, 'a').close()
            res = stat.S_IMODE(os.stat(tfile).st_mode) & stat.S_IXUSR
            self.assertEqual(res, 0)
            sched = Scheduler(None)
            sched._make_script_executable(tfile)
            res = stat.S_IMODE(os.stat(tfile).st_mode) & stat.S_IXUSR
            self.assertTrue(res > 0)

            # test non existant file, has no effect other then logging
            sched._make_script_executable(os.path.join(temp_dir,
                                                       'doesnotexist'))
        finally:
            shutil.rmtree(temp_dir)

    def test_generate_submit_command(self):

        sched = Scheduler('foo')
        # number_tasks, submitcmd, arrayflag is none
        res = sched._generate_submit_command('script', '/foo', None)
        self.assertEqual(res, 'To submit run: cd /foo;   script')

        # submitcmd, arrayflag is none
        res = sched._generate_submit_command('script', '/foo', 1)
        self.assertEqual(res, 'To submit run: cd /foo;   script')

        # submitcmd is none
        sched = Scheduler('foo', arrayflag='-t')
        res = sched._generate_submit_command('script', '/foo', 3)
        self.assertEqual(res, 'To submit run: cd /foo;   -t 1-3 script')

        sched = Scheduler('foo', arrayflag='-t', submitcmd='qsub')
        res = sched._generate_submit_command('script', '/foo', 3)
        self.assertEqual(res, 'To submit run: cd /foo; qsub -t 1-3 script')

        # number tasks is none
        res = sched._generate_submit_command('script', '/foo', None)
        self.assertEqual(res, 'To submit run: cd /foo; qsub script')

    def test_write_submit_script_invalid_script_name(self):
        sched = Scheduler('foo')
        try:
            sched.write_submit_script(None, 'hi', 'foo', 'yo', 'hi', '')
            self.fail('Expected InvalidScriptNameError')
        except InvalidScriptNameError as e:
            self.assertEqual(str(e), 'Script name cannot be None')

    def test_write_submit_script_invalid_working_dir(self):
        sched = Scheduler('foo')
        try:
            sched.write_submit_script('bye', None, 'foo', 'yo', 'hi', '')
            self.fail('Expected InvalidWorkingDirError')
        except InvalidWorkingDirError as e:
            self.assertEqual(str(e), 'Working dir cannot be None')

    def test_write_submit_script_unable_to_write_file(self):
        temp_dir = tempfile.mkdtemp()
        try:
            sched = Scheduler('foo')
            invalid_dir = os.path.join(temp_dir, 'nonexistantdir')
            sched.write_submit_script('myscript', invalid_dir,
                                      'foo', 'yo', 'hi', '')
            self.fail('Expected IOError')
        except IOError:
            pass
        finally:
            shutil.rmtree(temp_dir)

    def test_write_submit_script_singularity_cmd_is_none(self):
        temp_dir = tempfile.mkdtemp()
        try:
            sched = Scheduler('foo')
            cmd, myscript = sched.write_submit_script('myscript', temp_dir,
                                                      'foo', 'yo', 'hi', '')
            self.assertTrue(os.path.isfile(myscript))
            self.assertEqual(cmd, 'To submit run: cd ' + temp_dir + ';   ' +
                             myscript)
            f = open(myscript, 'r')
            self.assertEqual(f.read(), '')
            f.close()
        finally:
            shutil.rmtree(temp_dir)

    def test_write_submit_script_singularity_cmd_is_set(self):
        temp_dir = tempfile.mkdtemp()
        try:
            sched = Scheduler('foo', load_singularity_cmd='hi')
            cmd, myscript = sched.write_submit_script('myscript', temp_dir,
                                                      'foo', 'yo', 'hi', '')
            self.assertTrue(os.path.isfile(myscript))
            self.assertEqual(cmd, 'To submit run: cd ' + temp_dir + ';   ' +
                             myscript)
            f = open(myscript, 'r')
            self.assertEqual(f.read(), 'hi')
            f.close()
        finally:
            shutil.rmtree(temp_dir)

    def test_slurmgetters(self):
        sched = SLURMScheduler('foo')
        self.assertEqual(sched.get_clustername(), 'foo')
        self.assertEqual(sched.get_jobid_for_arrayjob_variable(),
                         '$SLURM_ARRAY_JOB_ID')
        self.assertEqual(sched.get_jobid_variable(), '$SLURM_JOB_ID')
        self.assertEqual(sched.get_taskid_variable(), '$SLURM_ARRAY_TASK_ID')
        self.assertEqual(sched.get_job_out_file_name(), '%A' +
                         Scheduler.OUT_SUFFIX)
        self.assertEqual(sched.get_array_job_out_file_name(), '%A.%a' +
                         Scheduler.OUT_SUFFIX)

    def test_slurm_get_script_header(self):
        sched = SLURMScheduler('foo')
        # all args and account, queue none
        self.assertEqual(sched._get_script_header(None, None, None, None),
                         '#!/bin/sh\n#\n#SBATCH --nodes=1\n#SBATCH '
                         '--export=SLURM_UMASK=0022\n')

        # no args none account & queue none
        self.assertEqual(sched._get_script_header('/foo', '/stdoutpath',
                                                  'name', '1:00:00',
                                                  required_mem_gb=1,
                                                  number_tasks=2),
                         '#!/bin/sh\n#\n#SBATCH --nodes=1\n#SBATCH -D /foo\n'
                         '#SBATCH --export=SLURM_UMASK=0022\n#SBATCH -o '
                         '/stdoutpath\n#SBATCH -J name\n#SBATCH -t 1:00:00'
                         '\n\n')

        # everything set
        sched = SLURMScheduler('foo', queue='compute', account='myact')
        self.assertEqual(sched._get_script_header('/foo', '/stdoutpath',
                                                  'name', '1:00:00',
                                                  required_mem_gb=1,
                                                  number_tasks=2),
                         '#!/bin/sh\n#\n#SBATCH --nodes=1\n#SBATCH -A myact\n'
                         '#SBATCH -D /foo\n#SBATCH -p compute\n#SBATCH '
                         '--export=SLURM_UMASK=0022\n#SBATCH -o /stdoutpath\n'
                         '#SBATCH -J name\n#SBATCH -t 1:00:00\n\n')

    def test_slurm_write_submit_script(self):
        temp_dir = tempfile.mkdtemp()
        try:
            sched = SLURMScheduler('foo',
                                   load_singularity_cmd='module load sing\n')
            cmd, script = sched.write_submit_script('scripty', temp_dir,
                                                    '/stdout', 'namey',
                                                    '1:00:00', '/bin/foo\n')
            self.assertEqual(cmd, 'To submit run: cd ' + temp_dir +
                             '; sbatch ' + os.path.join(temp_dir, 'scripty'))

            f = open(script, 'r')
            self.assertEqual(f.read(),
                             '#!/bin/sh\n#\n#SBATCH --nodes=1\n'
                             '#SBATCH -D ' + temp_dir + '\n#SBATCH '
                             '--export=SLURM_UMASK=0022\n#SBATCH -o /stdout\n'
                             '#SBATCH -J namey\n#SBATCH -t 1:00:00\n\n'
                             'module load sing\n/bin/foo\n')
            f.close()
        finally:
            shutil.rmtree(temp_dir)

    def test_sgegetters(self):
        sched = SGEScheduler('foo')
        self.assertEqual(sched.get_clustername(), 'foo')
        self.assertEqual(sched.get_jobid_for_arrayjob_variable(), '$JOB_ID')
        self.assertEqual(sched.get_jobid_variable(), '$JOB_ID')
        self.assertEqual(sched.get_taskid_variable(), '$SGE_TASK_ID')
        self.assertEqual(sched.get_job_out_file_name(), '$JOB_ID' +
                         Scheduler.OUT_SUFFIX)
        self.assertEqual(sched.get_array_job_out_file_name(),
                         '$JOB_ID.$TASK_ID' +
                         Scheduler.OUT_SUFFIX)

    def test_sge_get_script_header(self):
        sched = SGEScheduler('foo')
        # all args and account, queue none
        self.assertEqual(sched._get_script_header(None, None, None, None),
                         '#!/bin/sh\n#$ -V\n#$ -S /bin/sh\n#$ -notify\n\n\n')

        # no args none account & queue none
        self.assertEqual(sched._get_script_header('/foo', '/stdoutpath',
                                                  'name', '1:00:00',
                                                  required_mem_gb=1,
                                                  number_tasks=2),
                         '#!/bin/sh\n#$ -V\n#$ -S /bin/sh\n#$ -notify\n#$ -wd '
                         '/foo\n#$ -j y\n#$ -o /stdoutpath\n#$ -N name\n#$ -l '
                         'h_rt=1:00:00,h_vmem=1G\n\n')

        # everything set
        sched = SGEScheduler('foo', queue='compute', account='myact')
        self.assertEqual(sched._get_script_header('/foo', '/stdoutpath',
                                                  'name', '1:00:00',
                                                  required_mem_gb=1,
                                                  number_tasks=2),
                         '#!/bin/sh\n#$ -V\n#$ -S /bin/sh\n#$ -notify\n#$ -wd '
                         '/foo\n#$ -q compute\n#$ -j y\n#$ -o /stdoutpath\n#$ '
                         '-N name\n#$ -l h_rt=1:00:00,h_vmem=1G\n\n')

        # mem set, but walltime unset
        sched = SGEScheduler('foo', queue='compute', account='myact')
        self.assertEqual(sched._get_script_header('/foo', '/stdoutpath',
                                                  'name', None,
                                                  required_mem_gb=1,
                                                  number_tasks=2),
                         '#!/bin/sh\n#$ -V\n#$ -S /bin/sh\n#$ -notify\n#$ -wd '
                         '/foo\n#$ -q compute\n#$ -j y\n#$ -o /stdoutpath\n#$ '
                         '-N name\n#$ -l h_vmem=1G\n\n')

        # neither mem set, walltime set
        sched = SGEScheduler('foo', queue='compute', account='myact')
        self.assertEqual(sched._get_script_header('/foo', '/stdoutpath',
                                                  'name', None,
                                                  number_tasks=2),
                         '#!/bin/sh\n#$ -V\n#$ -S /bin/sh\n#$ -notify\n#$ -wd '
                         '/foo\n#$ -q compute\n#$ -j y\n#$ -o /stdoutpath\n#$ '
                         '-N name\n\n\n')

    def test_sge_write_submit_script(self):
        temp_dir = tempfile.mkdtemp()
        try:
            sched = SGEScheduler('foo',
                                 load_singularity_cmd='module load sing\n')
            cmd, script = sched.write_submit_script('scripty', temp_dir,
                                                    '/stdout', 'namey',
                                                    '1:00:00', '/bin/foo\n')
            self.assertEqual(cmd, 'To submit run: cd ' + temp_dir + '; qsub ' +
                             os.path.join(temp_dir, 'scripty'))

            f = open(script, 'r')
            self.assertEqual(f.read(),
                             '#!/bin/sh\n#$ -V\n#$ -S /bin/sh\n#$ -notify\n#$ '
                             '-wd ' + temp_dir + '\n#$ -j y\n#$ -o '
                             '/stdout\n#$ '
                             '-N namey\n#$ -l h_rt=1:00:00\n\nmodule load '
                             'sing\n/bin/foo\n')
            f.close()
        finally:
            shutil.rmtree(temp_dir)

    def test_pbsgetters(self):
        sched = PBSScheduler('foo')
        self.assertEqual(sched.get_clustername(), 'foo')
        self.assertEqual(sched.get_jobid_for_arrayjob_variable(), '$PBS_JOBID')
        self.assertEqual(sched.get_jobid_variable(), '$PBS_JOBID')
        self.assertEqual(sched.get_taskid_variable(), '$PBS_ARRAYID')
        self.assertEqual(sched.get_job_out_file_name(), '$PBS_JOBID' +
                         Scheduler.OUT_SUFFIX)
        self.assertEqual(sched.get_array_job_out_file_name(),
                         '$PBS_JOBID.$PBS_ARRAYID' +
                         Scheduler.OUT_SUFFIX)

    def test_pbs_get_script_header(self):
        sched = PBSScheduler('foo')
        # all args and account, queue none
        self.assertEqual(sched._get_script_header(None, None, None, None),
                         '#!/bin/sh\n#\n#PBS -V\n#PBS -m n\n#PBS -l '
                         'nodes=1:ppn=16:native:noflash\n')

        # no args none account & queue none
        self.assertEqual(sched._get_script_header('/foo', '/stdoutpath',
                                                  'name', '1:00:00',
                                                  required_mem_gb=1,
                                                  number_tasks=2),
                         '#!/bin/sh\n#\n#PBS -V\n#PBS -m n\n#PBS -wd /foo\n'
                         '#PBS -l '
                         'nodes=1:ppn=16:native:noflash\n#PBS -t 1-2\n'
                         '#PBS -j oe\n#PBS -o /stdoutpath\n#PBS -N name\n'
                         '#PBS -l walltime=1:00:00\n')

        # everything set
        sched = PBSScheduler('foo', queue='compute', account='myact')
        self.assertEqual(sched._get_script_header('/foo', '/stdoutpath',
                                                  'name', '1:00:00',
                                                  required_mem_gb=1,
                                                  number_tasks=2),
                         '#!/bin/sh\n#\n#PBS -V\n#PBS -m n\n#PBS -wd /foo\n'
                         '#PBS -q compute\n#PBS -A myact\n#PBS -l '
                         'nodes=1:ppn=16:native:noflash\n#PBS -t 1-2\n'
                         '#PBS -j oe\n#PBS -o /stdoutpath\n#PBS -N name\n'
                         '#PBS -l walltime=1:00:00\n')

        # mem set, but walltime unset
        sched = PBSScheduler('foo', queue='compute', account='myact')
        self.assertEqual(sched._get_script_header('/foo', '/stdoutpath',
                                                  'name', None,
                                                  required_mem_gb=1,
                                                  number_tasks=2),
                         '#!/bin/sh\n#\n#PBS -V\n#PBS -m n\n#PBS -wd /foo\n'
                         '#PBS -q compute\n#PBS -A myact\n#PBS -l '
                         'nodes=1:ppn=16:native:noflash\n#PBS -t 1-2\n'
                         '#PBS -j oe\n#PBS -o /stdoutpath\n#PBS -N name\n')

        # neither mem set, walltime set
        sched = PBSScheduler('foo', queue='compute', account='myact')
        self.assertEqual(sched._get_script_header('/foo', '/stdoutpath',
                                                  'name', None,
                                                  number_tasks=2),
                         '#!/bin/sh\n#\n#PBS -V\n#PBS -m n\n#PBS -wd /foo\n'
                         '#PBS -q compute\n#PBS -A myact\n#PBS -l '
                         'nodes=1:ppn=16:native:noflash\n#PBS -t 1-2\n'
                         '#PBS -j oe\n#PBS -o /stdoutpath\n#PBS -N name\n')

    def test_pbs_write_submit_script(self):
        temp_dir = tempfile.mkdtemp()
        try:
            sched = PBSScheduler('foo',
                                 load_singularity_cmd='module load sing\n')
            cmd, script = sched.write_submit_script('scripty', temp_dir,
                                                    '/stdout', 'namey',
                                                    '1:00:00', '/bin/foo\n',
                                                    number_tasks=3)
            self.assertEqual(cmd, 'To submit run: cd ' + temp_dir + '; qsub ' +
                             os.path.join(temp_dir, 'scripty'))

            f = open(script, 'r')
            self.assertEqual(f.read(),
                             '#!/bin/sh\n#\n#PBS -V\n#PBS -m n\n#PBS -wd ' +
                             temp_dir + '\n#PBS -l '
                             'nodes=1:ppn=16:native:noflash\n#PBS -t 1-3\n'
                             '#PBS -j oe\n#PBS -o /stdout\n#PBS -N namey\n'
                             '#PBS -l walltime=1:00:00\nmodule load sing\n'
                             '/bin/foo\n')
            f.close()
        finally:
            shutil.rmtree(temp_dir)

    def test_scheduler_factory_get_scheduler_by_cluster_name(self):
        sf = SchedulerFactory()
        self.assertEqual(sf.get_scheduler_by_cluster_name(None), None)

        self.assertEqual(sf.get_scheduler_by_cluster_name('foo'), None)

        sched = sf.get_scheduler_by_cluster_name(SchedulerFactory.COMET)
        self.assertEqual(sched.get_clustername(), SchedulerFactory.COMET)

        sched = sf.get_scheduler_by_cluster_name(SchedulerFactory.ROCCE)
        self.assertEqual(sched.get_clustername(), SchedulerFactory.ROCCE)

        sched = sf.get_scheduler_by_cluster_name(SchedulerFactory.GORDON)
        self.assertEqual(sched.get_clustername(), SchedulerFactory.GORDON)


if __name__ == '__main__':
    unittest.main()
