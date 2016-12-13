===============================
chmutil
===============================



.. image:: https://pyup.io/repos/github/coleslaw481/chmutil/shield.svg
     :target: https://pyup.io/repos/github/coleslaw481/chmutil/
     :alt: Updates

.. image:: https://travis-ci.org/CRBS/chmutil.svg?branch=master
       :target: https://travis-ci.org/CRBS/chmutil

.. image:: https://coveralls.io/repos/github/CRBS/chmutil/badge.svg?branch=master
   :target: https://coveralls.io/github/CRBS/chmutil?branch=master

Utility package to run `Cascaded Hierarchical Model (CHM) <https://www.sci.utah.edu/software/chm.html>`_ jobs on clusters.

Overview of how jobs are broken up:


.. image:: https://github.com/CRBS/chmutil/blob/master/images/chmutil.png


Tools
--------

* **chmrunner.py** -- Runs CHM job with specific jobid

* **createchmjob.py** -- Creates a set of CHM jobs to process a set of images

* **checkchmjob.py** -- Generates script that can be used to run CHM jobs created by **createchmjob.py** on various compute clusters (Gordon, Comet, & Rocce)


Dependencies
--------------

* `argparse <https://pypi.python.org/pypi/argparse>`_

* `configparser <https://pypi.python.org/pypi/configparser>`_

* `Pillow <https://pypi.python.org/pypi/Pillow>`_

* `CHM singularity image <https://github.com/crbs/chm_singularity>`_ (not required to build this software, but is needed to run the jobs)

Compatibility
-------------

* Should work on Python 2.6 & 2.7 on Linux distributions

Installation
------------

Try one of these:

::

  make dist
  pip install dist/chmutil*whl

  # or

  python setup.py install

  # soon will be available via pip install chmutil

  # NOTE if super user not available try this

  pip install --user dist/chmutil*whl


Usage
--------

::

  # creates a job assuming images are in ./images and ./trainedmodel has
  # chm model
  createchmjob.py ./images ./trainedmodel myrun --cluster rocce

License
-------

See LICENSE.txt_


Bugs
-----

Please report them `here <https://github.com/CRBS/chmutil/issues>`_


Credits
---------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _NCMIR: https://ncmir.ucsd.edu/
.. _LICENSE.txt: https://github.com/CRBS/chmutil/blob/master/LICENSE.txt
.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage

