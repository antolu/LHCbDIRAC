###############################################################################
# (c) Copyright 2019 CERN for the benefit of the LHCb Collaboration           #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".   #
#                                                                             #
# In applying this licence, CERN does not waive the privileges and immunities #
# granted to it by virtue of its status as an Intergovernmental Organization  #
# or submit itself to any jurisdiction.                                       #
###############################################################################
""" Running this file with "ganga gangaJobs.py" will submit some user jobs via Ganga
"""


#pylint: skip-file

myApp = prepareGaudiExec('DaVinci','v41r2', myPath='.')


j = Job(name='GangaJob-DVv41r2-wInputs')
j.application = myApp
j.application.options = ['ntuple_options_grid.py']
j.application.readInputData('inputdata.py')
j.backend = Dirac()
j.backend.settings['Destination'] = 'LCG.CERN.cern'
j.submitJob()

jBK = Job(name='GangaJob-DVv41r2-wInputBKK')
jBK.application = myApp
jBK.application.options = ['ntuple_options_grid_withBKK.py']
jBK.backend = Dirac()
jBK.submitJob()
