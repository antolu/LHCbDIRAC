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
""" Unit tests for LHCbDIRAC RunApplication module
"""


from mock import MagicMock

from LHCbDIRAC.Core.Utilities.RunApplication import RunApplication


def test_lbRunCommand():
  """ Testing lb-run command (for setting the environment)
  """
  ra = RunApplication()
  ra.extraPackages = [('package1', 'v1r0'), ('package2', 'v2r0'), ('package3', '')]
  ra.runTimeProject = 'aRunTimeProject'
  ra.runTimeProjectVersion = 'v1r1'
  ra.opsH = MagicMock()
  ra.opsH.getValue.return_value = ['lcg1', 'lcg2']
  ra.prodConf = True
  extraPackagesString, runtimeProjectString, externalsString = ra._lbRunCommandOptions()
  assert extraPackagesString == ' --use="package1 v1r0"  --use="package2 v2r0"  --use="package3"'
  assert runtimeProjectString == ' --runtime-project aRunTimeProject/v1r1'
  assert externalsString == ' --ext=lcg1 --ext=lcg2'

  ra.site = 'Site1'
  extraPackagesString, runtimeProjectString, externalsString = ra._lbRunCommandOptions()
  assert extraPackagesString == ' --use="package1 v1r0"  --use="package2 v2r0"  --use="package3"'
  assert runtimeProjectString == ' --runtime-project aRunTimeProject/v1r1'
  assert externalsString == ' --ext=lcg1 --ext=lcg2'


def test__gaudirunCommand(mocker):
  """ Testing what is run (the gaudirun command, for example)
  """
  mocker.patch("LHCbDIRAC.Core.Utilities.RunApplication.gConfig.getValue", return_value='LCG.Manchester.uk')

  ra = RunApplication()
  ra.opsH = MagicMock()
  ra.opsH.getValue.return_value = 'gaudirun.py'

  # simplest
  res = str(ra._gaudirunCommand())
  expected = 'gaudirun.py'
  assert res == expected

  # simplest with extra opts
  ra.extraOptionsLine = 'bla bla'
  res = str(ra._gaudirunCommand())
  expected = 'gaudirun.py gaudi_extra_options.py'
  assert res == expected
  with open('gaudi_extra_options.py', 'r') as fd:
    geo = fd.read()
    assert geo == ra.extraOptionsLine

  # productions style /1
  ra.prodConf = True
  ra.extraOptionsLine = ''
  ra.prodConfFileName = 'prodConf.py'
  res = str(ra._gaudirunCommand())
  expected = 'gaudirun.py prodConf.py'
  assert res == expected

  # productions style /2 (multicore)
  ra.optFile = ''
  ra.multicore = True
  res = str(ra._gaudirunCommand())
  assert res == expected  # it won't be allowed on this "CE"

  # productions style /3 (multicore and opts)
  ra.optFile = ''
  ra.extraOptionsLine = 'bla bla'
  res = str(ra._gaudirunCommand())
  expected = 'gaudirun.py prodConf.py gaudi_extra_options.py'
  assert res == expected  # it won't be allowed on this "CE"

  # productions style /4
  ra.extraOptionsLine = ''
  ra.commandOptions = ['$APP/1.py',
                       '$APP/2.py']
  res = str(ra._gaudirunCommand())
  expected = r'gaudirun.py $APP/1.py $APP/2.py prodConf.py'
  assert res == expected
