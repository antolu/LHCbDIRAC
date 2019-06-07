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
from ProdConf import ProdConf

ProdConf(
  NOfEvents=200,
  DDDBTag='dddb-20120831',
  CondDBTag='sim-20121025-vc-md100',
  AppVersion='v24r0',
  XMLSummaryFile='summaryBoole_00023060_00002595_2.xml',
  Application='Boole',
  OutputFilePrefix='00012345_00067890_2',
  XMLFileCatalog='pool_xml_catalog.xml',
  InputFiles = ['LFN:/lhcb/user/f/fstagni/test/12345/12345678/00012345_00067890_1.sim'],
  OutputFileTypes=['digi'],
)
