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
  NOfEvents=2,
  DDDBTag='Sim08-20130503-1',
  AppVersion='v45r3',
  XMLSummaryFile='summaryGauss_00012345_00067890_1.xml',
  Application='Gauss',
  OutputFilePrefix='00012345_00067890_1',
  RunNumber=2308595,
  XMLFileCatalog='pool_xml_catalog.xml',
  FirstEventNumber=518801,
  CondDBTag='Sim08-20130503-1-vc-mu100',
  OutputFileTypes=['sim'],
)
