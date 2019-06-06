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
#pylint: skip-file
# Used by Test_RunApplication.py (for integration test)

from ProdConf import ProdConf

ProdConf(
  NOfEvents=-1,
  DDDBTag='dddb-20150724',
  CondDBTag='cond-20161011',
  AppVersion='v42r2',
  XMLSummaryFile='summaryDaVinci_0daVinci_000v42r2_1.xml',
  OptionFormat='Stripping',
  Application='DaVinci',
  OutputFilePrefix='0daVinci_000v42r2',
  XMLFileCatalog='pool_xml_catalog.xml',
)
