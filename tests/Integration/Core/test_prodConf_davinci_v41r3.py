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
  DDDBTag='dddb-20150928',
  CondDBTag='sim-20160321-2-vc-mu100',
  AppVersion='v41r3',
  XMLSummaryFile='summaryDaVinci_00033857_00000007_7.xml',
  OptionFormat='merge',
  Application='DaVinci',
  OutputFilePrefix='00033857_00000007_7',
  XMLFileCatalog='pool_xml_catalog.xml',
  InputFiles=['LFN:00033857_00000006_6.B2DstMuNuX_D02K3Pi.StripTrig.dst'],
  OutputFileTypes=['B2DSTMUNUX_D02K3PI.STRIPTRIG.DST'],
)
