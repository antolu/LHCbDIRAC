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
