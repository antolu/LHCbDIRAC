#pylint: skip-file

from ProdConf import ProdConf

ProdConf(
  NOfEvents=-1,
  DDDBTag='dddb-20150928',
  CondDBTag='sim-20160321-2-vc-mu100',
  AppVersion='v32r2p1',
  XMLSummaryFile='summaryDaVinci_00033857_00000006_6.xml',
  Application='DaVinci',
  OutputFilePrefix='00033857_00000006',
  XMLFileCatalog='pool_xml_catalog.xml',
  InputFiles=['LFN:00033857_00000005_5.dst'],
  OutputFileTypes=['B2DSTMUNUX_D02K3PI.STRIPTRIG.DST'],
)
