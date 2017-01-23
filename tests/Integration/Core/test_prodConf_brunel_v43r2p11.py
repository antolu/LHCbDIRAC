#pylint: skip-file
# Used by Test_RunApplication.py (for integration test)

from ProdConf import ProdConf

ProdConf(
  NOfEvents=-1,
  DDDBTag='dddb-20150928',
  CondDBTag='sim-20160321-2-vc-mu100',
  AppVersion='v43r2p11',
  XMLSummaryFile='summaryBrunel_00033857_00000005_5.xml',
  Application='Brunel',
  OutputFilePrefix='00033857_00000005_5',
  XMLFileCatalog='pool_xml_catalog.xml',
  InputFiles=['LFN:00033857_00000004_4.digi'],
  OutputFileTypes=['dst'],
)
