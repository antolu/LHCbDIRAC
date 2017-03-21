#pylint: skip-file
# Used by Test_RunApplication.py (for integration test)

from ProdConf import ProdConf

ProdConf(
  NOfEvents=1,
  DDDBTag='dddb-20150928',
  AppVersion='v49r5',
  XMLSummaryFile='summaryGauss_00033857_00000001_1.xml',
  Application='Gauss',
  OutputFilePrefix='00033857_00000001_1',
  RunNumber=5732353,
  XMLFileCatalog='pool_xml_catalog.xml',
  FirstEventNumber=4340993,
  CondDBTag='sim-20160321-2-vc-mu100',
  OutputFileTypes=['sim'],
)
