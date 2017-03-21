#pylint: skip-file
# Used by Test_RunApplication.py (for integration test)

from ProdConf import ProdConf

ProdConf(
  NOfEvents=-1,
  DDDBTag='dddb-20150928',
  CondDBTag='sim-20160321-2-vc-mu100',
  AppVersion='v30r1',
  XMLSummaryFile='summaryBoole_00033857_00000002_3.xml',
  Application='Boole',
  OutputFilePrefix='00033857_00000002_3',
  XMLFileCatalog='pool_xml_catalog.xml',
  InputFiles=['LFN:00033857_00000001_1.sim'],
  OutputFileTypes=['digi'],
)
