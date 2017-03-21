#pylint: skip-file
# Used by Test_RunApplication.py (for integration test)

from ProdConf import ProdConf

ProdConf(
  NOfEvents=-1,
  DDDBTag='dddb-20150928',
  CondDBTag='sim-20160321-2-vc-mu100',
  AppVersion='v20r4',
  OptionFormat='l0app',
  XMLSummaryFile='summaryMoore_00033857_00000003_3.xml',
  Application='Moore',
  OutputFilePrefix='00033857_00000003_3',
  XMLFileCatalog='pool_xml_catalog.xml',
  InputFiles=['LFN:00033857_00000002_2.digi'],
  OutputFileTypes=['digi'],
)
