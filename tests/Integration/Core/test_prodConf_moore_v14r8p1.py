#pylint: skip-file

from ProdConf import ProdConf

ProdConf(
  NOfEvents=-1,
  DDDBTag='dddb-20150928',
  CondDBTag='sim-20160321-2-vc-mu100',
  AppVersion='v14r8p1',
  XMLSummaryFile='summaryMoore_00033857_00000004_4.xml',
  Application='Moore',
  OutputFilePrefix='00033857_00000004_4',
  XMLFileCatalog='pool_xml_catalog.xml',
  InputFiles=['LFN:00033857_00000003_3.digi'],
  OutputFileTypes=['digi'],
)
