#pylint: skip-file

from ProdConf import ProdConf

ProdConf(
  NOfEvents=1,
  DDDBTag='dddb-20150928',
  AppVersion='v49r5',
  XMLSummaryFile='summaryGauss_00000001_00000001_1.xml',
  Application='Gauss',
  OutputFilePrefix='00000001_00000001_1',
  RunNumber=5732353,
  XMLFileCatalog='pool_xml_catalog.xml',
  FirstEventNumber=4340993,
  CondDBTag='sim-20160321-2-vc-mu100',
  OutputFileTypes=['sim'],
)
