#pylint: skip-file
# Used by Test_RunApplication.py (for integration test)

from ProdConf import ProdConf

ProdConf(
  NOfEvents=-1,
  DDDBTag='dddb-20150724',
  CondDBTag='cond-20161004',
  AppVersion='v52r4',
  XMLSummaryFile='summaryDaVinci_0brunel_000v52r4_1.xml',
  Application='Brunel',
  OutputFilePrefix='0brunel_000v52r4',
  XMLFileCatalog='pool_xml_catalog.xml',
)
