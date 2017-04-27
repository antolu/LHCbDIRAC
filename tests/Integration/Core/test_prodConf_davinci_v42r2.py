#pylint: skip-file
# Used by Test_RunApplication.py (for integration test)

from ProdConf import ProdConf

ProdConf(
  NOfEvents=-1,
  DDDBTag='dddb-20150724',
  CondDBTag='cond-20161011',
  AppVersion='v42r2',
  XMLSummaryFile='summaryDaVinci_0daVinci_000v42r2_1.xml',
  OptionFormat='Stripping',
  Application='DaVinci',
  OutputFilePrefix='0daVinci_000v42r2',
  XMLFileCatalog='pool_xml_catalog.xml',
)
