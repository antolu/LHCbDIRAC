from ProdConf import ProdConf

ProdConf(
  NOfEvents=200,
  DDDBTag='dddb-20130929',
  CondDBTag='sim-20130522-vc-md100',
  AppVersion='v26r3',
  XMLSummaryFile='summaryBoole_00023060_00002595_2.xml',
  Application='Boole',
  OutputFilePrefix='00012345_00067890_2',
  XMLFileCatalog='pool_xml_catalog.xml',
  InputFiles = ['LFN:/lhcb/user/f/fstagni/test/12345/12345678/00012345_00067890_1.sim'],
  OutputFileTypes=['digi'],
)
