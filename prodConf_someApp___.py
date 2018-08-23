from ProdConf import ProdConf

ProdConf(
  NOfEvents=100,
  HistogramFile='Hist.root',
  RunNumber=123,
  AppVersion='v1r0',
  XMLSummaryFile='',
  Application='someApp',
  XMLFileCatalog='pool_xml_catalog.xml',
  FirstEventNumber=1,
  InputFiles=['LFN:/for/bar/'],
  OutputFileTypes=['DST', 'GAUSSHIST'],
)