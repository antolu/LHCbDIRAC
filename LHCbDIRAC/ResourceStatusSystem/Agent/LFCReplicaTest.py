# ARCHEOLOGY

# This LFCReplicaTest has to be rewritten/improved to
# work. Historically it has never been used. This piece of commented
# code can serve as an inspiration medium to the person who will maybe
# need one day to write such tests ;-)

# class LFCReplicaTest(object):
#   def __init__(self, path, timeout, fake=False):
#     self.path       = path
#     self.timeout    = timeout
#     self.cfg        = CS.getTypedDictRootedAt(
#       root="", relpath="/Resources/FileCatalogs/LcgFileCatalogCombined")
#     self.ro_mirrors = []

#     if not fake: # If not fake, run it!
#       import lfc
#       from DIRAC.Resources.Catalog.LcgFileCatalogClient import LcgFileCatalogClient
#       self.master_lfc = LcgFileCatalogClient(self.cfg['LcgGfalInfosys'], self.cfg['MasterHost'])
#       self.run_test()
#     else:
#       gLogger.warn("LFCReplicaTest runs in fake mode, nothing is done")

#   def run_test(self):
#   # Load the list of mirrors
#     for site in self.cfg:
#       if type(self.cfg[site]) == dict:
#         self.ro_mirrors.append(self.cfg[site]['ReadOnly'])

#     # For all the mirrors, do the unit test:
#     for mirror in self.ro_mirrors:
#       lfn =  '/lhcb/test/lfc-replication/%s/testFile.%s' % (mirror,time.time())
#       if not self.register_dummy(lfn):
#         gLogger.error("Error: "+lfn+" is already in the master or can't be registered \
# there...check your voms role is prodution \n")
#         continue

#       # Try to open a session
#       if lfc.lfc_startsess(mirror, "DIRAC_test"): # rc != 0 means error
#         continue

#       # Measure time to create replica and write XML file
#       time_to_create = self.time_to_create_rep(lfn)
#       fd = open(self.path + mirror + ".timing", "w")
#       try:
#         fd.write("%s" % time_to_create)
#       finally:
#         fd.close()

#       # Measure time to find a replica
#       if time_to_create == self.timeout:
#         time_to_find = self.timeout
#       else:
#         time_to_find = self.time_to_find_rep(lfn)

#       lfc.lfc_endsess()

#       # Measure time to delete a replica
#       removed = self.remove_replica(lfn)
#       if removed:
#         # Try to open a session
#         if lfc.lfc_startsess(mirror, "DIRAC_test"): # rc != 0 means error
#           continue
#         time_to_remove = self.time_to_remove_rep(lfn)
#         lfc.lfc_endsess()
#         gLogger.always('%s %s %s %s' % (mirror, time_to_create, time_to_find, time_to_remove))

#   @staticmethod
#   def pfn_of_token(SE):
#     cfg = CS.getTypedDictRootedAt(
#       root="",
#       relpath="/Resources/StorageElements/" + SE + "/AccessProtocol.1")
#     return "srm://" + cfg['Host'] + cfg['Path']

#   def register_dummy(self, lfn, size=0, SE="CERN-USER", guid=makeGuid(), chksum=""):
#     pfn = self.pfn_of_token(SE) + lfn
# #    res = self.master_lfc.addFile(lfn, pfn, size, SE, guid, chksum)
#     res = self.master_lfc.addFile(lfn)

#     if not res['OK']:
#       gLogger.info("register_dummy: %s" % res['Message'])
#     return res['OK'] and res['Value']['Successful'].has_key(lfn)

#   def get_replica(self, lfn):
#     reps = {}
#     rc, replica_objs = lfc.lfc_getreplica("/grid" + lfn, "", "")
#     if rc:
#       gLogger.error(lfc.sstrerror(lfc.cvar.serrno))
#     else:
#       for r in replica_objs:
#         SE = r.host
#         pfn = r.sfn.strip()
#       reps[SE] = pfn
#     return reps

#   def remove_replica(self, lfn, SE="CERN-USER"):
#     pfn = self.pfn_of_token(SE) + lfn
#     res = self.master_lfc.removeReplica((lfn, pfn, SE))
#     if res['OK'] == False:
#       gLogger.info("remove_replica: %s" % res['Message'])
#     return res['OK'] and res['Value']['Successful'].has_key(lfn)

#   def remove_file(self, lfn):
#     res = self.master_lfc.removeFile(lfn)
#     return res['OK'] and res['Value']['Successful'].has_key(lfn)

#   def time_to_find_rep(self, lfn):
#     start_time = time.time()
#     while True:
#       reps = self.get_replica(lfn)
#       if reps.has_key('CERN-USER'):
#         return time.time() - start_time
#       else:
#         if (time.time() - start_time < self.timeout) : time.sleep(0.1)
#         else                                         : return self.timeout

#   def time_to_create_rep(self, lfn):
#     start_time = time.time()
#     while True:
#       if lfc.lfc_access("/grid" + lfn, 0) == 0:
#         return time.time() - start_time
#       else:
#         if (time.time() - start_time < self.timeout) : time.sleep(0.1)
#         else                                         : return self.timeout

#   def time_to_remove_rep(self, lfn):
#     start_time = time.time()
#     while not lfc.lfc_access("/grid" + lfn, 0): # rc = 0 if accessible
#       if (time.time() - start_time < self.timeout) : time.sleep(0.1)
#       else                                         : return self.timeout
#     return time.time() - start_time
