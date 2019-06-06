###############################################################################
# (c) Copyright 2019 CERN for the benefit of the LHCb Collaboration           #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".   #
#                                                                             #
# In applying this licence, CERN does not waive the privileges and immunities #
# granted to it by virtue of its status as an Intergovernmental Organization  #
# or submit itself to any jurisdiction.                                       #
###############################################################################
"""
Set of functions used by the DMS checking scripts
"""

__RCSID__ = "$Id$"

import os
from DIRAC import gLogger
from DIRAC.Core.Utilities.List import breakListIntoChunks

from LHCbDIRAC.DataManagementSystem.Client.ScriptExecutors import removeFiles, removeReplicas


def __removeFile(lfns):
  """
  Use the ScriptExecutors removeFile method
  """
  if isinstance(lfns, basestring):
    lfns = [lfns]
  removeFiles(lfns)


def __removeReplica(lfnDict):
  """
  Use the ScriptExecutors removeReplicas method
  """
  seLFNs = {}
  for lfn in lfnDict:
    for se in lfnDict[lfn]:
      seLFNs.setdefault(se, []).append(lfn)
  for se, lfns in seLFNs.iteritems():
    removeReplicas(lfns, [se])
  return seLFNs


def __replaceReplica(dm, seLFNs):
  """
  Re-replicate replicas that had just been removed because they were bad
  It uses the DataManager instance from the ConsistencyCheck
  """
  if seLFNs:
    gLogger.notice("Now replicating bad replicas...")
    success = {}
    failed = {}
    for se, lfns in seLFNs.iteritems():
      for lfn in lfns:
        res = dm.replicateAndRegister(lfn, se)
        if res['OK']:
          success.update(res['Value']['Successful'])
          failed.update(res['Value']['Failed'])
        else:
          failed[lfn] = res['Message']

    failures = 0
    errors = {}
    for lfn, reason in failed.iteritems():
      reason = str(reason)
      errors[reason] = errors.setdefault(reason, 0) + 1
      failures += 1
    gLogger.notice("\t%d success, %d failures%s" % (len(success), failures, ':' if failures else ''))
    for reason in errors:
      gLogger.notice('\tError %s : %d files' % (reason, errors[reason]))


def _dumpErrorAndFiles(title, lfnList, maxFiles, dumpStr, fileName, fp):
  """
  Print out errors and a (restricted) list of LFNs
  dump the whole list in a file
  """
  gLogger.error(title)
  if not gLogger.info('\n'.join(sorted(lfnList))):
    if len(lfnList) > maxFiles:
      gLogger.error('First %d files:' % maxFiles)
    gLogger.error('\n'.join(sorted(lfnList)[0:maxFiles]))
  if fileName is None:
    return None
  if fp is None:
    fp = open(fileName, 'w')
  dumpStr1 = dumpStr.strip().replace('\n', '')
  if not dumpStr.endswith(' '):
    dumpStr += ' '
  if not dumpStr.startswith('\n'):
    dumpStr = '\n' + dumpStr
  fp.write(dumpStr)
  fp.write(dumpStr.join([''] + sorted(lfnList)))
  gLogger.notice("Whole list available with 'grep %s %s'" % (dumpStr1, fileName))
  return fp


def _getUniqueFileName(name):
  """ Get a file name that doesn't exist given a prefix """
  suffix = ''
  nb = 0
  while True:
    fileName = name + '%s.txt' % suffix
    if not os.path.exists(fileName):
      break
    nb += 1
    suffix = '-%d' % nb
  return fileName


def doCheckFC2SE(cc, bkCheck=True, fixIt=False, replace=False, maxFiles=None, fixOption='FixIt'):
  """
  Method actually calling for the the check using ConsistencyChecks module
  It prints out results and calls corrective actions if required
  """
  cc.checkFC2SE(bkCheck)

  if maxFiles is None:
    maxFiles = 20
  fileName = _getUniqueFileName('CheckFC2SE')
  fp = None
  if cc.existLFNsBKRepNo:
    gLogger.notice('>>>>')
    affectedRuns = set(str(run) for run in cc.existLFNsBKRepNo.itervalues() if run)
    title = "%d files are in the FC but have replica = NO in BK:\nAffected runs: %s" % \
        (len(cc.existLFNsBKRepNo),
         ','.join(sorted(affectedRuns) if affectedRuns else 'None'))
    fp = _dumpErrorAndFiles(title, cc.existLFNsBKRepNo, maxFiles, 'InFCButBKNo', fileName, fp)
    if fixIt:
      gLogger.notice("Going to fix them, setting the replica flag")
      res = cc.bkClient.addFiles(cc.existLFNsBKRepNo.keys())
      if res['OK']:
        gLogger.notice("\tSuccessfully added replica flag")
      else:
        gLogger.error('Failed to set the replica flag', res['Message'])
    else:
      gLogger.notice("Use --%s to fix it (set the replica flag)" % fixOption)
    gLogger.notice('<<<<')

  elif bkCheck:
    gLogger.notice("No files in FC with replica = NO in BK -> OK!")

  if cc.existLFNsNotInBK:
    gLogger.notice('>>>>')

    title = "%d files are in the FC but are NOT in BK" % len(cc.existLFNsNotInBK)
    fp = _dumpErrorAndFiles(title, cc.existLFNsNotInBK, maxFiles, 'InFCButNotInBK ', fileName, fp)
    if fixIt:
      gLogger.notice("Going to fix them, by removing from the FC and storage")
      __removeFile(cc.existLFNsNotInBK)
    else:
      gLogger.notice("Use --%s to fix it (remove from FC and storage)" % fixOption)
    gLogger.notice('<<<<')

  else:
    gLogger.notice("No files in FC not in BK -> OK!")

  seOK = True
  if cc.existLFNsNoSE:
    gLogger.notice('>>>>')

    seOK = False
    title = "%d files are in the BK and FC but are missing at some SEs" % len(cc.existLFNsNoSE)
    fixStr = "removing them from catalogs" if not replace else "re-replicating them"
    fp = _dumpErrorAndFiles(title, cc.existLFNsNoSE, maxFiles, 'InFCButNotInSE ', fileName, fp)
    if fixIt:
      gLogger.notice("Going to fix, " + fixStr)
      removeLfns = []
      replicasToRemove = {}
      for lfn, ses in cc.existLFNsNoSE.iteritems():
        if ses == 'All':
          removeLfns.append(lfn)
        else:
          replicasToRemove.setdefault(lfn, []).extend(ses)
      if removeLfns:
        __removeFile(removeLfns)
      if replicasToRemove:
        seLFNs = __removeReplica(replicasToRemove)
        if replace:
          __replaceReplica(cc.dataManager, seLFNs)
    else:
      if not replace:
        fixStr += "; use --Replace if you want to re-replicate them"
      gLogger.notice("Use --%s to fix it (%s)" % (fixOption, fixStr))
    gLogger.notice('<<<<')

  else:
    gLogger.notice("No missing replicas at sites -> OK!")

  if cc.existLFNsBadFiles or cc.existLFNsNotExisting:
    gLogger.notice('>>>>')

    seOK = False
    if cc.existLFNsBadFiles:
      title = "%d files have a bad checksum" % len(cc.existLFNsBadFiles)
      fp = _dumpErrorAndFiles(title, cc.existLFNsBadFiles, maxFiles, 'BadFiles', fileName, fp)
    if cc.existLFNsNotExisting:
      title = "%d files don't exist at any SE" % len(cc.existLFNsNotExisting)
      fp = _dumpErrorAndFiles(title, cc.existLFNsNotExisting, maxFiles, 'LostFiles', fileName, fp)
    toRemove = sorted(set(cc.existLFNsBadFiles) | set(cc.existLFNsNotExisting))
    if fixIt:
      gLogger.notice("Going to fix them, removing files from catalogs and storage")
      __removeFile(toRemove)
    else:
      gLogger.notice("Use --%s to fix it (remove files from SE and catalogs)" % fixOption)
    gLogger.notice('<<<<')

  if cc.existLFNsBadReplicas:
    gLogger.notice('>>>>')

    seOK = False
    title = "%d replicas have a bad checksum" % len(cc.existLFNsBadReplicas)
    badChecksum = ['%s @ %s' % (lfn, ','.join(sorted(se)))
                   for lfn, se in cc.existLFNsBadReplicas.iteritems()]
    fp = _dumpErrorAndFiles(title, badChecksum, maxFiles, 'BadChecksum', fileName, fp)
    fixStr = "remove replicas from SE and catalogs" if not replace else "re-replicating them"
    if fixIt:
      gLogger.notice("Going to fix, " + fixStr)
      seLFNs = __removeReplica(cc.existLFNsBadReplicas)
      if replace:
        __replaceReplica(cc.dataManager, seLFNs)
    else:
      if not replace:
        fixStr += "; use --Replace if you want to re-replicate them"
      gLogger.notice("Use --%s to fix it (%s)" % (fixOption, fixStr))
    gLogger.notice('<<<<')

  if not cc.existLFNsBadFiles and not cc.existLFNsBadReplicas:
    gLogger.notice("No replicas have a bad checksum -> OK!")
  if seOK:
    gLogger.notice("All files exist and have a correct checksum -> OK!")


def doCheckFC2BK(cc, fixFC=False, fixBK=False, listAffectedRuns=False):
  """
  Method actually calling for the the check using ConsistencyChecks module
  It prints out results and calls corrective actions if required
  """
  cc.checkFC2BK()

  maxFiles = 10
  suffix = ''
  nb = 0
  baseName = 'CheckFC2BK' + ('-%s' % cc.prod if cc.prod else '')
  while True:
    fileName = baseName + '%s.txt' % suffix
    if not os.path.exists(fileName):
      break
    nb += 1
    suffix = '-%d' % nb
  fp = None
  if cc.existLFNsBKRepNo:
    gLogger.notice('>>>>')

    affectedRuns = list(set(str(run) for run in cc.existLFNsBKRepNo.itervalues()))
    gLogger.error("%d files are in the FC but have replica = NO in BK" % len(cc.existLFNsBKRepNo))
    from LHCbDIRAC.DataManagementSystem.Client.ConsistencyChecks import ConsistencyChecks
    ccAux = ConsistencyChecks()
    gLogger.notice("====== Now checking %d files from FC to SE ======" % len(cc.existLFNsBKRepNo))
    ccAux.lfns = cc.existLFNsBKRepNo.keys()
    doCheckFC2SE(ccAux, bkCheck=False, fixIt=fixFC, fixOption='FixFC')
    cc.existLFNsBKRepNo = sorted(set(cc.existLFNsBKRepNo) - set(ccAux.existLFNsNoSE) -
                                 set(ccAux.existLFNsNotExisting) - set(ccAux.existLFNsBadFiles))
    if cc.existLFNsBKRepNo:
      gLogger.notice("====== Completed, %d files are in the FC and SE but have replica = NO in BK ======" %
                     len(cc.existLFNsBKRepNo))
      if fp is None:
        fp = open(fileName, 'w')
      fp.write('\nInFCButBKNo '.join([''] + sorted(cc.existLFNsBKRepNo)))
      res = cc.bkClient.getFileMetadata(cc.existLFNsBKRepNo)
      if not res['OK']:
        gLogger.fatal("Unable to get file metadata", res['Message'])
        return
      if res['Value']['Failed']:
        gLogger.error("No metadata found for some files", '%d files' % len(res['Value']['Failed']))
      success = res['Value']['Successful']
      filesInvisible = set(lfn for lfn, meta in success.iteritems() if meta['VisibilityFlag'][0].upper() == 'N')
      filesVisible = set(success) - filesInvisible
      gLogger.notice('%d files are visible, %d files are invisible' %
                     (len(filesVisible), len(filesInvisible)))
      # Try and print the whole as INFO (in case --Verbose was used).
      #   If nothing printed, print a limited number of files as ERROR
      if not gLogger.info('\n'.join('%s : Visi %s' % (lfn, success.get(lfn, {}).get('VisibilityFlag', '?'))
                                    for lfn in sorted(cc.existLFNsBKRepNo))):
        if len(cc.existLFNsBKRepNo) > maxFiles:
          gLogger.notice('First %d files:' % maxFiles)
        gLogger.error('\n'.join('%s : Visi %s' % (lfn, success.get(lfn, {}).get('VisibilityFlag', '?'))
                                for lfn in sorted(cc.existLFNsBKRepNo)[0:maxFiles]))
      if listAffectedRuns:
        gLogger.notice('Affected runs: %s' % ','.join(affectedRuns))
      gLogger.notice("Full list of files:    grep InFCButBKNo %s" % fileName)
      if fixBK:
        gLogger.notice("Going to fix them, setting the replica flag")
        res = cc.bkClient.addFiles(list(success))
        if res['OK']:
          gLogger.notice("\tSuccessfully added replica flag to %d files" % len(success))
        else:
          gLogger.error('Failed to set the replica flag', res['Message'])
      elif fixFC:
        gLogger.notice("Going to fix them, by removing from the FC and storage")
        __removeFile(success)
      else:
        gLogger.notice("Use --FixBK to fix it (set the replica flag) or --FixFC (for removing from FC and storage)")
    else:
      gLogger.notice("====== Completed, no files in the FC with replica = NO in BK ======")
    gLogger.notice('<<<<')

  else:
    gLogger.notice("No files in FC with replica = NO in BK -> OK!")

  if cc.existLFNsNotInBK:
    gLogger.notice('>>>>')

    gLogger.error("%d files are in the FC but are NOT in BK:" % len(cc.existLFNsNotInBK))
    if fp is None:
      fp = open(fileName, 'w')
    fp.write('\nInFCNotInBK '.join([''] + sorted(cc.existLFNsNotInBK)))
    if not gLogger.info('\n'.join(sorted(cc.existLFNsNotInBK))):
      if len(cc.existLFNsNotInBK) > maxFiles:
        gLogger.notice('First %d files:' % maxFiles)
      gLogger.error('\n'.join(sorted(cc.existLFNsNotInBK[0:maxFiles])))
    gLogger.notice("Full list of files:    grep InFCNotInBK %s" % fileName)
    if fixFC:
      gLogger.notice("Going to fix them, by removing from the FC and storage")
      __removeFile(cc.existLFNsNotInBK)
    else:
      gLogger.notice("Use --FixFC to fix it (remove from FC and storage)")
    gLogger.notice('<<<<')

  else:
    gLogger.notice("No files in FC not in BK -> OK!")
  if fp is not None:
    fp.close()


def doCheckBK2FC(cc, checkAll=False, fixIt=False):
  """
  Method actually calling for the the check using ConsistencyChecks module
  It prints out results and calls corrective actions if required
  """
  cc.checkBK2FC(checkAll)
  maxPrint = 10
  chunkSize = 100
  suffix = ''
  nb = 0
  baseName = 'CheckBK2FC' + ('-%s' % cc.prod if cc.prod else '')
  while True:
    fileName = baseName + '%s.txt' % suffix
    if not os.path.exists(fileName):
      break
    nb += 1
    suffix = '-%d' % nb
  fp = None

  if checkAll:
    if cc.existLFNsBKRepNo:
      gLogger.notice('>>>>')

      nFiles = len(cc.existLFNsBKRepNo)
      comment = "%d files are in the FC but have replica = NO in BK" % nFiles
      if nFiles > maxPrint:
        comment += ' (first %d LFNs) : \n' % maxPrint
      comment += '\n'.join(cc.existLFNsBKRepNo[:maxPrint])
      gLogger.error(comment)
      if fp is None:
        fp = open(fileName, 'w')
      fp.write('\nInFCButBKNo '.join([''] + sorted(cc.existLFNsBKRepNo)))
      gLogger.notice("To get the full list:")
      gLogger.notice("    grep InFCButBKNo %s" % fileName)
      if fixIt:
        gLogger.notice("Setting the replica flag...")
        nFiles = 0
        for lfnChunk in breakListIntoChunks(cc.existLFNsBKRepNo, chunkSize):
          res = cc.bkClient.addFiles(lfnChunk)
          if not res['OK']:
            gLogger.error("Something wrong: %s" % res['Message'])
          else:
            nFiles += len(lfnChunk)
        gLogger.notice("Successfully set replica flag to %d files" % nFiles)
      else:
        gLogger.notice("Use option --FixIt to fix it (set the replica flag)")
      gLogger.notice('<<<<')

    else:
      gLogger.notice("No LFNs exist in the FC but have replicaFlag = No in the BK -> OK!")

  if cc.absentLFNsBKRepYes:
    gLogger.notice('>>>>')

    nFiles = len(cc.absentLFNsBKRepYes)
    comment = "%d files have replicaFlag = Yes but are not in FC:" % nFiles
    if nFiles > maxPrint:
      comment += ' (first %d LFNs) : \n' % maxPrint
    comment += '\n'.join(cc.absentLFNsBKRepYes[:maxPrint])
    gLogger.error(comment)
    if fp is None:
      fp = open(fileName, 'w')
    fp.write('\nInBKButNotInFC '.join([''] + sorted(cc.absentLFNsBKRepYes)))
    gLogger.notice("To get the full list:")
    gLogger.notice("    grep InBKButNotInFC %s" % fileName)
    if fixIt:
      gLogger.notice("Removing the replica flag...")
      nFiles = 0
      for lfnChunk in breakListIntoChunks(cc.absentLFNsBKRepYes, chunkSize):
        res = cc.bkClient.removeFiles(lfnChunk)
        if not res['OK']:
          gLogger.error("Something wrong:", res['Message'])
        else:
          nFiles += len(lfnChunk)
      gLogger.notice("Successfully removed replica flag to %d files" % nFiles)
    else:
      gLogger.notice("Use option --FixIt to fix it (remove the replica flag)")
    gLogger.notice('<<<<')

  else:
    gLogger.notice("No LFNs have replicaFlag = Yes but are not in the FC -> OK!")
  if fp:
    fp.close()


def doCheckSE(cc, seList, fixIt=False):
  cc.checkSE(seList)

  if cc.absentLFNsInFC:
    gLogger.notice('>>>>')
    gLogger.notice('%d files are not in the FC' % len(cc.absentLFNsInFC))
    if fixIt:
      __removeFile(cc.absentLFNsInFC)
    else:
      gLogger.notice("Use --FixIt to fix it (remove from catalogs")
    gLogger.notice('<<<<')

  if cc.existLFNsNoSE:
    gLogger.notice('<<<<')
    gLogger.notice('%d files are not present at %s' % (len(cc.existLFNsNoSE), ', '.join(sorted(seList))))
    gLogger.notice('\n'.join(sorted(cc.existLFNsNoSE)))
  else:
    gLogger.notice('No LFNs missing at %s' % ', '.join(sorted(seList)))
