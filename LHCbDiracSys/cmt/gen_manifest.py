#!/usr/bin/env python
# encoding: utf-8
'''
Small script to generate the manifest.xml file in InstallArea.
'''
import os
import sys
import logging
import re

from subprocess import Popen, PIPE

logging.basicConfig(level=logging.DEBUG)

# dest = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]),
#                                     os.pardir, os.pardir,
#                                     'InstallArea', os.environ['CMTCONFIG'],
#                                     'manifest.xml'))
# logging.debug('Writing manifest file %s', dest)

dest = sys.argv[1]

# decode project version
m = re.search(r"\$[^$]*/tags.*/(v\d+r\d+(p\d+)?)/[^$]* \$",
              '''$URL: http://svn.cern.ch/guest/dirac/LHCbDirac/trunk/LHCbDiracSys/cmt/gen_manifest.py $''')
if not m:
    version = 'head'
else:
    version = m.group(1)

logging.info('collecting dependencies info')

logging.debug('getting LCG version')
heptools_version = Popen(['cmt', 'show', 'macro_value', 'LCG_config_version'],
                         stdout=PIPE).communicate()[0].strip()
logging.debug('using LCG %s', heptools_version)

# mapping between LCG_Interface name and RPM name for special cases
rpm_names = {'Expat': 'expat'}
fix_rpm_name = lambda n: rpm_names.get(n, n)

# run CMT to get the list of externals
logging.debug('getting used CMT packages')
p = Popen(['cmt', 'show', 'uses'], stdout=PIPE)
externals = [l.split()[1]
             for l in p.stdout
             if l.startswith('use') and
                'LCG_Interfaces' in l and
                'LHCBGRID' not in l]
externals.sort()
logging.debug('detected %d required externals', len(externals))

# get the versions of the externals
def get_ext_vers(ext):
    '''
    Ask CMT the version of an external.
    '''
    logging.debug('getting version of %s', ext)
    vers = Popen(['cmt', 'show', 'macro_value',
                  '%s_native_version' % ext],
                 stdout=PIPE).communicate()[0].strip()
    if not vers:
        logging.warning('cannot find version of %s', ext)
    else:
        logging.debug('using %s %s', ext, vers)
    return vers

externals = [(fix_rpm_name(ext), get_ext_vers(ext)) for ext in externals]

# get the version of required projects
def get_projects_vers(projects):
    '''
    Ask CMT the version of a project.
    '''
    logging.debug('getting version of %s', ', '.join(projects))
    uses = Popen(['cmt', 'show', 'projects'],
                 stdout=PIPE).communicate()[0]
    versions = dict((p.upper(), '') for p in projects)
    for l in uses.splitlines():
        if l.startswith('  ') and not l.startswith('   '):
            p, v, _ = l.split(None, 2)
            if p in versions:
                versions[p] = v.split('_')[-1]

    vers = []
    for p in projects:
        v =  versions[p.upper()]
        if not v:
            logging.warning('cannot find version of %s', p)
        else:
            logging.debug('using %s %s', p, v)
        vers.append(v)
    return vers

dirac_version, lhcbgrid_version = get_projects_vers(['Dirac', 'LHCbGrid'])

destdir = os.path.dirname(dest)
if not os.path.exists(destdir):
    logging.debug('creating directory %s', destdir)
    os.makedirs(destdir)

logging.debug('writing manifest file %s', dest)
xml = open(dest, 'w')
xml.write('''<?xml version="1.0" encoding="UTF-8"?>
<manifest>
  <project name="LHCbDirac" version="{version}"/>
  <heptools>
    <version>{heptools_version}</version>
    <binary_tag>{platform}</binary_tag>
    <packages>
      {packages}
    </packages>
  </heptools>
  <used_projects>
    <project name="LHCbGrid" version="{lhcbgrid_version}" />
    <project name="Dirac" version="{dirac_version}" />
  </used_projects>
</manifest>
'''.format(version=version,
           heptools_version=heptools_version,
           platform=os.environ['CMTCONFIG'],
           packages='\n      '.join(['<package name="%s" version="%s"/>' % ext
                                     for ext in externals]),
           lhcbgrid_version=lhcbgrid_version,
           dirac_version=dirac_version,
          ))
xml.close()
logging.info('created %s', dest)
