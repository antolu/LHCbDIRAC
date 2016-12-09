#!/usr/bin/python

import argparse
import errno
import docker
import getpass
import logging
import os
import pprint
import requests
import sys
import tempfile
import urlparse


class LHCbDockerMgmt(object):

  def __init__(self, marathonMasters, version):
    """ c'tor

        :param marathonMasters: list of urls of the masters
    """

    self.marathonMasterList = marathonMasters
    self.marathonMaster = self.__chooseMarathonMaster()
    self.gitRepositoryURL = 'https://gitlab.cern.ch/lhcb-dirac/LHCbDIRAC/raw/'
    self.dockerRegistry = 'gitlab-registry.cern.ch'
    self.dockerRepository = 'gitlab-registry.cern.ch/lhcb-dirac/lhcbdirac'
    self.dockerClient = docker.Client(version='1.23')
    self.version = version

  def __chooseMarathonMaster(self):
    """ Pick among the possible masters one that answers to the ping request

        :return: one working url
    """

    for candidate in self.marathonMasterList:
      logging.debug("Attempting to ping %s" % candidate)
      pingUrl = urlparse.urljoin(candidate, 'ping')
      res = requests.get(pingUrl)
      if res.status_code == 200:
        logging.info("Master server chosen: %s" % candidate)
        return candidate

    raise requests.ConnectionError(
        "None of the candidate is answering to ping")

  def _getRunningDiracComponents(self):
    """ Query Marathon to see the running apps, and keep only
        the dirac components

        :return: list of marathon app json dicts
    """

    fullUrl = urlparse.urljoin(self.marathonMaster, '/v2/apps')
    logging.info("Getting running components at %s" % fullUrl)
    res = requests.get(fullUrl)
    if res.status_code != 200:
      logging.error("Error fetching applications: %s %s" %
                    (res.status_code, res.content))
      return

    apps = res.json()['apps']
    diracApps = []

    for app in apps:
      appId = app['id']
      dockerConfig = app.get('container', {}).get('docker')
      if dockerConfig:
        dockerParams = dockerConfig['parameters']
        for param in dockerParams:
          if param['key'] == 'label' and 'dirac_component=' in param['value']:
            diracApps.append(app)

    return diracApps

  def __putConfiguration(self, config):
    """ Change the application definition in marathon with
        the one passed in parameters

        :param config: list of marathon app json dicts

    """

    fullUrl = urlparse.urljoin(self.marathonMaster, '/v2/apps')

    res = requests.put(fullUrl, json=config)
    if res.status_code == 200:
      logging.info("Successfuly put config")
    else:
      logging.error("Failed to put configuration %s %s" %
                    (res.status_code, res.content))
      raise Exception("Could not put configuration to marathon")

  def softUpdate(self):
    """ Perform a soft update of the LHCbDIRAC version
        by changing nothing to the running configuration
        except the image url
    """

    runningDiracApps = self._getRunningDiracComponents()

    # Update each app configuration
    for app in runningDiracApps:
      # bug in marathon..
      app.pop('uris')
      app.pop('version')

      # change image version
      # http://gitlab:port/repo:oldTag -> http://gitlab:port/repo:newTag
      runningImg = app['container']['docker']['image']
      imgSplit = runningImg.split(':')
      imgSplit[-1] = self.version
      newImg = ':'.join(imgSplit)
      app['container']['docker']['image'] = newImg

    self.__putConfiguration(runningDiracApps)

  def __downloadLHCbDiracDockerFilesLocal(self):
    import shutil
    baseDir = '/afs/cern.ch/user/c/chaen/mesos-dirac/lhcbdiracStructure/lhcbdirac'

    shutil.copyfile(os.path.join(
        baseDir, 'dirac_self_ping.py'), 'dirac_self_ping.py')
    shutil.copyfile(os.path.join(
        baseDir, 'dockerEntrypoint.sh'), 'dockerEntrypoint.sh')

    with open(os.path.join(baseDir, 'Dockerfile.tpl'), 'r') as tpl:
      with open('Dockerfile', 'w') as dockerfile:
        dockerfile.write(''.join(tpl.readlines()).replace(
            '$$LHCB_DIRAC_VERSION$$', self.version))

  def __downloadLHCbDiracDockerFiles(self):
    """ Download in the current directory the files needed to build the docker image.
        This assumes that there is a git tag/branch named after the version. The files
        will be taken from that particular branch/tag
    """

    baseUrl = urlparse.urljoin(self.gitRepositoryURL, os.path.join(
        self.version, 'container/lhcbdirac/'))
    #baseUrl = urlparse.urljoin(self.gitRepositoryURL, os.path.join('mesos', 'container/lhcbdirac/'))

    # Download each file
    for fn in ['dirac_self_ping.py', 'dockerEntrypoint.sh', 'Dockerfile.tpl']:
      fileUrl = urlparse.urljoin(baseUrl, fn)
      res = requests.get(fileUrl)
      if res.status_code != 200:
        logging.error("Could not get %s:%s %s" %
                      (fileUrl, res.status_code, res.content))
        raise Exception("Could not download the LHCbDIRAC docker files")

      content = res.content

      # We need to write the desired version in the Dockerfile
      if fn == 'Dockerfile.tpl':
        fn = 'Dockerfile'
        content = res.content.replace('$$LHCB_DIRAC_VERSION$$', self.version)

      with open(fn, 'w') as f:
        f.write(content)

  def __dockerLogin(self):
    """ Log into the remote registry after prompting the user for username/password

        :return: the credential dict (which contains the password in clear text !!!)
    """

    logging.info("Docker login")
    username = raw_input("Username:")
    password = getpass.getpass()
    return self.dockerClient.login(username, password, registry=self.dockerRegistry)

  def __dockerBuild(self):
    """ Build the Docker image and tag it localy

    """

    logging.info("Building image")
    localTag = 'lhcbdirac:%s' % self.version
    resp = self.dockerClient.build(path='.', tag=localTag)
    for line in resp:
      logging.info(line)
      if 'errorDetail' in line:
        raise Exception("Could not build the image")

  def dockerPublish(self):
    """ Push the local image to the remote registry
        The remote tag will only be the version
    """

    logging.info("Publishing the image to %s" % self.dockerRepository)

    # retag
    tagged = self.dockerClient.tag(
        image='lhcbdirac:%s' % self.version, repository=self.dockerRepository, tag=self.version)
    if not tagged:
      logging.error("Could not tag the %s to the remote repository")
      raise Exception("Could not tag")

    credentials = self.__dockerLogin()
    resp = self.dockerClient.push(
        repository=self.dockerRepository, tag=self.version)
    # For some reasons, resp is sometimes a generator, sometimes a string...
    if isinstance(resp, basestring):
      logging.info(resp)
    else:
      for line in resp:
        if 'progressDetail' in line:
          sys.stdout.write("%s\r" % line)
          sys.stdout.flush()
        else:
          logging.info(line)

  def buildDockerImage(self, publish=False):
    """ Build the LHCbDocker image and give it a local tag
        <lhcbdirac:version>

        :param publish(bool): if set to True, the image is pushed to the registry
    """

    logging.info('Building docker image for version %s' % self.version)
    curDir = os.getcwd()
    try:
      tmpDir = tempfile.mkdtemp()
      logging.info('Working in %s' % tmpDir)
      os.chdir(tmpDir)
      self.__downloadLHCbDiracDockerFiles()
      self.__dockerBuild()

    finally:
      os.chdir(curDir)


if __name__ == '__main__':
  logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

  defaultMarathonMasters = ['http://lbmesosms01.cern.ch:8080',
                            'http://lbmesosms02.cern.ch:8080', 'http://lbmesosms03.cern.ch:8080']
  progDescription = """ Management script for LHCbDIRAC mesos.
                    """
  parser = argparse.ArgumentParser(description=progDescription, add_help=True)
  parser.add_argument('-v', '--version', required=True,
                      help='Version of LHcbDIRAC to work on (e.g. v8r4p5)',
                      dest='version')

  parser.add_argument('-m', '--marathon',
                      help='Url(s) of the marathon master(s) (e.g. http://lbmesosms01.cern.ch:8080). Can take a space separated list',
                      dest='marathonMasters', nargs='+',
                      default=defaultMarathonMasters)

  parser.add_argument('-b', '--build',
                      help='Localy build the Docker image',
                      dest='build', action='store_true')

  parser.add_argument('-d', '--deploy',
                      help='Deploy the version on the Marathon Cluster (!! The image must exist !)',
                      dest='deploy', action='store_true')

  parser.add_argument('-r', '--release',
                      help='Push the version of the image to the central registry. The image must exist localy (see --build)',
                      dest='release', action='store_true')

  parser.add_argument('-a', '--all',
                      help='--build --deploy --release)',
                      dest='doAll', action='store_true')

  args = parser.parse_args()

  if args.doAll and any([args.build, args.release, args.deploy]):
    logging.error("--all incompatible with --build --deploy --release")
    parser.print_help()
    sys.exit(1)

  if not any([args.build, args.release, args.deploy, args.doAll]):
    logging.error("Then what do you want to do ??!!")
    parser.print_help()
    sys.exit(1)

  mgt = LHCbDockerMgmt(args.marathonMasters, args.version)

  if args.build or args.doAll:
    mgt.buildDockerImage()

  if args.release or args.doAll:
    mgt.dockerPublish()

  if args.deploy or args.doAll:
    mgt.softUpdate()
