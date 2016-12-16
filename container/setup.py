# This is the setup.py used to package the deployment script
# and fetch the needed dependecy

from setuptools import setup, find_packages
import  os

if os.path.isfile('README.md'):
    long_description = open('README.md').read()
else:
    long_description = '???'

base_dir = os.path.dirname(__file__)
setup(
    name='lhcb-dirac-mesos',
    description='manages the images of LHCbDIRAC used in Mesos',
    version="0.0.1",
    author='Christophe Haen',
    author_email='christophe.haen@cern.ch',
    url='https://gitlab.cern.ch/chaen/LHCbDIRAC',
    license='GPLv3',
    long_description=long_description,
    #packages=find_packages(),
    scripts=[os.path.join(base_dir,'dirac-docker-mgmt.py')],
    install_requires = ['docker==2.0.0'],
)
