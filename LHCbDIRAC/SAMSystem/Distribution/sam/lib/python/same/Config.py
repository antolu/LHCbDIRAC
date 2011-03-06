import ConfigParser
import os

if os.environ.has_key("SAME_HOME"):
  configFilename=os.environ["SAME_HOME"] + "/etc/same.conf"

  config=ConfigParser.ConfigParser()
  config.set('DEFAULT','home',os.environ["HOME"])
  config.set('DEFAULT','same_home',os.environ["SAME_HOME"])
  config.read(configFilename)
  config.read(os.environ["HOME"]+'/.same.conf')
  if os.environ.has_key("SAME_CONFIG"):
    conf=os.environ["SAME_CONFIG"]
    config.read(conf)
else:
  config=None

def load(filename):
  config.read(filename)
  
def save(filename):
  home=config.get('DEFAULT','home')
  same_home=config.get('DEFAULT','same_home')
  config.remove_option('DEFAULT','home')
  config.remove_option('DEFAULT','same_home')
  out=open(filename,'w')
  config.write(out)
  out.close()
  config.set('DEFAULT','home',home)
  config.set('DEFAULT','same_home',same_home)
  
  
