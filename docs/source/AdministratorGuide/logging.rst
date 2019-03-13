==============
LHCbDIRAC Logs
==============

Job logs aka LogSE
==================

The LogSE is used to store the log files from the production jobs. it is defined in `/Operations/defaults/LogStorage/LogSE = LogSE-EOS`.

The content is exposed via the normal EOS protocols, but also via a `CERN web service <http://lhcb-dirac-logse.web.cern.ch/>`_ . Any member of the lhcb-geoc group can `manage this web service <https://webservices.web.cern.ch/>`_. The reason for having this web service is to be able to use htaccess and php magic.

The aim of the php and htaccess magic is to transparently move from a log directory per job to one zip archive per job, and still expose the content the same way, allowing for easy browsing.

The `.htaccess`, `listzip.php` and `extract.php` are stored at the root of the logSE. The php scripts are shared with the Core soft team and available in `this repo <https://gitlab.cern.ch/lhcb-core/LbNightlyTools/tree/master/python/LbNightlyTools/Scripts>`_.

The `.htaccess` content is pasted bellow::

  # Option mandatory for CERN website exploration to work
  Options +Indexes

  # Allows rewrite ruls
  RewriteEngine On

  # this is a clever trick to avoid RewriteBase
  # see http://stackoverflow.com/a/21063276
  # basically, {ENV:BASE} eval to the location of the htaccess
  RewriteCond "%{REQUEST_URI}::$1" "^(.*?/)(.*)::\2$"
  RewriteRule "^(.*)$" "-" [E=BASE:%1]


  # These rules expect path that follows the convention
  # <something>/LOG/<numbers>/<numbers>/<numbers>
  # In practice, what we have is
  # MC/2018/LOG/<prodNumber>/<first digits of the prod number>/<jobNumber>
  # What we want to be able to do is to expose the exact same way the log files of a job:
  # * stored in a directory <jobNumber>
  # * stored in a zip file <jobNumber.zip>, containing itself a <jumbNumber> directory

  # Aim: list the zip file as if it was a directory
  # If the URL targets is a non existing directory
  RewriteCond "%{DOCUMENT_ROOT}/%{REQUEST_URI}" !-d
  # if the url is if the form "<something>/LOG/<int>/<int>/<int>/ (Note the "/: at the end)
  # we redirect to "{ENV:BASE}/listzip.php?zip=<something>/LOG/<int>/<int>/<int>.zip
  RewriteRule "^(.*/LOG/[0-9]+/[0-9]+/([0-9]+))/$" "%{ENV:BASE}listzip.php?zip=$1.zip" [PT,B,L]

  # Aim: extract artifacts from specific zip files
  # If we have a URL targetting a non existing file
  RewriteCond "%{DOCUMENT_ROOT}/%{REQUEST_URI}" !-f
  # if the url is if the form "<something>/LOG/<int>/<int>/<int>/<a path>
  # we redirect to "{ENV:BASE}/extract.php?zip=<something>/LOG/<int>/<int>/<int>.zip&path=<int>/<a path>
  RewriteRule "^(.*/LOG/[0-9]+/[0-9]+/([0-9]+))/(.+)" "%{ENV:BASE}extract.php?zip=$1.zip&path=$2/$3" [PT,B,L]




Centralized logging
===================

-----
TL;DR
-----

All the logs (up to the VERBOSE level) from the Agents and services are visible on `lblogs.cern.ch`, and are stored in the directory `/logdata/dirac`. They are aggregated per components.

------------
More details
------------

Each and every component send their logs at the VERBOSE level in a message queue. This is configured using the `message queue backend <https://dirac.readthedocs.io/en/latest/DeveloperGuide/AddingNewComponents/Utilities/gLogger/Backends/index.html#messagequeuebackend>`_ , and the queue is described in the `MQServices resources <https://dirac.readthedocs.io/en/latest/AdministratorGuide/DIRACSites/MessageQueues/index.html?highlight=MQServices#message-queues>`_

The logs are then consumed by a logstash server, and dumped into the file. This is configured in the `ai-puppet-hostgroup-volhcb repository <https://gitlab.cern.ch/ai/it-puppet-hostgroup-volhcb>`_.
