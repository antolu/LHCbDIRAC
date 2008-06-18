#!/bin/bash

for dir in /opt/dirac/mysql/db /opt/dirac/mysql/log ; do
  [ -e $dir ] && echo "Existing directory $dir" && echo "Skip MySQL installation" && exit
done

echo -n 'Enter root password:'
read -t 30 -s passwd || exit 1
echo

echo -n 'Enter Dirac password:'
read -t 30 -s diracpwd || exit 1
echo

mkdir -p /opt/dirac/mysql/db
mkdir -p /opt/dirac/mysql/log


mysql_install_db --datadir=/opt/dirac/mysql/db/ 2>&1 > /opt/dirac/mysql/log/mysql_install_db.log
/opt/dirac/pro/mysql/share/mysql/mysql.server start

cd /opt/DIRAC3

for DB in ` find DIRAC/*/DB -name "*DB.sql" -exec basename {} .sql \;` ; do

        echo $DB
        file=DIRAC/*/DB/$DB.sql
        grep -qi "use $DB;" $file || echo ERROR $file
        grep -qi "use $DB;" $file || continue
        mysqladmin create $DB
        echo "GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON $DB.* TO Dirac@localhost IDENTIFIED BY '"$diracpwd"'" | mysql -u root
        echo "GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON $DB.* TO Dirac@'%' IDENTIFIED BY '"$diracpwd"'" | mysql -u root
        awk 'BEGIN { N = 0 }
             { if ( tolower($0) ~ tolower("use "ENVIRON["DB"]";") ) N=1;
               if ( N == 1 ) print }' $file | mysql -u root $DB

done

mysqladmin flush-privileges
mysqladmin -u root password "$passwd"
mysqladmin -u root -h volhcb01.cern.ch password "$passwd"

/opt/dirac/pro/mysql/share/mysql/mysql.server stop
