#!/bin/bash

echo "PREPARING MYSQL DATABSE"

mysql -u root -p$MYSQL_ROOT_PASSWORD < /init/init.sql

echo "MYSQL SETUP COMPLETE"

exit 0
