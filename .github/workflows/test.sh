#!/bin/bash

#  Copyright (c) 2020, 2021, Oracle and/or its affiliates.
#
#  This software is dual-licensed to you under the Universal Permissive License
#  (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License
#  2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose
#  either license.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

# This script will start an Oracle Database inside a Docker container and then
# execute the Oracle R2DBC test suite with a configuration that has it connect
# to that database.
#
# The database version is configured by the first parameter. The version is 
# expressed is as <major>.<minor>.<patch> version number, for example: "18.4.0"
#
# The database port number is configured by the second parameter. If multiple
# databases are created by running this script in parallel, then a unique port
# number should be provided for each database.
#
# This script makes no attempt to clean up. The docker container is left
# running, and the database retains the test user and any other modifications
# that the test suite may have performed.
# It is assumed that the Github Runner will clean up any state this script
# leaves behind.


# The startup directory is mounted as a volume inside the docker container.
# The container's entry point script will execute any .sh and .sql scripts
# it finds under /opt/oracle/scripts/startup. The startup scripts are run
# after the database instance is active.A numeric prefix on the script name
# determines the order in which scripts are run. The final script, prefixed
# with "99_" will create a file named "ready" in the mounted volume, indicating
# that all scripts have completed and the database is ready for testing. 

# Create directory with the startup scripts. Naming the directory with the
# version number should isolate it from database container that is running 
# concurrently, assuming multiple containers are not running the same database
# version.
startUp=$PWD/$1/startup
mkdir -p $startUp
cp $PWD/startup/* $startUp

# Create the 99_ready.sh script. It will touch a file in the mounted startup
# directory.
startUpMount=/opt/oracle/scripts/startup
readyFile=ready
echo "touch -f $startUpMount/$readyFile" > $startUp/99_ready.sh

# The oracle/docker-images repo is cloned. This repo provides Dockerfiles along
# with a handy script to build images of Oracle Database. For now, this script
# is just going to build an Express Edition (XE) image, because this can be 
# done in an automated fashion. Other editions would require a script to accept
# a license agreement.
git clone https://github.com/oracle/docker-images.git
cd docker-images/OracleDatabase/SingleInstance/dockerfiles/
./buildContainerImage.sh -v $1 -x

# Run the image in a detached container
# The startup directory is mounted. It contains a createUser.sql script that
# creates a test user. The docker container will run this script once the
# database has started.
# The database port number, 1521, is mapped to the host system. The Oracle
# R2DBC test suite is configured to connect with this port.
docker run --name test_db --detach --rm -p $2:1521 -v $startUp:$startUpMount oracle/database:$1-xe

# Wait for the database instance to start. The final startup script will create
# a file named "ready" in the startup scripts directory. When that file exists, 
# it means the database is ready for testing.
echo "Waiting for database to start..."
until [ -f $startUp/$readyFile ]
do
  docker logs --since 1s test_db
  sleep 1
done

# Create a configuration file and run the tests. The service name, "xepdb1",
# is always created for the XE database. It would probably change for other 
# database editions. The test user is created by the startup/01_createUser.sql
# script
cd $GITHUB_WORKSPACE
echo "Configuration for testing with Oracle Database $1" > src/test/resources/$1.properties
echo "DATABASE=xepdb1" >> src/test/resources/$1.properties
echo "HOST=localhost" >> src/test/resources/$1.properties
echo "PORT=$2" >> src/test/resources/$1.properties
echo "USER=test" >> src/test/resources/$1.properties
echo "PASSWORD=test" >> src/test/resources/$1.properties
echo "CONNECT_TIMEOUT=30" >> src/test/resources/$1.properties
echo "SQL_TIMEOUT=30" >> src/test/resources/$1.properties
mvn -Doracle.r2dbc.config=$1.properties clean compile test
