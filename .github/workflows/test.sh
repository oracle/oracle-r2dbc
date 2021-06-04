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
# with "99_" will create a file named "done" in the mounted volumn. When the
# "done" file exists, this signals that the database is active and that all
# startup scripts have completed.
startUpScripts=$PWD/startup
startUpMount=/opt/oracle/scripts/startup
echo "touch $startUpMount/done" > $startUpScripts/99_done.sh


# The oracle/docker-images repo is cloned. This repo provides Dockerfiles along
# with a handy script to build images of Oracle Database. For now, this script
# is just going to build an 18.4.0 XE image, because this can be done in an
# automated fashion, without having to accept license agreements required by
# newer versions like 19 and 21.
# TODO: Also test with newer database versions
git clone https://github.com/oracle/docker-images.git
cd docker-images/OracleDatabase/SingleInstance/dockerfiles/
./buildContainerImage.sh -v 18.4.0 -x

# Run the image in a detached container
# The startup directory is mounted. It contains a createUser.sql script that
# creates a test user. The docker container will run this script once the
# database has started.
# The database port number, 1521, is mapped to the host system. The Oracle
# R2DBC test suite is configured to connect with this port.
docker run --name test_db --detach --rm -p 1521:1521 -v $startUpScripts:$startUpMount oracle/database:18.4.0-xe

# Wait for the database instance to start. The final startup script will create
# a file named "done" in the startup directory. When that file exists, it means
# the database is ready for testing.
echo "Waiting for database to start..."
until [ -f $startUpScripts/done ]
do
  docker logs --since 3s test_db
  sleep 3
done

# Create a configuration file and run the tests. The service name, "xepdb1",
# is always created for the 18.4.0 XE database, but it would probably change
# for other database versions (TODO). The test user is created by the
# startup/01_createUser.sql script
cd $GITHUB_WORKSPACE
echo "DATABASE=xepdb1" > src/test/resources/config.properties
echo "HOST=localhost" >> src/test/resources/config.properties
echo "PORT=1521" >> src/test/resources/config.properties
echo "USER=test" >> src/test/resources/config.properties
echo "PASSWORD=test" >> src/test/resources/config.properties
echo "CONNECT_TIMEOUT=30" >> src/test/resources/config.properties
echo "SQL_TIMEOUT=30" >> src/test/resources/config.properties
mvn test
