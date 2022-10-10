--  Copyright (c) 2020, 2021, Oracle and/or its affiliates.
--
--  This software is dual-licensed to you under the Universal Permissive License
--  (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License
--  2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose
--  either license.
--
--  Licensed under the Apache License, Version 2.0 (the "License");
--  you may not use this file except in compliance with the License.
--  You may obtain a copy of the License at
--
--     https://www.apache.org/licenses/LICENSE-2.0
--
--  Unless required by applicable law or agreed to in writing, software
--  distributed under the License is distributed on an "AS IS" BASIS,
--  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--  See the License for the specific language governing permissions and
--  limitations under the License.

-- This script will start an Oracle Database inside a Docker container and then
-- execute the Oracle R2DBC test suite with a configuration that has it connect
-- to that database.
--
-- This script makes no attempt to clean up. The docker container is left
-- running, and the database retains the test user and any other modifications
-- that the test suite may have performed.
-- It is assumed that the Github Runner will clean up any state this script
-- leaves behind.


-- This script will be run as sysdba connected to the container database. This
-- script will create a test user in the xepdb1 pluggable database. The test
-- user is granted permission to connect to the database, create/query/modify
-- tables, and to query some V$ views:
--   v$open_cursor (to verify if cursors are being closed).
--   v$transaction (to verify if TransactionDefinitions are applied).
--   v$session (to verify if VSESSION_* Options are applied).
ALTER SESSION SET CONTAINER=xepdb1;
CREATE ROLE r2dbc_test_role;
GRANT SELECT ON v_$open_cursor TO r2dbc_test_role;
GRANT SELECT ON v_$transaction TO r2dbc_test_role;
GRANT SELECT ON v_$session TO r2dbc_test_role;
GRANT CREATE VIEW TO r2dbc_test_role;

CREATE USER test IDENTIFIED BY test;
GRANT connect, resource, unlimited tablespace, r2dbc_test_role TO test;
ALTER USER test DEFAULT TABLESPACE users;
ALTER USER test TEMPORARY TABLESPACE temp;
