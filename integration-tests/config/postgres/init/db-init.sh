#!/bin/bash
#
# Copyright 2018-2023 contributors to the Marquez project
# SPDX-License-Identifier: Apache-2.0
#
# Usage: $ ./init-db.sh

set -eu

psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" > /dev/null <<-EOSQL

  CREATE DATABASE meltano;
  CREATE DATABASE prefect;

  CREATE USER ${MARQUEZ_USER};
  ALTER USER ${MARQUEZ_USER} WITH PASSWORD '${MARQUEZ_PASSWORD}';

  CREATE DATABASE ${MARQUEZ_DB};
  GRANT ALL PRIVILEGES ON DATABASE ${MARQUEZ_DB} TO ${MARQUEZ_USER};

  \connect ${MARQUEZ_DB}
  GRANT ALL ON SCHEMA public TO ${MARQUEZ_USER};
  ALTER DATABASE ${MARQUEZ_DB} SET search_path TO public;

EOSQL