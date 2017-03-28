# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import snowflake.connector
from airflow.hooks.dbapi_hook import DbApiHook
import pandas as pd
import logging

class SnowflakeHook(DbApiHook):
    """
    Hook to communicate with Snowflake

    Make sure there is a line like the following to the _hooks dictionary in __init__.py in this directory

                'snowflake_hook': ['SnowflakeHook'],

    Make sure that airflow/models.py has lines like the following, this will allow the Ad Hoc Query tool
    at /admin/queryview/ to use these connections too.

                elif self.conn_type == 'snowflake':
                return contrib_hooks.SnowflakeHook(snowflake_conn_id=self.conn_id)

    File:  contrib/hooks/snowflake_hook.py
    Version: Tue Mar 28 18:00:00 UTC 2017
    Author: David Abercrombie, dabercrombie@sharethrough.com
    """
    conn_name_attr = 'snowflake_conn_id'
    default_conn_name = 'snowflake_default'
    supports_autocommit = True

    def get_conn(self, conn_name=default_conn_name):
        """
        Get a Snowflake database connection using the Airflow CONNECTION table.

        The Airflow CONNECTION table has columns for typical parameters such as HOST, USER, and PASSWORD,
        but Snowflake uses slightly different parameters. It makes sense to store the Snowflake ACCOUNT
        parameter in the HOST column, but other Snowflake parameters are stored as JSON in the EXTRA column.
        We use Airflow’s extra_dejson.get() to get them.

        Example EXTRA: {"database":"dev_db", "warehouse":"dev_wh", "role":"etl_dev"}
        Example usage: SnowflakeHook().get_conn(conn_name=’etl_dev’)

        :param conn_name: A value of CONNECTION.CONN_ID where CONN_TYPE='snowflake'
        :return: A connection to the Snowflake database.
        """
        logging.info('Connecting to Snowflake using conn_name = ' + conn_name)
        conn = self.get_connection(conn_name)
        conn = snowflake.connector.connect(
            account=conn.host,
            user=conn.login,
            password=conn.password,
            schema=conn.schema,
            database=conn.extra_dejson.get('database'),
            warehouse=conn.extra_dejson.get('warehouse'),
            role=conn.extra_dejson.get('role'),
            region=conn.extra_dejson.get('region'),
            autocommit=conn.extra_dejson.get('autocommit'),
        )
        return conn

    def get_pandas_df(self, sql):
        """
        We need to overide this method in order to use connections other than the default_conn_name
        :param sql: A query input via the web UI at /admin/queryview/
        :return: a Panda data frame
        """
        conn = self.get_conn(self.snowflake_conn_id)
        df = pd.read_sql_query(sql, conn)
        return df

    def get_records(self, sql):
        """Have not tested yet, but based on get_pandas_df testing, we need to pass connection param to get_conn()"""
        conn = self.get_conn(self.snowflake_conn_id)
        result=conn.execute(sql)
        results=result.fetchall()
        return results

