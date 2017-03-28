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


from airflow.models import BaseOperator, TaskInstance
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
import logging


class SnowflakeSQL(BaseOperator):
    """
    Execute a SQL string or file, supports templating.

    Behavior is mostly inherited from Airflow abstract class BaseOperator, and
    it overrides the execute() method to use Snowflake connector execute_string().

    The sql parameter can be either a SQL string or a path to a SQL script with multiple statements.
    The statements are all executed in a single session, so you can use temporary tables and do
    transaction control. Either the string or the script may include Jinja macros such as execution time,
    which is very handy for selecting time-based data working sets for ETL batches.

    The snowflake_conn_id parameter is optional, defaulting to 'snowflake_default'. See SnowflakeHook on how
    to encode things like warehouse, database, and role as EXTRAs to this connection identifer

    Note that the path to the file is relative to the dags_folder configuration path. For example, if your DAGs
    are in /opt/airflow/dags then your SQL scripts can be in /opt/airflow/dags/sql and you would use the sql parameter
    might look like this sql='sql/dir1/dir2/script.sql'

    Make sure that the _operators dictionary in this directory's __init__.py has a line like the following:

            'snowflake_operator': ['SnowflakeSQL']

    File: contrib/operators/snowflake_operator.py
    Version: Tue Mar 28 18:00:00 UTC 2017
    Author: David Abercrombie, dabercrombie@sharethrough.com
    """
    template_fields = ('sql',)      # To enable Junja templating on this parameter
    template_ext = ('.sql',)        # if parameter ends with .sql, then Airflow uses it as a path relative to dag_folder
    ui_color = '#ededed'

    @apply_defaults
    def __init__(self,
                 sql,
                 snowflake_conn_id='snowflake_default',
                 *args, **kwargs
                 ):
        super(SnowflakeSQL, self).__init__(*args, **kwargs)
        self.sql = sql
        self.snowflake_conn_id=snowflake_conn_id


    def execute(self, context):
        """
        Executes one or more SQL statements passed a string.

        Uses The Snowflake Connection.execute_string that executes one or more SQL statements passed a string.
        This string can contain mutiple statements separated by semi-colons, and can contain newlines and comments.
        By default, this method returns a sequence of Cursor objects in the order of execution.
        The string will have already been processed by the Jinja template engine by the time it gets here.

        See https://docs.snowflake.net/manuals/user-guide/python-connector-api.html#id1
        """
        logging.info('Starting SnowflakeSQL.execute, snowflake_conn_id=' + self.snowflake_conn_id)
        try:
            SnowflakeHook().get_conn(conn_name=self.snowflake_conn_id).execute_string(self.sql)

        finally:
            logging.info('Finished SnowflakeSQL.execute')




