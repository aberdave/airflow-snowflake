# airflow-snowflake

Code to be contributed to the Apache Airflow (incubating) project for ETL workflow management. 

This code provides two interfaces to the Snowflake Data Warehouse in the form of a "hook" and "operator"

For Airflow, see these links:

https://airflow.incubator.apache.org/concepts.html#hooks 
https://airflow.incubator.apache.org/concepts.html#operators

This code uses the Python connector to Snowflake, see https://docs.snowflake.net/manuals/user-guide/python-connector.html

#SnowflakeHook()

    Hook to communicate with Snowflake

    Make sure there is a line like the following to the _hooks dictionary in __init__.py in this directory

                'snowflake_hook': ['SnowflakeHook'],

    Make sure that airflow/models.py has lines like the following, this will allow the Ad Hoc Query tool
    at /admin/queryview/ to use these connections too.

                elif self.conn_type == 'snowflake':
                return contrib_hooks.SnowflakeHook(snowflake_conn_id=self.conn_id)

     The Airflow CONNECTION table has columns for typical parameters such as HOST, USER, and PASSWORD,
     but Snowflake uses slightly different parameters. It makes sense to store the Snowflake ACCOUNT
     parameter in the HOST column, but other Snowflake parameters are stored as JSON in the EXTRA column.
     We use Airflow’s extra_dejson.get() to get them.

     Example EXTRA: {"database":"dev_db", "warehouse":"dev_wh", "role":"etl_dev"}
     Example usage: SnowflakeHook().get_conn(conn_name=’etl_dev’)

# SnowflakeSQL() operator

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


# Limitations

This code has only been tested with Airflow version 1.7.1.3.
This code is not (yet) incorporated into teh Airflow distribution.
