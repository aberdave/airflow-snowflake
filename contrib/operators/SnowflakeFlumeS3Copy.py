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

# 2017-07-19 16:00:00 -0700

from airflow.models import BaseOperator, TaskInstance
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.hooks import S3Hook
import boto
from boto.s3.key import Key
import logging
from snowflake.connector.util_text import split_statements # for QC_zero_rows
from tempfile import NamedTemporaryFile
import os
from datetime import datetime, timedelta



class SnowflakeFlumeS3Copy(BaseOperator):
    """
    Does a Snowflake COPY INTO from S3 files loaded by Flume. 
    
    Best explained via example.

    It is now shortly after 19:10. We know that files continue to come 
    into the /1800/ bucket for a while after 19:00, typically up to about 
    six minutes late, 19:06. Since the /1800/ folder should be stable by 19:10,
    We start to load the /1800/ dirctory into Snowflake, excluding Flume .tmp
    files. This takes about 3-5 minutes if the ad server logs are in the same
    region as Snowflake, but 10-20 minutes to cross from east to west.
    
    After the /1800/ files are loaded in, we load the non-tmp files from the 
    pervious hour, /1700/, again. Snowflake will load only those non-tmp files, 
    if any, that it missed seeing diring it previous run (paremeter FORCE=FALSE, 
    the default). Usually, with no new files, Snowflake takes just a few seconds 
    to confirm that no new files exist. t will a bit longer if it finds a few.
    
    Now for the Flume .tmp files that we have so far ignored. Here is the deal:
    
    * They are an artifact of Flume's approach to atomic file operations that do
      not work well with S3 (in S3, writes are atomic but renames are not, the
      opposite of normal filesystems).
    
    * If we see a .tmp file in S3, then it has already been fully written to S3
      by Flume. It will never be changing. S3 writes are atomic.
    
    * Sometimes, we get a .tmp file that is a duplicate of a non-tmp file, due to
      Flume's buggy S3 design (a rename is done via copy and delete, the delete
      might fail).
    
    * Non-duplicated .tmp files are OK and necessary to import into Snowflake,
      since these are examlples of where Flume's S3 copy failed).
      HOwever, importing duplicated .tmp files will introduce duplicate data,
      and must be prevented.
    
      By default, Snowflake will not copy a file more than once, even if you 
      list them explicitly withthe FILES parameter (default FORCE=FALSE).
      So it is safe to try to load files twice (e.g. a /1700/ .tmp file sill
      get imported at the 18:10 run, but Snowlake will not re-import it when
      the 19:10 run loks back two hours, fines this file, and includes it in
      the list of files to import). 
    
    Under these assumptions, we can use this approach for our 19:10 load 
    of /1800/ and /1700/ .tmp files:
    
    1. At 19:10, use boto to find the .tmp files in the /1800/ and /1700/ directories.
    2. Hold this list of .tmp files in a Python data structure.
    3. Use boto again to see if any of these .tmp files ahve non-tmp duplicates
    4. Use Python to determine which .tmp files to load, ands construct COPY INTO commands
    5. Execute the COPY INTO(s) to load non-duplicated .tmp files from /1700/ and /1800/

    File: contrib/operators/snowflake_operator.py
    Version: Mon May 10 18:55:35 UTC
    Author: David Abercrombie, dabercrombie@sharethrough.com
    """
    template_fields = ('source_hour',)  # To enable Jinja templating on this parameter
    template_ext = ('.sql',)  # irrelevant here
    ui_color = '#ededed'

    @apply_defaults
    def __init__(self,
                 source_stage='REQUIRED_BUT_THERE_IS_NO_DEFAULT_STAGE',
                 source_hour='REQUIRED_BUT_THERE_IS_NO_DEFAULT_SOURCE_HOUR',
                 source_region_keys=['REQUIRED_BUT_THERE_ARE_NO_DEFAULT_REGION_KEYS'],
                 source_base_path_key='',
                 source_pattern=".*[0-9]$",
                 destination_table='REQUIRED_BUT_THERE_IS_NO_DEFAULT_DESTINATION_TABLE',
                 hour_key_format='%Y-%m-%d/%H%M',
                 file_format='(TYPE=JSON)',
                 snowflake_conn_id='snowflake_default',
                 s3_conn_id='s3_default',
                 *args, **kwargs
                 ):
        super(SnowflakeFlumeS3Copy, self).__init__(*args, **kwargs)
        self.source_stage = source_stage
        self.source_hour = source_hour
        self.source_region_keys = source_region_keys
        self.source_base_path_key = source_base_path_key
        self.source_pattern = source_pattern
        self.hour_key_format = hour_key_format
        self.destination_table = destination_table
        self.file_format = file_format
        self.snowflake_conn_id = snowflake_conn_id
        self.s3_conn_id = s3_conn_id


    def generate_full_key_prefix(self, hour_str, region_key):
        # generate stage and path string for Snowflake FROM clause, depending on S3 key prefix conventions
        if self.source_base_path_key == '':
            full_key_prefix = region_key + "/" + hour_str
        else:
            full_key_prefix = self.source_base_path_key + "/" + region_key + "/" + hour_str

        return full_key_prefix


    def generate_copy_epoch_files_sql (self, hour_str, region_key, etl_batch_tag):
        """generate a Snowflake COPY INTO SQL command. Specify pattern for extension, etc. Note our S3 key convention"""

        # generate stage and path string for Snowflake FROM clause
        full_key_prefix = self.generate_full_key_prefix(hour_str, region_key)

        copy_sql_string = (
            "COPY INTO " + self.destination_table + " (src, etl_batch_tag) "
            "FROM (select s.*, '" + etl_batch_tag + "' as etl_batch_tag "
            "      from @" + self.source_stage + '/' + full_key_prefix + " s) "
            "FILE_FORMAT = " + self.file_format + " "
            "FORCE=FALSE "
            "PATTERN = '" + self.source_pattern + "' "
            "ON_ERROR = CONTINUE "
        )
        return copy_sql_string


    def generate_copy_tmp_files_sql (self, hour_str, region_key, etl_batch_tag, s3_bucket_name, boto_s3_connection):
        """generate a Snowflake COPY INTO SQL command. Avoids duplicate .tmp Flume files. Note our S3 key convention"""

        # generate stage and path string for Snowflake FROM clause
        full_key_prefix = self.generate_full_key_prefix(hour_str, region_key)

        # boto must have this trailing slash, unlike Snowflake
        full_key_prefix_with_slash = full_key_prefix + "/"
        boto_s3_bucket = boto_s3_connection.get_bucket(s3_bucket_name)

        """ Step 1: Use boto to make a list of all *.tmp files in S3 (about a few dozen per ETL run)
            Step 2: Use string processing to strip the .tmp from the end of each filename, put into a new list.
                    These would be the duplicate non-tmp files, and already imported, if they exist.
            Step 3: Use boto to check whether each non-tmp (potential duplicate) actually exists. 
                    If it does NOT exist, then there is no duplicate, so the .tmp version needs importing.
                    If it does exist, then there is a duplicate, so the .tmp must not be imported.
            Step 4: Cast the Python list to a string, anc convert its brackets to pasrethesis to make a SQL list
            Step 5: Assemble the SQL string used to import non-duplicated .tmp files into Snowflake.
        """

        logging.info('getting list of all .tmp files in s3://' + s3_bucket_name + "/" + full_key_prefix_with_slash)
        potential_tmp_file_list = []
        for k in boto_s3_bucket.list(prefix=full_key_prefix_with_slash):
            if k.name.find('.tmp', len(k.name) - 4) != -1:
                potential_tmp_file_list.append(k.name)

        non_tmp_file_version_list = []
        for potential_tmp_file in potential_tmp_file_list:
            non_tmp_name = potential_tmp_file[0:len(potential_tmp_file) - 4]
            non_tmp_file_version_list.append(non_tmp_name)

        logging.info('finding.tmp files without epoch in s3://' + s3_bucket_name + "/" + full_key_prefix_with_slash)
        actual_tmp_file_list = []
        for non_tmp_file_version in non_tmp_file_version_list:
            k2 = Key(boto_s3_bucket, name=non_tmp_file_version)
            if not k2.exists():
                actual_tmp_file_list.append(non_tmp_file_version + '.tmp')

        actual_tmp_file_string = str(actual_tmp_file_list)
        actual_tmp_file_string = actual_tmp_file_string.replace('[', '(')
        actual_tmp_file_string = actual_tmp_file_string.replace(']', ')')

        copy_sql_string = (
            "COPY INTO " + self.destination_table + " (src, etl_batch_tag) "
            "FROM (select s.*, '" + etl_batch_tag + "' as etl_batch_tag "
            "         from @" + self.source_stage + " s) "
            "FILES = " + actual_tmp_file_string + " "
            "FILE_FORMAT = " + self.file_format + " "
            "FORCE=FALSE "
            "ON_ERROR = CONTINUE"
        )
        return copy_sql_string


    def execute(self, context):
        """
        Loads hourly data from S3 into Snowflake, where the hourly data comes from Flume transport of logs.
        
        Flume has a number of quirks, this operator deals only with the first item:
         
               1) it keeps loading files into an hour-named key prefix for about six minutes after the hour has ended, 
               2) it leaves *.tmp files around and tehse need to loaded along with the normal files that end in 
                  epoch millisecond numerals such as *.1493665203276 for 2017-05-01 19:00:03.276
        EXCEPT 3) *.tmp files with the same basename as a epoch millisecond file are duplicates that must be ignored

        Airflow generates a variable called 'ts' that is the ETL execution date expressed
        as a string like '2017-05-01T19:00:00', and this string gets into a task using
        Airflow's Jinja templating like this:
        
             source_hour='{{ ts }}'
        
        We need to convert this 'ts' string to a Python naive datetime object for two reasons:
        
         1) so that we can convert it to an S3 key prefix used by Flume: .../2017-05-01/1900/...
         2) so that we can subtract an hour an look for files that were missed in the previous hour.
        
        By the way, we can use the raw 'ts' string as our ETL_BATCH_TAG, as is our convention
        """

        """Basic date string processing and diagnostic logging of input parameters"""

        logging.info('Starting SnowflakeFlumeS3Copy.execute(), snowflake_conn_id=' + self.snowflake_conn_id)
        logging.info('source_stage=' + self.source_stage)
        logging.info('source_region_keys=' + str(self.source_region_keys))
        logging.info('source_base_path_key=' + self.source_base_path_key)
        logging.info('destination_table=' + self.destination_table)

        source_hour_dt = datetime.strptime(self.source_hour,'%Y-%m-%dT%H:%M:%S')
        source_hour_str = datetime.strftime(source_hour_dt,self.hour_key_format)

        previous_hour_dt = source_hour_dt - timedelta(hours=1)
        previous_hour_str = datetime.strftime(previous_hour_dt,self.hour_key_format)

        etl_batch_tag = self.source_hour

        logging.info('source_hour_str=' + '/' + source_hour_str + '/')
        logging.info('previous_hour_str=' + '/' + previous_hour_str + '/')
        logging.info('etl_batch_tag=' + etl_batch_tag)

        logging.info("Generating SQL for regular, UNIX epoch style log files")
        # initialize empty list of SQL commands that we will append statements. We will first build this list,
        # then iterate through it with a Snowflake connection and cursor.
        #
        copy_epoch_files_sql_list = []

        # Use local class method self.generate_copy_epoch_files_sql() to make SQL for this hour for all region keys
        #
        for region_key in self.source_region_keys:
            copy_epoch_files_sql = self.generate_copy_epoch_files_sql(
                hour_str=source_hour_str,
                region_key=region_key,
                etl_batch_tag=etl_batch_tag)
            copy_epoch_files_sql_list.append(copy_epoch_files_sql)

        # Repeat generating commands for the previous hour (for all region keys)
        for region_key in self.source_region_keys:
            copy_epoch_files_sql = self.generate_copy_epoch_files_sql(
                hour_str=previous_hour_str,
                region_key=region_key,
                etl_batch_tag=etl_batch_tag)
            copy_epoch_files_sql_list.append(copy_epoch_files_sql)


        try:
            logging.info("Getting Snowflake connection")
            snowflake_connection = SnowflakeHook().get_conn(conn_name=self.snowflake_conn_id)
            snowflake_cursor = snowflake_connection.cursor()

            logging.info("Getting boto connection to S3")
            boto_s3_hook = S3Hook(s3_conn_id=self.s3_conn_id)
            boto_s3_connection = boto_s3_hook.get_conn()

            logging.info("Asking Snowflake for staging S3 URL from information_schema.stages")
            get_s3_bucketname_from_stage_sql = (
                "select stage_url, stage_region "
                "from information_schema.stages "
                "where stage_name = upper('" + self.source_stage + "') "
            )
            s3_bucket_name, s3_bucket_region = snowflake_cursor.execute(get_s3_bucketname_from_stage_sql).fetchone()

            # strip off the leading 's3://' that coms from Sbowflake since boto does not like it
            s3_bucket_name = s3_bucket_name[5:]
            s3_bucket_name = s3_bucket_name.replace('/','')

            logging.info('s3_bucket_name=' + s3_bucket_name)
            logging.info('s3_bucket_region=' + s3_bucket_region)

            """Make SQL for .tmp files. The goal here is to not load a .tmp file that is a duplicate of a normal file"""

            logging.info("Generating SQL for regular, UNIX epoch style log files, neds boto to do bucket listings")
            # initialize empty list of SQL commands that we will append statements. We will first build this list,
            # then iterate through it with a Snowflake connection and cursor.
            #
            copy_tmp_files_sql_list = []

            # Use local class method self.generate_copy_tmp_files_sql() to make SQL for this hour for all region keys
            #
            for region_key in self.source_region_keys:
                copy_tmp_files_sql = self.generate_copy_tmp_files_sql(
                    hour_str=source_hour_str,
                    region_key=region_key,
                    etl_batch_tag=etl_batch_tag,
                    s3_bucket_name=s3_bucket_name,
                    boto_s3_connection=boto_s3_connection
                )
                copy_tmp_files_sql_list.append(copy_tmp_files_sql)

            # Repeat generating commands for the previous hour (for all region keys)
            for region_key in self.source_region_keys:
                copy_tmp_files_sql = self.generate_copy_tmp_files_sql(
                    hour_str=previous_hour_str,
                    region_key=region_key,
                    etl_batch_tag=etl_batch_tag,
                    s3_bucket_name=s3_bucket_name,
                    boto_s3_connection=boto_s3_connection
                )
                copy_tmp_files_sql_list.append(copy_tmp_files_sql)


            logging.info('Loading regular epoch files #######################################################')
            for cmd in copy_epoch_files_sql_list:
                # logging.info(cmd)
                snowflake_cursor.execute(cmd)

            logging.info('Loading Flume .tmp files ###########################################################')
            for cmd in copy_tmp_files_sql_list:
                # logging.info(cmd)
                snowflake_cursor.execute(cmd)

        finally:
            # cleanup
            snowflake_cursor.close()
            snowflake_connection.close()
            boto_s3_connection.close()
            logging.info('Finished SnowflakeFlumeS3Copy.execute()')


