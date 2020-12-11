# salary_increment


import boto3
import json
import sys
import yaml
import datetime
import csv

from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions

def execute(spark, glueContext, args):
    
    properties = {
    "user" : "<username>",
    "password" : "<password>"
    }
    df = spark.read.jdbc(url= "jdbc:postgresql://<rds_host_name>/<database_name>", table="<schema.table>", properties=properties)
   

    
    base_df = base_df.withColumn('employee_id', F.sha2(F.concat(F.col('first_name'), F.col('last_name')), 256)).withColumn('department_id', F.sha2(F.col('dept_name'), 256)).withColumn('updated_salary', (F.col('salary') + (F.col('salary') * (F.col('salary_increment')/100))).cast('integer'))

    emp_df = base_df.select('employee_id' ,'first_name', 'last_name', 'salary', 'department_id')

    dept_df = base_df.select('department_id' ,'dept_name', 'salary_increment')

    updated_salary_df = base_df.select('employee_id', 'updated_salary')

    emp_dy = DynamicFrame.fromDF(emp_df, glueContext, "emp_df")

    dept_dy = DynamicFrame.fromDF(dept_df, glueContext, "dept_df")

    updated_salary_dy = DynamicFrame.fromDF(updated_salary_df, glueContext, "updated_salary_df")
   

    glueContext.write_dynamic_frame.from_jdbc_conf(frame = dept_dy, catalog_connection = "redshift", connection_options = {"dbtable": "legends.department", "database": "dev"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "legend_contact_newrecords_insert")

    glueContext.write_dynamic_frame.from_jdbc_conf(frame = emp_dy, catalog_connection = "redshift", connection_options = {"dbtable": "legends.employee", "database": "dev"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "legend_contact_newrecords_insert")

    glueContext.write_dynamic_frame.from_jdbc_conf(frame = updated_salary_dy, catalog_connection = "redshift", connection_options = {"dbtable": "legends.updated_salary", "database": "dev"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "legend_contact_newrecords_insert")

    
    
def main():

    args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME'])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    execute(
        spark,
        glueContext,
        args
    )


if __name__ == "__main__":
    main()
