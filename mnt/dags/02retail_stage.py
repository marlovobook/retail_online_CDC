import os
import logging
import csv
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
from tempfile import NamedTemporaryFile


from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.dummy import DummyOperator
from airflow.providers.common.sql.operators.sql import SQLColumnCheckOperator
from airflow.providers.common.sql.operators.sql import SQLTableCheckOperator

#Create function for loading staged table into postgres

# def _load_data_stage():

#     postgres_hook = PostgresHook(postgres_conn_id='pg_container')
#     conn = postgres_hook.get_conn()
#     cursor = conn.cursor()
    
#     file_name = 'dags/temp/online_retail_stage.csv'

#     #Copy_expery needs to call file_name
#     #STDIN means Standard Input = an input stream where data issent to and read by a program
#     #In this case, it is file_name variable
#     postgres_hook.copy_expert(

#         """
#             COPY 
#                 dbo.table_online_retail_stage(id, Invoice, StockCode, Description,Quantity,InvoiceDate,Price,Customer_ID,Country,last_updated, operation)
            
#             FROM STDIN DELIMITER ',' CSV HEADER

#         """,
#         file_name,

#     )

# def test_data_is_not_null():
#     # quantity isnot null

#     return



default_args = {
    'owner' : 'BOOK',
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    'catchup' : False,
    ######## w8 previous task ##########
    'wait_for_downstream' : True,
    'depends_on_past':True,
    ######## w8 previous task ##########
    'catchup' : False, 
}
# Define your DAG
with DAG(
    dag_id='02_retail_stage',
    default_args=default_args,
    description='Copy online_retail_stage file from local',
    schedule_interval="@daily",  # Set your desired schedule interval '@daily'
    start_date=datetime(2009, 12, 1),  # Set the start date of the DAG
    end_date=datetime(2009, 12, 6) # Set the end date of the DAG

)as dags:
    
    ext_task_sensor = ExternalTaskSensor(
        
        #allowed_states will be ['success'] by default 
        #meaning this task will be success only the targeted task is in success stage
        task_id='check_online_retail_data',
        external_dag_id='01_retail_origin',
        external_task_id='insert_original_data',
        # the worker wil poke to find the successful every 30s and stop working after 1800s
        timeout=1800,
        poke_interval=30,

        # Two mode:
        ## 'poke' = stand by mode while waiting for next poke
        ### 'reschedule' = sensor will terminate inself until the next poke
        mode='reschedule'
    )
    
    """
    Using pythom Assertion
    """
    data_quality_check = DummyOperator(
        task_id="data_quality_check"
        )



    column_check = SQLColumnCheckOperator(

            task_id='SQLColumnCheckOperator_task',
            table='wh.table_online_retail_origin',
            column_mapping={
                "id" : {

                    #count that there are 0 row of null
                    "null_check" : {
                        "equal_to" : 0,
                        
                        #tolerance is a percentage 
                        #that the result may be out of bounds 
                        #but still considered successful.
                        # 0 = 0%; 1 = 100%
                        "tolerance" : 0,    
                    }
                },

                "quantity" : {
                    "null_check" : {
                        "equal_to" : 0,
                    },

                    "min" : {
                        "geq_to" : -1012
                    }
                }

            },

            partition_clause=None,
            conn_id='pg_container',
            database=None,
            accept_none=True,

        )
    
    # from airflow.providers.common.sql.operators.sql import SQLTableCheckOperator
    # SQLTableCheckOperator_task = SQLTableCheckOperator(
    #     task_id='SQLTableCheckOperator_task',
    #     table="wh.table_online_retail_origin",
    #     checks ={
    #         ""
    #     },
    #     partition_clause=None,
    #     conn_id='pg_container',
    #     database=None,
    #     )



    # create_retail_stage = PostgresOperator(
    #     task_id='create_online_retail_stage_in_data_warehouse',
    #     postgres_conn_id="pg_container",
    #     sql=f"""
    #         DROP TABLE IF EXISTS wh.table_online_retail_stage;

    #         CREATE TABLE wh.table_online_retail_stage (
    #             id INT,
    #             Invoice VARCHAR(100),
    #             StockCode VARCHAR(100),
    #             Description VARCHAR(100),
    #             Quantity INT,
    #             InvoiceDate TIMESTAMP (CURRENT_DATE::TIMESTAMP),
    #             Price FLOAT,
    #             Customer_ID VARCHAR(100),
    #             Country VARCHAR(100),
    #             last_updated TIMESTAMP (CURRENT_DATE::TIMESTAMP),
    #             operation char(1),
    #             constraint table_online_retail_stage_pk primary key (id, last_updated)
    #         );

            
        
    #     """,
    # )

    """
    Incremental latest data
    """

    # insert_retail_stage = PostgresOperator(
    #     task_id="insert_retail_stage",
    #     postgres_conn_id="pg_container",
    #     sql=f"""
    #         INSERT INTO wh.table_online_retail_stage (
    #             id,
    #             Invoice,
    #             StockCode,
    #             Description,
    #             Quantity,
    #             InvoiceDate,
    #             Price,
    #             Customer_ID,
    #             Country,
    #             last_updated,
    #             operation
    #         )
    #         SELECT
    #             id,
    #             Invoice,
    #             StockCode,
    #             Description,
    #             Quantity,
    #             InvoiceDate,
    #             Price,
    #             Customer_ID,
    #             Country,
    #             last_updated,
    #             operation
    #         FROM
    #             dbo.table_online_retail_stage

    #         WHERE
    #             InvoiceDate >= '{{{{ds}}}}' AND InvoiceDate < '{{{{next_ds}}}}'
    #     """,
    # )

    #merge the changes from staging table into target table
    #possible to make in incremental load ###ww

    merge_changes_table = PostgresOperator(
        task_id = "merge_chages",
        postgres_conn_id="pg_container",
        sql =f"""

            merge into wh.table_online_retail_origin
            using
            (
                SELECT distinct id,
                    first_value(invoice) over w as invoice, 
                    first_value(stockcode) over w as stockcode, 
                    first_value(description) over w as description, 
                    first_value(quantity) over w as quantity, 
                    first_value(invoicedate) over w as invoicedate, 
                    first_value(price) over w as price, 
                    first_value(customer_id) over w as customer_id, 
                    first_value(country) over w as country, 
                    first_value(last_updated) over w as last_updated, 
                    first_value(operation) over w as operation
                FROM dbo.table_online_retail_stage
                WHERE InvoiceDate >= '{{{{ds}}}}' AND InvoiceDate < '{{{{next_ds}}}}'
                    window w as (partition by id order by last_updated desc)
                order by id

            ) cdc
            on wh.table_online_retail_origin.id=cdc.id
            when not matched and cdc.operation='I' then
                insert values(cdc.id, cdc.invoice, cdc.stockcode, cdc.description, cdc.quantity, cdc.invoicedate, cdc.price, cdc.customer_id, cdc.country, cdc.last_updated, cdc.operation)
            when matched and cdc.operation='D' then
                delete
            when matched and cdc.operation='U' then
                update set  invoice=cdc.invoice,
                            stockcode=cdc.stockcode,
                            description=cdc.description,
                            quantity=cdc.quantity,
                            invoicedate=cdc.invoicedate,
                            price=cdc.price,
                            customer_id=cdc.customer_id,
                            country=cdc.country,
                            last_updated=cdc.last_updated,
                            operation=cdc.operation
            ;

        """
    )

    # load_data = PythonOperator(
    #     task_id="load_data_stage",
    #     python_callable=_load_data_stage,
    # )
   
    # delete_staged_table = PostgresOperator(
    #     task_id="delete_staged_table",
    #     postgres_conn_id="pg_container",
    #     sql="""
    #         DELETE FROM wh.table_online_retail_stage
    #     """,
    # )

    end = DummyOperator(task_id='end')

    

    # Set task dependencies

    ext_task_sensor >> data_quality_check  >> merge_changes_table  >> column_check >> end
    