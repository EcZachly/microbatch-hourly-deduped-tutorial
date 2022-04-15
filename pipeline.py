from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.contrib.operators.spark_sql_operator import SparkSQLOperator
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
default_args = {
    'owner': 'Zach Wilson',
    'depends_on_past': False,
    'start_date': datetime(2020, 10, 10),
    'email': [''],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}
dag = DAG(dag_id='hourly_dedupe_as_daily_micro_batch',
          default_args=default_args,
          schedule_interval="@daily")

hours = list(range(1, 24))
products = ['website', 'app']
all_merges = [
        '1_2': ['1', '2'],
        '3_4': ['3', '4'],
        '5_6': ['5', '6'],
        '7_8': ['7','8'],
        '9_10': ['9', '10'],
        '11_12': ['11', '12'],
        '13_14': ['13', '14'],
        '15_16': ['15', '16'],
        '17_18': ['17', '18'],
        '19_20': ['19', '20'],
        '21_22': ['21', '22'],
        '23_24': ['23', '24'],
        '1_4': ['1_2', '3_4'],
        '5_8': ['5_6', '7_8'],
        '9_12': ['9_10', '11_12'],
        '13_16': ['13_14', '15_16'],
        '17_20': ['17_18', '19_20'],
        '21_24': ['21_22', '23_24'],
        '1_8': ['1_4', '5_8'],
        '9_16': ['9_12', '13_16'],
        '17_24': ['17_20', '21_24'],
        '1_16': ['1_8', '9_16'],
        '1_24': ['1_16', '17_24']
]

merge_tasks = {}
wait_for_tasks = {}

for product_name in products:
    product_str = f'product_name={product_name}'
    for hour in hours:
        hour_str = f'hour={str(hour)}'
        ds_str = "ds='{{ ds }}'"
        task_key = f'merge_{product_name}_{hour}'
        partition = '/'.join([ds_str, hour_str , product_str])
        wait_for_tasks[f'wait_for_{product_name}_{hour}'] = HivePartitionSensor(table='event_source', partition=partition)
        insert_spec = f"INSERT OVERWRITE TABLE hourly_deduped_source PARTITION ({ds_str}, {hour_str}, {product_str})"
        merge_tasks[task_key] = SparkSQLOperator(
                                         task_id=task_key,
                                         conn_id='spark_local',
                                         query=f"""
                                             {insert_spec}
                                             SELECT
                                                product_id,
                                                event_type,
                                                MIN(event_timestamp_epoch) as min_event_timestamp_epoch,
                                                MAX(event_timestamp_epoch) AS max_event_timestamp_epoch,
                                                MAP_FROM_ARRAYS(
                                                    COLLECT_LIST(event_location),
                                                    COLLECT_LIST(event_timestamp_epoch)
                                                ) AS event_locations
                                             FROM event_source
                                             GROUP BY product_id, event_type
                                         """,
                                         name=task_key,
                                         dag=dag
                                         )
        merge_tasks[task_key].set_upstream([wait_for_tasks[f'wait_for_{product_name}_{hour}']])



    for merge_id, merge_dependencies in all_merges.items():
        product_str = f'product_name={product_name}'
        earlier_hour = merge_dependencies[0]
        later_hour = merge_dependencies[1]
        earlier_task = merge_tasks[f'merge_{product_name}_{earlier_hour}']
        later_task = merge_tasks[f'merge_{product_name}_{later_hour}']
        merge_hour_str = f'hour={merge_id}'
        task_key = f'merge_{product_name}_{merge_id}'
        ds_str = "ds='{{ ds }}'"
        insert_spec = f"INSERT OVERWRITE TABLE hourly_deduped_source PARTITION ({ds_str}, {merge_hour_str}, {product_str})"

        # On the final merge, we want to write to the table that doesn't have the hour partition
        if merge_id == '1-24':
            insert_spec =  f"INSERT OVERWRITE TABLE deduped_source PARTITION ({ds_str}, {product_str})"

        merge_tasks[task_key] = SparkSQLOperator(
                         task_id=task_key,
                         conn_id='spark_local',
                         query=f"""
                             {insert_spec}
                             WITH earlier AS (
                                SELECT * FROM hourly_deduped_source
                                WHERE {ds_str} AND hour = {earlier_hour} AND product_name = {product_name}
                             ),
                             later AS (
                               SELECT * FROM hourly_deduped_source
                               WHERE {ds_str} AND hour = {later_hour} AND product_name = {product_name}
                             )
                             SELECT
                                COALESCE(e.product_id, l.product_id) as product_id,
                                COALESCE(e.event_type, l.event_type) AS event_type,
                                COALESCE(e.min_event_timestamp_epoch, l.min_event_timestamp_epoch) as min_event_timestamp_epoch,
                                COALESCE(l.max_event_timestamp_epoch, e.max_event_timestamp_epoch) AS max_event_timestamp_epoch,
                                CONCAT(e.event_locations, l.event_locations) as event_locations
                             FROM earlier e
                                FULL OUTER JOIN later l
                                    ON e.product_id = l.product_id
                                    AND e.event_type = l.event_type
                         """,
                         name=task_key,
                         dag=dag
                     )
        merge_task.set_upstream(earlier_task, later_task)

    


