


import logging

import airflow
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta 



args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1),
    'provide_context': True,   
    'retries': 2,
    'retry_delay': timedelta(minutes=7)
}

dag = airflow.DAG(
    'batch_job_example_koalas1',
    schedule_interval='@once',
    default_args=args,
    max_active_runs=1
)

# https://docs.aws.amazon.com/emr/latest/APIReference/API_RunJobFlow.html
default_emr_settings = {"Name": "test koalas",
                        "LogUri": "s3://dendsparktut/logs/", #TODO
                        "ReleaseLabel": "emr-5.30.1",
                        "Instances": {
                            "InstanceGroups": [
                                {
                                    "Name": "Master nodes",
                                    "Market": "ON_DEMAND",
                                    "InstanceRole": "MASTER",
                                    "InstanceType": "m5.xlarge",
                                    "InstanceCount": 1
                                },
                                {
                                    "Name": "Slave nodes",
                                    "Market": "ON_DEMAND",
                                    "InstanceRole": "CORE",
                                    "InstanceType": "m5.xlarge",
                                    "InstanceCount": 1
                                }
                            ],
                            "Ec2KeyName": "emr-key", #TODO
                            "KeepJobFlowAliveWhenNoSteps": True,
                            'EmrManagedMasterSecurityGroup': 'sg-0c56881282db88127', #TODO
                            'EmrManagedSlaveSecurityGroup': 'sg-0eb734a9fee345864', #TODO
                            'Placement': {
                                'AvailabilityZone': 'us-west-2a',
                            },

                        },
                        "BootstrapActions": [
                            {
                                'Name': 'install koalas',
                                'ScriptBootstrapAction': {
                                    'Path': 's3://dendsparktut/emr_bootstrap.sh' #TODO
                                }
                            }
                        ],

                        "Applications": [
                            {"Name": "Spark"}
                        ],
                        "VisibleToAllUsers": True,
                        "JobFlowRole": "EMR_EC2_DefaultRole",
                        "ServiceRole": "EMR_DefaultRole",
                        "Tags": [
                            {
                                "Key": "app",
                                "Value": "analytics"
                            },
                            {
                                "Key": "environment",
                                "Value": "development"
                            }
                        ]
                        }


copy_script_step = [
    {
        'Name': 'setup - copy files',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['aws', 's3', 'cp', 's3://dendsparktut/src/etl.py' , '/home/hadoop/'] #TODO
        }
    }]
    
    
run_job_step = [{
        'Name': 'run koalas',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit', '/home/hadoop/etl.py'] #TODO
        }
    }
    ]


def check_data_exists():
    logging.info('checking that data exists in s3')
    source_s3 = S3Hook(aws_conn_id='aws_default')
    keys = source_s3.list_keys(bucket_name='dendsparktut',#TODO
                               prefix='raw_data/') #TODO
    logging.info('keys {}'.format(keys))


check_data_exists_task = PythonOperator(task_id='check_data_exists',
                                        python_callable=check_data_exists,
                                        provide_context=False,
                                        dag=dag)

create_job_flow_task = EmrCreateJobFlowOperator(
    task_id='create_job_flow',
    aws_conn_id='aws_default',
    emr_conn_id='emr_default',
    job_flow_overrides=default_emr_settings,
    dag=dag
)


copy_python_script = EmrAddStepsOperator(
    task_id='copy_script',
    # XComs let tasks exchange messages
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=copy_script_step,
    dag=dag
)

watch_prev_step_task1 = EmrStepSensor(
    task_id='watch_prev_step1',
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull('copya_script', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=dag
)


run_spark_job = EmrAddStepsOperator(
    task_id='run_spark_job',
    # XComs let tasks exchange messages
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=run_job_step,
    dag=dag
)

watch_prev_step_task2 = EmrStepSensor(
    task_id='watch_prev_step2',
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull('run_spark_job', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=dag
)


terminate_job_flow_task = EmrTerminateJobFlowOperator(
    task_id='terminate_job_flow',
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    aws_conn_id='aws_default',
    trigger_rule="all_done",
    dag=dag
)

check_data_exists_task >> create_job_flow_task  >> copy_python_script >> watch_prev_step_task1 >> run_spark_job >> watch_prev_step_task2  >> terminate_job_flow_task


