from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrCreateJobFlowOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.models import Variable


# Configurations

EMR_LOGS = Variable.get("BUCKET")  # replace this with your bucket name for EMR logs
BUCKET_NAME = Variable.get("BUCKET")  # bucket with dags
REGION = Variable.get("AWS_REGION")
os.environ['AWS_DEFAULT_REGION'] = Variable.get("AWS_REGION")
SPARK_STEPS = [
    {
        "Name": f"processing-{datetime.now().strftime('%s')}",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                "--conf",
                "spark.executor.memory=10g",
                "--conf",
                "spark.executor.cores=3",
                "--conf",
                "spark.driver.memory=3g",
                "--conf",
                "spark.default.parallelism=90",
                "--conf",
                "spark.yarn.executor.memoryOverhead=1g",
                "--conf",
                "spark.serializer=org.apache.spark.serializer.KryoSerializer",
                "--conf",
                "spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog",
                "--conf",
                "spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
                "--packages",
                "com.amazonaws:aws-java-sdk-bundle:1.12.31,org.opensearch.client:opensearch-hadoop:1.0.1,org.apache.hudi:hudi-spark3.3-bundle_2.12:0.13.1",
                "--py-files",
                "s3://504902519997-ps-leucipa-enrichment-pipeline/main.py",
                "--input",
                "s3://504902519997-ps-leucipa-enrichment-pipeline/",
            ],
        },
    },
    {
        "Name": "View result",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "bash",
                "-c",
                "echo 'Done!'",
            ],
        },
    },
]

JOB_FLOW_OVERRIDES = {
    "Name": "Demo Ephemeral EMR Cluster",
    "ReleaseLabel": "emr-6.11.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
    "BootstrapActions": [
        {
            "Name": "Just an example",
            "ScriptBootstrapAction": {
                "Args": ["Hello World! This is Demo Ephemeral EMR Cluster - EspExpert Enrichment"],
                "Path": "file:/bin/echo",
            },
        }
    ],
    "LogUri": "s3n://{}/emr_logs/".format(EMR_LOGS),
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"},
                }
            ],
        }
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1,
            },
            {
                "Name": "Core - 2",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            },
            {
                "Name": "ON_TASK",
                "Market": "ON_DEMAND",
                "InstanceRole": "TASK",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
        "Ec2SubnetId": Variable.get("EMR_SUBNET_ID"),
    },
    "JobFlowRole": Variable.get("EMR_JOBFLOW_ROLE_GENERATED"),
    "ServiceRole": Variable.get("EMR_SERVICE_ROLE"),
}

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 10, 17),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "enrichment_pipeline_test",
    default_args=default_args,
    max_active_runs=1,
    schedule_interval=None,
    catchup=False,
    tags=["example-tag"],
)

start_data_pipeline = DummyOperator(task_id="start_data_pipeline", dag=dag)

# Create an EMR cluster
create_emr_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_default",
    emr_conn_id="emr_default",
    dag=dag,
    region_name=REGION,
)

# Add your steps to the EMR cluster
step_adder = EmrAddStepsOperator(
    task_id="add_steps",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    steps=SPARK_STEPS,
    dag=dag,
)

last_step = len(SPARK_STEPS) - 1
# wait for the steps to complete
step_checker = EmrStepSensor(
    task_id="watch_step",
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')["
    + str(last_step)
    + "] }}",
    aws_conn_id="aws_default",
    dag=dag,
)

# Terminate the EMR cluster
terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    dag=dag,
)

end_data_pipeline = DummyOperator(task_id="end_data_pipeline", dag=dag)

start_data_pipeline >> create_emr_cluster
create_emr_cluster >> step_adder >> step_checker >> terminate_emr_cluster
terminate_emr_cluster >> end_data_pipeline