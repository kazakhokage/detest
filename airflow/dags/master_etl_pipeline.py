from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'yessen',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'master_etl_pipeline',
    default_args=default_args,
    description='main triggering dag',
    schedule_interval='0 */6 * * *',  # Every 6 hours
    catchup=False,
    tags=['etl', 'master', 'orchestration'],
) as dag:
    
    start = DummyOperator(task_id='pipeline_start')
    
    # Stage 1: Operational → MRR
    trigger_operational_to_mrr = TriggerDagRunOperator(
        task_id='trigger_operational_to_mrr',
        trigger_dag_id='etl_operational_to_mrr',
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        execution_date='{{ execution_date }}',
    )
    
    # Stage 2: MRR → STG
    trigger_mrr_to_stg = TriggerDagRunOperator(
        task_id='trigger_mrr_to_stg',
        trigger_dag_id='etl_mrr_to_stg',
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        execution_date='{{ execution_date }}',
    )
    
    # Stage 3: STG → DWH
    trigger_stg_to_dwh = TriggerDagRunOperator(
        task_id='trigger_stg_to_dwh',
        trigger_dag_id='etl_stg_to_dwh',
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
        execution_date='{{ execution_date }}',
    )
    
    end = DummyOperator(task_id='pipeline_complete')
    
    # Define pipeline flow
    start >> trigger_operational_to_mrr >> trigger_mrr_to_stg >> trigger_stg_to_dwh >> end
