from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from datetime import datetime

project_config = ProjectConfig(dbt_project_path='/opt/airflow/dbt_crypto')
profile_config = ProfileConfig(
    profile_name='dbt_crypto',
    profiles_yml_filepath='/opt/airflow/dbt_profiles/profiles.yml',
    target_name='airflow_container'
)
exectution_config = ExecutionConfig(dbt_executable_path='/opt/airflow/dbt_venv/bin/dbt')


basic_cosmos_dag = DbtDag(
    # dbt/cosmos-specific parameters
    project_config=project_config,
    profile_config=profile_config,
    execution_config=exectution_config,
    operator_args={
        "full_refresh": True,  # used only in dbt commands that support this flag
    },
    # normal dag parameters
    schedule="@daily",
    start_date=datetime(2026, 8, 11),
    catchup=False,
    dag_id="dbt_updater_dag",
    default_args={"retries": 2},
)
