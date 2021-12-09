from prefect import Flow, task, Parameter, artifacts
from prefect.tasks.airbyte.airbyte import AirbyteConnectionTask
from prefect.tasks.dbt.dbt import DbtShellTask
from prefect.tasks.snowflake.snowflake import SnowflakeQuery
from prefect.tasks.secrets.base import PrefectSecret
from datetime import timedelta

sync_airbyte_connection = AirbyteConnectionTask(
    max_retries=3, retry_delay=timedelta(seconds=10)
)
run_dbt = DbtShellTask(
    command="dbt run",
    environment="dev",
    profile_name="github_common_contributors",
    helper_script="cd github_common_contributors",
    set_profiles_envar=False,
    max_retries=3,
    retry_delay=timedelta(seconds=10),
)
query_snowflake = SnowflakeQuery(
    user="AIRBYTE_USER",
    database="AIRBYTE_DATABASE",
    schema="AIRBYTE_SCHEMA",
    role="AIRBYTE_ROLE",
    warehouse="AIRBYTE_WAREHOUSE",
    max_retries=3,
    retry_delay=timedelta(seconds=10),
)


@task
def generate_result_markdown(common_committers, common_issue_submitters):
    markdown_lines = []
    markdown_lines.append("# Committers common between Prefect, Airbyte, and dbt")
    for committer in common_committers:
        markdown_lines.append(f"{committer[0]}")

    markdown_lines.append("# Issue submitters common between Prefect, Airbyte and dbt")
    for issue_submitter in common_issue_submitters:
        markdown_lines.append(f"{issue_submitter[0]}")
    artifacts.create_markdown("\n".join(markdown_lines))


with Flow("Determine common contributors flow") as flow:
    # Airbyte connection strings
    airbyte_github_connection_id = Parameter("AIRBYTE_GITHUB_CONNECTION_ID")
    dbt_github_connection_id = Parameter("DBT_GITHUB_CONNECTION_ID")
    prefect_github_connection_id = Parameter("PREFECT_GITHUB_CONNECTION_ID")

    # Snowflake configuration
    snowflake_account = Parameter("SNOWFLAKE_ACCOUNT")
    snowflake_password = PrefectSecret("SNOWFLAKE_PASSWORD")

    # Sync Airbyte GitHub data
    airbyte_github_sync = sync_airbyte_connection(
        airbyte_server_host="localhost",
        airbyte_server_port=8000,
        airbyte_api_version="v1",
        connection_id=airbyte_github_connection_id,
    )

    # Sync dbt GitHub data
    dbt_github_sync = sync_airbyte_connection(
        connection_id=dbt_github_connection_id,
        airbyte_server_host="localhost",
        airbyte_server_port=8000,
        airbyte_api_version="v1",
    )

    # Sync Prefect GitHub data
    prefect_github_sync = sync_airbyte_connection(
        airbyte_server_host="localhost",
        airbyte_server_port=8000,
        airbyte_api_version="v1",
        connection_id=prefect_github_connection_id,
    )

    # Run dbt to create views
    dbt_run = run_dbt(
        upstream_tasks=[airbyte_github_sync, dbt_github_sync, prefect_github_sync]
    )

    # Query common committers from Snowflake
    common_committers = query_snowflake(
        account=snowflake_account,
        password=snowflake_password,
        upstream_tasks=[dbt_run],
        query="""
        select login from commit_authors
        """,
    )

    # Query common issue submitters from Snowflake
    common_issue_submitters = query_snowflake(
        account=snowflake_account,
        password=snowflake_password,
        upstream_tasks=[dbt_run],
        query="""
        select login from issue_submitters
        """,
    )
    generate_result_markdown(common_committers, common_issue_submitters)
