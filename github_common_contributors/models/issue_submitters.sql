with issue_submitters as (
  select distinct piu.node_id,
    piu.login
  from AIRBYTE_DATABASE.AIRBYTE_SCHEMA.PREFECT_ISSUES_USER as piu
    inner join AIRBYTE_DATABASE.AIRBYTE_SCHEMA.AIRBYTE_ISSUES_USER as aiu ON aiu.node_id = piu.node_id
    inner join AIRBYTE_DATABASE.AIRBYTE_SCHEMA.DBT_ISSUES_USER as diu ON diu.node_id = COALESCE(piu.node_id, aiu.node_id)
)
select *
from issue_submitters