with commit_authors as (
  select distinct pca.node_id,
    pca.login
  from AIRBYTE_DATABASE.AIRBYTE_SCHEMA.PREFECT_COMMITS_AUTHOR as pca
    inner join AIRBYTE_DATABASE.AIRBYTE_SCHEMA.AIRBYTE_COMMITS_AUTHOR as aca ON aca.node_id = pca.node_id
    inner join AIRBYTE_DATABASE.AIRBYTE_SCHEMA.DBT_COMMITS_AUTHOR as dca ON dca.node_id = COALESCE(pca.node_id, aca.node_id)
)
select *
from commit_authors