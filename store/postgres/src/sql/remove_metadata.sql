with dds as (
  select *
    from subgraphs.dynamic_ethereum_contract_data_source
   where deployment = $1),
  md0 as (delete from subgraphs.dynamic_ethereum_contract_data_source e
    where e.deployment = $1
    returning e.id),
  md1 as (delete from subgraphs.ethereum_block_handler_entity e
    where left(e.id, 46) = $1 or left(e.id, 40) in (select id from dds)
    returning e.id),
  md2 as (delete from subgraphs.ethereum_block_handler_filter_entity e
    where left(e.id, 46) = $1 or left(e.id, 40) in (select id from dds)
    returning e.id),
  md3 as (delete from subgraphs.ethereum_call_handler_entity e
    where left(e.id, 46) = $1 or left(e.id, 40) in (select id from dds)
    returning e.id),
  md4 as (delete from subgraphs.ethereum_contract_abi e
    where left(e.id, 46) = $1 or left(e.id, 40) in (select id from dds)
    returning e.id),
  md5 as (delete from subgraphs.ethereum_contract_data_source e
    where left(e.id, 46) = $1 or left(e.id, 40) in (select id from dds)
    returning e.id),
  md6 as (delete from subgraphs.ethereum_contract_data_source_template e
    where left(e.id, 46) = $1 or left(e.id, 40) in (select id from dds)
    returning e.id),
  md7 as (delete from subgraphs.ethereum_contract_data_source_template_source e
    where left(e.id, 46) = $1 or left(e.id, 40) in (select id from dds)
    returning e.id),
  md8 as (delete from subgraphs.ethereum_contract_event_handler e
    where left(e.id, 46) = $1 or left(e.id, 40) in (select id from dds)
    returning e.id),
  md9 as (delete from subgraphs.ethereum_contract_mapping e
    where left(e.id, 46) = $1 or left(e.id, 40) in (select id from dds)
    returning e.id),
  md10 as (delete from subgraphs.ethereum_contract_source e
    where left(e.id, 46) = $1 or left(e.id, 40) in (select id from dds)
    returning e.id),
  md11 as (delete from subgraphs.subgraph_deployment e
    where left(e.id, 46) = $1 or left(e.id, 40) in (select id from dds)
    returning e.id),
  md12 as (delete from subgraphs.subgraph_deployment_assignment e
    where left(e.id, 46) = $1 or left(e.id, 40) in (select id from dds)
    returning e.id),
  md13 as (delete from subgraphs.subgraph_deployment_detail e
    where left(e.id, 46) = $1 or left(e.id, 40) in (select id from dds)
    returning e.id),
  md14 as (delete from subgraphs.subgraph_error e
    where left(e.id, 46) = $1 or left(e.id, 40) in (select id from dds)
    returning e.id),
  md15 as (delete from subgraphs.subgraph_manifest e
    where left(e.id, 46) = $1 or left(e.id, 40) in (select id from dds)
    returning e.id)
select sum(rows) as metadata_count from (
  select count(*) as rows from md0
 union all
  select count(*) as rows from md1
 union all
  select count(*) as rows from md2
 union all
  select count(*) as rows from md3
 union all
  select count(*) as rows from md4
 union all
  select count(*) as rows from md5
 union all
  select count(*) as rows from md6
 union all
  select count(*) as rows from md7
 union all
  select count(*) as rows from md8
 union all
  select count(*) as rows from md9
 union all
  select count(*) as rows from md10
 union all
  select count(*) as rows from md11
 union all
  select count(*) as rows from md12
 union all
  select count(*) as rows from md13
 union all
  select count(*) as rows from md14
 union all
  select count(*) as rows from md15
) a
