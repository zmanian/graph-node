create table removed_deployments(
  id           serial primary key,
  deployment   text primary key,
  removed_at   timestamp default current_timestamp not null,

  schema_name  text not null,
  created_at   int,
  subgraphs    text not null,

  row_count    int,
  entity_count int,
  latest_ethereum_block_number int,

  total_bytes  numeric,
  index_bytes  numeric,
  toast_bytes  numeric,
  table_bytes  numeric
);
