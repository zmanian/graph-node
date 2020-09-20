LAYOUT:=cargo run -p graph-store-postgres --example layout -- -d subgraphs

all: copy_dds.sql remove_metadata.sql

copy_dds.sql: ../subgraphs.graphql
	$(LAYOUT) -g copy-dds $< > $@
remove_metadata.sql: ../subgraphs.graphql
	$(LAYOUT) -g remove-metadata $< > $@
