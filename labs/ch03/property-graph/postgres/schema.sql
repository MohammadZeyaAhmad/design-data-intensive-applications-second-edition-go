CREATE SCHEMA IF NOT EXISTS graph_lab;
CREATE TABLE IF NOT EXISTS graph_lab.runs(id text PRIMARY KEY);
CREATE TABLE IF NOT EXISTS graph_lab.nodes(
 run text NOT NULL REFERENCES graph_lab.runs ON DELETE CASCADE,tenant text NOT NULL,id text NOT NULL,kind text NOT NULL CHECK(kind IN('warehouse','supplier')),
 properties jsonb NOT NULL CHECK(jsonb_typeof(properties)='object'),PRIMARY KEY(run,tenant,id)
);
CREATE TABLE IF NOT EXISTS graph_lab.edges(
 run text NOT NULL REFERENCES graph_lab.runs ON DELETE CASCADE,tenant text NOT NULL,id text NOT NULL,source text NOT NULL,target text NOT NULL,kind text NOT NULL CHECK(kind IN('route','supplies')),
 properties jsonb NOT NULL CHECK(jsonb_typeof(properties)='object'),
 cost bigint GENERATED ALWAYS AS((properties->>'cost')::bigint) STORED NOT NULL CHECK(cost BETWEEN 1 AND 1000000),
 capacity bigint GENERATED ALWAYS AS((properties->>'capacity')::bigint) STORED NOT NULL CHECK(capacity BETWEEN 0 AND 1000000),
 enabled boolean GENERATED ALWAYS AS((properties->>'enabled')::boolean) STORED NOT NULL,
 PRIMARY KEY(run,tenant,id),FOREIGN KEY(run,tenant,source) REFERENCES graph_lab.nodes ON DELETE CASCADE,FOREIGN KEY(run,tenant,target) REFERENCES graph_lab.nodes ON DELETE CASCADE
);
