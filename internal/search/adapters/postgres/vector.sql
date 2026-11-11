CREATE EXTENSION IF NOT EXISTS vector VERSION '0.8.1';
CREATE TABLE search.embeddings (
 tenant_id text NOT NULL,id text NOT NULL,model text NOT NULL,model_version text NOT NULL,dimensions integer NOT NULL CHECK(dimensions=8),
 document_version bigint NOT NULL CHECK(document_version>0),version bigint NOT NULL CHECK(version>0),embedding vector(8) NOT NULL,metadata jsonb NOT NULL,
 PRIMARY KEY(tenant_id,id,model,model_version),FOREIGN KEY(tenant_id,id) REFERENCES search.documents
);
CREATE INDEX search_vectors_cosine_hnsw ON search.embeddings USING hnsw(embedding vector_cosine_ops) WITH(m=8,ef_construction=64);
CREATE INDEX search_vectors_l2_hnsw ON search.embeddings USING hnsw(embedding vector_l2_ops) WITH(m=8,ef_construction=64);
CREATE TABLE search.embedding_history(tenant_id text NOT NULL,id text NOT NULL,model text NOT NULL,model_version text NOT NULL,version bigint NOT NULL,body jsonb NOT NULL,metadata jsonb NOT NULL,PRIMARY KEY(tenant_id,id,model,model_version,version));
CREATE TRIGGER immutable_embedding_history BEFORE UPDATE OR DELETE ON search.embedding_history FOR EACH ROW EXECUTE FUNCTION search.immutable();
