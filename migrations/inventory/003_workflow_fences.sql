CREATE TABLE inventory.workflow_steps (
    tenant_id text NOT NULL,
    id uuid NOT NULL,
    sku text NOT NULL,
    quantity bigint NOT NULL CHECK(quantity>0 AND quantity<=1000000000),
    closed boolean NOT NULL DEFAULT false,
    PRIMARY KEY(tenant_id,id)
);
