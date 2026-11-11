-- name: GetProductDetails :one
SELECT d.sku,d.brand_id,b.name AS brand_name,d.description,d.attributes,d.version,
 ARRAY(SELECT c.category FROM catalog.product_categories c WHERE c.tenant_id=d.tenant_id AND c.sku=d.sku ORDER BY c.category)::text[] AS categories,
 COALESCE((SELECT jsonb_agg(jsonb_build_object('code',v.code,'label',v.label) ORDER BY v.position) FROM catalog.product_variants v WHERE v.tenant_id=d.tenant_id AND v.sku=d.sku),'[]'::jsonb)::jsonb AS variants
FROM catalog.product_details d JOIN catalog.brands b ON b.tenant_id=d.tenant_id AND b.id=d.brand_id
WHERE d.tenant_id=$1 AND d.sku=$2;

-- name: ListProductDetails :many
SELECT d.sku,d.brand_id,b.name AS brand_name,d.description,d.attributes,d.version,
 ARRAY(SELECT c.category FROM catalog.product_categories c WHERE c.tenant_id=d.tenant_id AND c.sku=d.sku ORDER BY c.category)::text[] AS categories,
 COALESCE((SELECT jsonb_agg(jsonb_build_object('code',v.code,'label',v.label) ORDER BY v.position) FROM catalog.product_variants v WHERE v.tenant_id=d.tenant_id AND v.sku=d.sku),'[]'::jsonb)::jsonb AS variants
FROM catalog.product_details d JOIN catalog.brands b ON b.tenant_id=d.tenant_id AND b.id=d.brand_id
WHERE d.tenant_id=$1 AND d.sku=ANY($2::text[]) ORDER BY d.sku;
