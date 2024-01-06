CREATE TABLE identity__address_types (
    address_type_id Int32,
    name String,
    created_at DateTime64
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY address_type_id;
---
CREATE TABLE identity__individuals (
  individual_id Int32,
  individual_uuid String,
  favorite_color String,
  created_at DateTime64,
  updated_at DateTime64
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY individual_id;
