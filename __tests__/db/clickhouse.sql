CREATE TABLE identity__address_types (
    address_type_id Int32,
    name String,
    created_at DateTime64
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY address_type_id;
