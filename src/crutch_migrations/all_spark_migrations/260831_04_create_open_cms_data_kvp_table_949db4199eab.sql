-- revision_id:949db4199eab;
-- prev_revision_id:57037b6c19b4;
begin
create table if not exists {{cat}}.{{schema}}.open_cms_data_kvp(
    load_id string
    , zip_name string
    , unzipped_name string
    , sheet_name string
    , sheet_index bigint
    , heading string
    , table_row_index bigint
    , table_key string
    , table_key_simple string
    , table_val string
    , created_at TIMESTAMP
    , updated_at TIMESTAMP
);
end;