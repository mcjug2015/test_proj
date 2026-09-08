-- revision_id:07990e2a101e;
-- prev_revision_id:e5ce0039b32b;
begin
create table if not exists {{cat}}.{{schema}}.metrics(
    id string,
    metric_name string,
    payload variant,
    last_updated TIMESTAMP
);
end;