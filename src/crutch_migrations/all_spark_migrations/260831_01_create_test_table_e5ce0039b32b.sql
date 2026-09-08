-- revision_id:e5ce0039b32b;
-- prev_revision_id:;
begin
create table if not exists {{cat}}.{{schema}}.test_table(int_id bigint, stuff string);
end;