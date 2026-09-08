-- revision_id:db357bc27b12;
-- prev_revision_id:;
begin
create volume if not exists {{cat}}.{{schema}}.vol1;
end;