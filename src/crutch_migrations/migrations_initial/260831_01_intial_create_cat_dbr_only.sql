-- revision_id:;
begin
create catalog if not exists {{cat}};
grant ALL PRIVILEGES, MANAGE on catalog {{cat}} to `users_and_sps`;
end;