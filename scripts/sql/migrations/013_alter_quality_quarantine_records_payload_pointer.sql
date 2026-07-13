alter table quality.quarantine_records
    add column if not exists payload_bucket text,
    add column if not exists payload_object_key text,
    add column if not exists payload_size_bytes bigint,
    add column if not exists payload_checksum_sha256 text;

do $$
begin
    if not exists (
        select 1
        from pg_constraint
        where conname = 'quarantine_records_payload_size_nonnegative'
          and conrelid = 'quality.quarantine_records'::regclass
    ) then
        alter table quality.quarantine_records
            add constraint quarantine_records_payload_size_nonnegative
            check (payload_size_bytes is null or payload_size_bytes >= 0);
    end if;
end $$;

create index if not exists quarantine_records_payload_object_idx
    on quality.quarantine_records(payload_bucket, payload_object_key);
