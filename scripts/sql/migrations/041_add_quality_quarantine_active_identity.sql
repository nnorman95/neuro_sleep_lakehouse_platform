create unique index if not exists quarantine_records_active_identity_uidx
    on quality.quarantine_records (
        source_system,
        record_key,
        error_code
    )
    where status in ('open', 'reviewed');
