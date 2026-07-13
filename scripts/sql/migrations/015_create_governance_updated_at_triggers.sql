create or replace function governance.set_updated_at()
returns trigger
language plpgsql
as $$
begin
    new.updated_at = now();
    return new;
end;
$$;

drop trigger if exists set_updated_at_data_contract_registry
    on governance.data_contract_registry;

create trigger set_updated_at_data_contract_registry
before update on governance.data_contract_registry
for each row
execute function governance.set_updated_at();

drop trigger if exists set_updated_at_column_classification
    on governance.column_classification;

create trigger set_updated_at_column_classification
before update on governance.column_classification
for each row
execute function governance.set_updated_at();
