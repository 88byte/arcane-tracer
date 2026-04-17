-- Bulk ingestion schema additions for SAM Exclusions, IRS 990 officers,
-- FAPIIS records, and SAM monthly Entity bulk-extract columns on entities.

-- SAM.gov Exclusions List (federal debarment)
create table if not exists sam_exclusions (
  id                uuid primary key default gen_random_uuid(),
  exclusion_id      text not null unique,
  name              text,
  dba_name          text,
  address           jsonb,
  exclusion_type    text,
  exclusion_program text,
  excluding_agency  text,
  active_date       date,
  termination_date  date,
  raw_data          jsonb,
  created_at        timestamptz not null default now(),
  updated_at        timestamptz not null default now()
);
create index if not exists idx_sam_exclusions_name on sam_exclusions(name);
create index if not exists idx_sam_exclusions_active_date on sam_exclusions(active_date);
alter table sam_exclusions enable row level security;
drop policy if exists "Service role only" on sam_exclusions;
create policy "Service role only" on sam_exclusions
  for all using (auth.role() = 'service_role');

-- Entity officers (IRS 990 via ProPublica, and future sources)
create table if not exists entity_officers (
  id            uuid primary key default gen_random_uuid(),
  entity_id     uuid references entities(id) on delete cascade,
  officer_name  text not null,
  title         text,
  compensation  numeric,
  source        text,
  filing_year   integer,
  raw_data      jsonb,
  created_at    timestamptz not null default now()
);
create index if not exists idx_entity_officers_entity on entity_officers(entity_id);
create index if not exists idx_entity_officers_name on entity_officers(officer_name);
alter table entity_officers enable row level security;
drop policy if exists "Service role only" on entity_officers;
create policy "Service role only" on entity_officers
  for all using (auth.role() = 'service_role');

-- FAPIIS records (federal contractor performance history)
create table if not exists fapiis_records (
  id            uuid primary key default gen_random_uuid(),
  record_id     text not null unique,
  entity_name   text,
  uei           text,
  record_type   text,
  description   text,
  agency        text,
  record_date   date,
  raw_data      jsonb,
  created_at    timestamptz not null default now()
);
create index if not exists idx_fapiis_records_uei on fapiis_records(uei);
create index if not exists idx_fapiis_records_entity_name on fapiis_records(entity_name);
create index if not exists idx_fapiis_records_date on fapiis_records(record_date);
alter table fapiis_records enable row level security;
drop policy if exists "Service role only" on fapiis_records;
create policy "Service role only" on fapiis_records
  for all using (auth.role() = 'service_role');

-- SAM monthly Entity Public V2 bulk-extract fields on entities
alter table public.entities add column if not exists address jsonb;
alter table public.entities add column if not exists ein text;
alter table public.entities add column if not exists naics_primary text;
alter table public.entities add column if not exists sam_registration_status text;
alter table public.entities add column if not exists sam_exclusion_flag boolean;
alter table public.entities add column if not exists sam_registration_date date;
alter table public.entities add column if not exists sam_expiration_date date;
