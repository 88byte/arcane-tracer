-- Baseline capture of public.access_requests as it exists in production.
-- Idempotent: uses IF NOT EXISTS so running this against a project that already
-- has the table is a no-op. This file documents the current shape of the table,
-- its indexes, and its RLS policy so future migrations can be linearized.

-- Table
CREATE TABLE IF NOT EXISTS public.access_requests (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name text NOT NULL,
    email text NOT NULL,
    org text,
    level text NOT NULL,
    usecase text NOT NULL,
    ip text,
    status text DEFAULT 'pending'::text,
    stripe_link text,
    notes text,
    submitted_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz DEFAULT now()
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_access_requests_email
    ON public.access_requests USING btree (email);

CREATE INDEX IF NOT EXISTS idx_access_requests_status
    ON public.access_requests USING btree (status);

-- Row level security
ALTER TABLE public.access_requests ENABLE ROW LEVEL SECURITY;

-- Policy: service role only. Inserts from the Next.js and Express apps must go
-- through the service role key. Anon and authenticated roles have no access.
-- Note: the production policy has been updated by migration
-- 20260411000001_fix_access_requests_rls_perf.sql to wrap auth.role() in a
-- subquery. This baseline documents the original definition for historical
-- continuity; if you are replaying migrations from scratch, run the perf fix
-- immediately after this file.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_policies
        WHERE schemaname = 'public'
          AND tablename = 'access_requests'
          AND policyname = 'Service role only'
    ) THEN
        CREATE POLICY "Service role only"
            ON public.access_requests
            AS PERMISSIVE
            FOR ALL
            TO public
            USING (auth.role() = 'service_role');
    END IF;
END
$$;
