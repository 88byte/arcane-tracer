-- Terms of service and methodology acceptance tracking.
-- 1. Adds nullable terms columns to access_requests so existing rows remain valid.
-- 2. Creates terms_acceptances to record each browser fingerprint acceptance
--    event, with an SHA-256 ip_hash and user_agent for anti-abuse only. The
--    raw IP is never stored.
-- 3. RLS: service role only, consistent with access_requests. The backend
--    writes via the service role key; anon and authenticated have no access.

-- ── access_requests: add nullable terms columns ──────────────
ALTER TABLE public.access_requests
    ADD COLUMN IF NOT EXISTS terms_accepted_at timestamptz;

ALTER TABLE public.access_requests
    ADD COLUMN IF NOT EXISTS terms_version text;

-- ── terms_acceptances table ──────────────────────────────────
CREATE TABLE IF NOT EXISTS public.terms_acceptances (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    fingerprint text NOT NULL,
    terms_version text NOT NULL,
    methodology_version text,
    accepted_at timestamptz NOT NULL DEFAULT now(),
    ip_hash text,
    user_agent text
);

CREATE INDEX IF NOT EXISTS idx_terms_acceptances_fingerprint
    ON public.terms_acceptances USING btree (fingerprint);

-- ── Row level security ───────────────────────────────────────
ALTER TABLE public.terms_acceptances ENABLE ROW LEVEL SECURITY;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_policies
        WHERE schemaname = 'public'
          AND tablename = 'terms_acceptances'
          AND policyname = 'Service role only'
    ) THEN
        CREATE POLICY "Service role only"
            ON public.terms_acceptances
            AS PERMISSIVE
            FOR ALL
            TO public
            USING ((select auth.role()) = 'service_role'::text);
    END IF;
END
$$;
