-- Fix auth_rls_initplan perf advisor warning on public.access_requests.
-- The existing "Service role only" policy calls auth.role() directly, which
-- Postgres re-evaluates for every row. Wrapping the call in (select ...) lets
-- the planner cache the result once per query.
--
-- Applied to production on 2026-04-11 via the Supabase MCP.

DROP POLICY IF EXISTS "Service role only" ON public.access_requests;

CREATE POLICY "Service role only"
    ON public.access_requests
    AS PERMISSIVE
    FOR ALL
    TO public
    USING ((select auth.role()) = 'service_role');
