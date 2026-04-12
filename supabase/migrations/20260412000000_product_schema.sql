-- Stage 2: Full product schema for Arcane Tracer
-- Tables: entities, awards, flags, risk_scores, briefs, feed_items, data_pulls, exclusion_list, users

-- =============================================================================
-- 1. entities
-- =============================================================================
CREATE TABLE IF NOT EXISTS public.entities (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name text NOT NULL,
    entity_type text NOT NULL,
    duns text UNIQUE,
    uei text UNIQUE,
    cage_code text,
    state text,
    country text DEFAULT 'US',
    incorporation_date date,
    source text NOT NULL,
    source_id text,
    metadata jsonb DEFAULT '{}',
    created_at timestamptz DEFAULT now(),
    updated_at timestamptz DEFAULT now()
);

ALTER TABLE public.entities ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Service role only"
    ON public.entities AS PERMISSIVE FOR ALL TO public
    USING ((select auth.role()) = 'service_role'::text);

CREATE INDEX IF NOT EXISTS idx_entities_name ON public.entities USING btree (name text_pattern_ops);
CREATE INDEX IF NOT EXISTS idx_entities_uei ON public.entities USING btree (uei);
CREATE INDEX IF NOT EXISTS idx_entities_duns ON public.entities USING btree (duns);
CREATE INDEX IF NOT EXISTS idx_entities_source_source_id ON public.entities USING btree (source, source_id);

-- =============================================================================
-- 2. awards
-- =============================================================================
CREATE TABLE IF NOT EXISTS public.awards (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_id uuid NOT NULL REFERENCES public.entities(id) ON DELETE CASCADE,
    award_type text NOT NULL,
    award_id text NOT NULL UNIQUE,
    amount_obligated numeric(15,2),
    total_value numeric(15,2),
    awarding_agency text,
    funding_agency text,
    description text,
    period_start date,
    period_end date,
    source text DEFAULT 'usaspending',
    source_url text,
    raw_data jsonb DEFAULT '{}',
    created_at timestamptz DEFAULT now()
);

ALTER TABLE public.awards ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Service role only"
    ON public.awards AS PERMISSIVE FOR ALL TO public
    USING ((select auth.role()) = 'service_role'::text);

CREATE INDEX IF NOT EXISTS idx_awards_entity_id ON public.awards USING btree (entity_id);
CREATE INDEX IF NOT EXISTS idx_awards_award_type ON public.awards USING btree (award_type);
CREATE INDEX IF NOT EXISTS idx_awards_period_start ON public.awards USING btree (period_start);

-- =============================================================================
-- 3. flags
-- =============================================================================
CREATE TABLE IF NOT EXISTS public.flags (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_id uuid NOT NULL REFERENCES public.entities(id) ON DELETE CASCADE,
    flag_type text NOT NULL,
    severity text NOT NULL DEFAULT 'medium',
    score_contribution integer NOT NULL,
    citation_source text NOT NULL,
    citation_url text,
    citation_detail text NOT NULL,
    is_active boolean DEFAULT true,
    reviewed_at timestamptz,
    reviewed_by text,
    created_at timestamptz DEFAULT now(),
    updated_at timestamptz DEFAULT now()
);

ALTER TABLE public.flags ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Service role only"
    ON public.flags AS PERMISSIVE FOR ALL TO public
    USING ((select auth.role()) = 'service_role'::text);

CREATE INDEX IF NOT EXISTS idx_flags_entity_id ON public.flags USING btree (entity_id);
CREATE INDEX IF NOT EXISTS idx_flags_flag_type ON public.flags USING btree (flag_type);
CREATE INDEX IF NOT EXISTS idx_flags_is_active ON public.flags USING btree (is_active);

-- =============================================================================
-- 4. risk_scores
-- =============================================================================
CREATE TABLE IF NOT EXISTS public.risk_scores (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_id uuid NOT NULL REFERENCES public.entities(id) ON DELETE CASCADE UNIQUE,
    total_score integer NOT NULL DEFAULT 0,
    flag_count integer NOT NULL DEFAULT 0,
    meets_public_threshold boolean DEFAULT false,
    excluded boolean DEFAULT false,
    exclusion_reason text,
    last_computed_at timestamptz DEFAULT now(),
    created_at timestamptz DEFAULT now(),
    updated_at timestamptz DEFAULT now()
);

ALTER TABLE public.risk_scores ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Service role only"
    ON public.risk_scores AS PERMISSIVE FOR ALL TO public
    USING ((select auth.role()) = 'service_role'::text);

CREATE INDEX IF NOT EXISTS idx_risk_scores_meets_public ON public.risk_scores USING btree (meets_public_threshold);
CREATE INDEX IF NOT EXISTS idx_risk_scores_total_score ON public.risk_scores USING btree (total_score DESC);

-- =============================================================================
-- 5. briefs
-- =============================================================================
CREATE TABLE IF NOT EXISTS public.briefs (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_id uuid NOT NULL REFERENCES public.entities(id) ON DELETE CASCADE,
    risk_score_id uuid REFERENCES public.risk_scores(id) ON DELETE SET NULL,
    content text NOT NULL,
    summary text,
    model_version text NOT NULL,
    prompt_hash text,
    citations jsonb NOT NULL DEFAULT '[]',
    is_current boolean DEFAULT true,
    created_at timestamptz DEFAULT now()
);

ALTER TABLE public.briefs ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Service role only"
    ON public.briefs AS PERMISSIVE FOR ALL TO public
    USING ((select auth.role()) = 'service_role'::text);

CREATE INDEX IF NOT EXISTS idx_briefs_entity_id ON public.briefs USING btree (entity_id);
CREATE INDEX IF NOT EXISTS idx_briefs_risk_score_id ON public.briefs USING btree (risk_score_id);
CREATE INDEX IF NOT EXISTS idx_briefs_is_current ON public.briefs USING btree (is_current);

-- =============================================================================
-- 6. feed_items
-- =============================================================================
CREATE TABLE IF NOT EXISTS public.feed_items (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_id uuid NOT NULL REFERENCES public.entities(id) ON DELETE CASCADE,
    risk_score_id uuid REFERENCES public.risk_scores(id) ON DELETE SET NULL,
    brief_id uuid REFERENCES public.briefs(id) ON DELETE SET NULL,
    published_at timestamptz DEFAULT now(),
    unpublished_at timestamptz,
    created_at timestamptz DEFAULT now()
);

ALTER TABLE public.feed_items ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Service role only"
    ON public.feed_items AS PERMISSIVE FOR ALL TO public
    USING ((select auth.role()) = 'service_role'::text);

CREATE INDEX IF NOT EXISTS idx_feed_items_entity_id ON public.feed_items USING btree (entity_id);
CREATE INDEX IF NOT EXISTS idx_feed_items_risk_score_id ON public.feed_items USING btree (risk_score_id);
CREATE INDEX IF NOT EXISTS idx_feed_items_brief_id ON public.feed_items USING btree (brief_id);
CREATE INDEX IF NOT EXISTS idx_feed_items_published_at ON public.feed_items USING btree (published_at DESC);

-- =============================================================================
-- 7. data_pulls
-- =============================================================================
CREATE TABLE IF NOT EXISTS public.data_pulls (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source text NOT NULL,
    pull_type text NOT NULL,
    started_at timestamptz DEFAULT now(),
    completed_at timestamptz,
    status text DEFAULT 'running',
    records_fetched integer DEFAULT 0,
    records_created integer DEFAULT 0,
    records_updated integer DEFAULT 0,
    error_message text,
    metadata jsonb DEFAULT '{}',
    created_at timestamptz DEFAULT now()
);

ALTER TABLE public.data_pulls ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Service role only"
    ON public.data_pulls AS PERMISSIVE FOR ALL TO public
    USING ((select auth.role()) = 'service_role'::text);

CREATE INDEX IF NOT EXISTS idx_data_pulls_source ON public.data_pulls USING btree (source);
CREATE INDEX IF NOT EXISTS idx_data_pulls_status ON public.data_pulls USING btree (status);
CREATE INDEX IF NOT EXISTS idx_data_pulls_started_at ON public.data_pulls USING btree (started_at DESC);

-- =============================================================================
-- 8. exclusion_list
-- =============================================================================
CREATE TABLE IF NOT EXISTS public.exclusion_list (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    category text NOT NULL UNIQUE,
    description text NOT NULL,
    is_active boolean DEFAULT true,
    created_at timestamptz DEFAULT now()
);

ALTER TABLE public.exclusion_list ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Service role only"
    ON public.exclusion_list AS PERMISSIVE FOR ALL TO public
    USING ((select auth.role()) = 'service_role'::text);

-- =============================================================================
-- 9. users (app-level, not auth.users)
-- =============================================================================
CREATE TABLE IF NOT EXISTS public.users (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    email text NOT NULL UNIQUE,
    name text,
    org text,
    tier text NOT NULL DEFAULT 'free',
    stripe_customer_id text,
    stripe_subscription_id text,
    subscription_status text DEFAULT 'none',
    created_at timestamptz DEFAULT now(),
    updated_at timestamptz DEFAULT now()
);

ALTER TABLE public.users ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Service role only"
    ON public.users AS PERMISSIVE FOR ALL TO public
    USING ((select auth.role()) = 'service_role'::text);

CREATE INDEX IF NOT EXISTS idx_users_stripe_customer_id ON public.users USING btree (stripe_customer_id);
