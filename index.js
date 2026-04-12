require('dotenv').config();
const crypto  = require('crypto');
const express  = require('express');
const cors     = require('cors');
const { Resend } = require('resend');
const { createClient } = require('@supabase/supabase-js');

const app  = express();
const port = process.env.PORT || 3000;

// ── CLIENTS ─────────────────────────────────────────────────
const resend   = new Resend(process.env.RESEND_API_KEY);
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

// ── MIDDLEWARE ───────────────────────────────────────────────
app.use(cors({
  origin: [
    'https://arcanetracer.com',
    'https://www.arcanetracer.com',
    'http://localhost:3000',
    'http://127.0.0.1:5500' // local dev with Live Server
  ]
}));
app.use(express.json());

// ── HEALTH CHECK ─────────────────────────────────────────────
app.get('/health', (req, res) => {
  res.json({ ok: true, service: 'arcane-tracer-api', ts: new Date().toISOString() });
});

// ── ACCESS REQUEST ───────────────────────────────────────────
// Called by the landing page contact form.
// 1. Validates required fields
// 2. Logs to Supabase access_requests table
// 3. Sends you a detailed notification email via Resend
// 4. Sends the submitter a clean auto-reply
app.post('/access-request', async (req, res) => {
  try {
    const { name, email, org, level, usecase, terms_accepted_at, terms_version, fingerprint } = req.body;
    const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress || 'unknown';

    // ── VALIDATION ────────────────────────────────────────────
    if (!name || !email || !level || !usecase) {
      return res.status(400).json({ ok: false, error: 'Required fields missing.' });
    }
    if (!email.includes('@') || !email.includes('.')) {
      return res.status(400).json({ ok: false, error: 'Invalid email address.' });
    }

    const submittedAt = new Date().toISOString();

    // ── RESOLVE TERMS ACCEPTANCE ──────────────────────────────
    // If the frontend sent a fingerprint and terms_version, look up the
    // matching row in terms_acceptances and stamp this access_request with
    // its accepted_at. If no row is found, leave the columns NULL and log a
    // warning. The landing page modal is the primary gate; this is a
    // belt-and-suspenders check.
    let resolvedTermsAcceptedAt = terms_accepted_at || null;
    let resolvedTermsVersion = terms_version || null;
    if (fingerprint && terms_version) {
      const { data: ack, error: ackError } = await supabase
        .from('terms_acceptances')
        .select('accepted_at')
        .eq('fingerprint', fingerprint)
        .eq('terms_version', terms_version)
        .order('accepted_at', { ascending: false })
        .limit(1)
        .maybeSingle();
      if (ackError) {
        console.error('terms_acceptances lookup error:', ackError);
      }
      if (ack && ack.accepted_at) {
        resolvedTermsAcceptedAt = ack.accepted_at;
        resolvedTermsVersion = terms_version;
      } else {
        console.warn('access-request submitted without matching terms_acceptances row', {
          fingerprint, terms_version
        });
        resolvedTermsAcceptedAt = null;
        resolvedTermsVersion = null;
      }
    }

    // ── LOG TO SUPABASE ───────────────────────────────────────
    const { error: dbError } = await supabase
      .from('access_requests')
      .insert({
        name, email, org: org || null, level, usecase, ip,
        terms_accepted_at: resolvedTermsAcceptedAt,
        terms_version: resolvedTermsVersion,
        submitted_at: submittedAt,
        status: 'pending'
      });

    if (dbError) {
      console.error('Supabase insert error:', dbError);
      // Continue anyway — don't fail the user because of a DB issue
    }

    // ── NOTIFY YOU ────────────────────────────────────────────
    // Clean, formatted email delivered to your inbox immediately
    const { data: d1, error: e1 } = await resend.emails.send({
      from: 'Arcane Tracer <onboarding@resend.dev>',
      to: [process.env.OWNER_EMAIL],
      subject: `[Access Request] ${level} — ${name}`,
      html: `
        <div style="font-family:'DM Sans',Arial,sans-serif;max-width:600px;margin:0 auto;background:#050F1E;color:#ffffff;padding:32px;border:1px solid rgba(0,87,255,0.2);">
          <div style="font-family:Arial,sans-serif;font-size:11px;letter-spacing:3px;text-transform:uppercase;color:#00D4FF;margin-bottom:16px;">ARCANE TRACER · NEW ACCESS REQUEST</div>
          <h1 style="font-size:28px;font-weight:900;letter-spacing:-1px;color:#ffffff;margin:0 0 24px;">${name}</h1>

          <table style="width:100%;border-collapse:collapse;margin-bottom:24px;">
            <tr>
              <td style="padding:10px 0;border-bottom:1px solid rgba(255,255,255,0.07);color:rgba(255,255,255,0.4);font-size:12px;width:120px;vertical-align:top;">Email</td>
              <td style="padding:10px 0;border-bottom:1px solid rgba(255,255,255,0.07);color:#ffffff;font-size:14px;"><a href="mailto:${email}" style="color:#0057FF;">${email}</a></td>
            </tr>
            <tr>
              <td style="padding:10px 0;border-bottom:1px solid rgba(255,255,255,0.07);color:rgba(255,255,255,0.4);font-size:12px;vertical-align:top;">Organization</td>
              <td style="padding:10px 0;border-bottom:1px solid rgba(255,255,255,0.07);color:#ffffff;font-size:14px;">${org || 'Not provided'}</td>
            </tr>
            <tr>
              <td style="padding:10px 0;border-bottom:1px solid rgba(255,255,255,0.07);color:rgba(255,255,255,0.4);font-size:12px;vertical-align:top;">Access Level</td>
              <td style="padding:10px 0;border-bottom:1px solid rgba(255,255,255,0.07);font-size:14px;">
                <span style="background:rgba(0,87,255,0.2);color:#0057FF;padding:4px 12px;font-size:12px;font-weight:700;letter-spacing:1px;">${level.toUpperCase()}</span>
              </td>
            </tr>
            <tr>
              <td style="padding:10px 0;border-bottom:1px solid rgba(255,255,255,0.07);color:rgba(255,255,255,0.4);font-size:12px;vertical-align:top;">Submitted</td>
              <td style="padding:10px 0;border-bottom:1px solid rgba(255,255,255,0.07);color:#ffffff;font-size:14px;font-family:monospace;">${submittedAt}</td>
            </tr>
            <tr>
              <td style="padding:10px 0;color:rgba(255,255,255,0.4);font-size:12px;vertical-align:top;">IP</td>
              <td style="padding:10px 0;color:rgba(255,255,255,0.5);font-size:12px;font-family:monospace;">${ip}</td>
            </tr>
          </table>

          <div style="margin-bottom:8px;color:rgba(255,255,255,0.4);font-size:12px;text-transform:uppercase;letter-spacing:1px;">Use Case</div>
          <div style="background:rgba(255,255,255,0.05);border:1px solid rgba(255,255,255,0.07);padding:16px;font-size:14px;color:rgba(255,255,255,0.75);line-height:1.7;margin-bottom:28px;">${usecase.replace(/\n/g, '<br>')}</div>

          ${level === 'Professional'
            ? `<div style="padding:16px 20px;background:rgba(0,87,255,0.1);border:1px solid rgba(0,87,255,0.3);margin-bottom:24px;">
                <div style="font-size:12px;font-weight:700;color:#00D4FF;text-transform:uppercase;letter-spacing:1px;margin-bottom:6px;">Next Step</div>
                <div style="font-size:13px;color:rgba(255,255,255,0.6);">If approved, reply with their Stripe Professional payment link. Once paid, provision their account in Supabase and send login credentials via Resend.</div>
               </div>`
            : `<div style="padding:16px 20px;background:rgba(255,183,0,0.08);border:1px solid rgba(255,183,0,0.2);margin-bottom:24px;">
                <div style="font-size:12px;font-weight:700;color:#FFB700;text-transform:uppercase;letter-spacing:1px;margin-bottom:6px;">Next Step</div>
                <div style="font-size:13px;color:rgba(255,255,255,0.6);">Institutional request. Schedule a call or email to discuss scope. Create a custom Stripe invoice once terms are agreed.</div>
               </div>`
          }

          <a href="mailto:${email}?subject=Re: Your Arcane Tracer Access Request" style="display:inline-block;background:#0057FF;color:white;padding:12px 28px;font-size:14px;font-weight:700;letter-spacing:0.3px;text-decoration:none;">Reply to ${name.split(' ')[0]}</a>
        </div>
      `
    });
    if (e1) console.error('Resend notify error:', e1);
    else console.log('Notify email sent:', d1?.id);

    // ── AUTO-REPLY TO SUBMITTER ───────────────────────────────
    const { data: d2, error: e2 } = await resend.emails.send({
      from: 'Arcane Tracer <onboarding@resend.dev>',
      to: [email],
      subject: 'We received your Arcane Tracer access request',
      html: `
        <div style="font-family:'DM Sans',Arial,sans-serif;max-width:600px;margin:0 auto;background:#F2F7FF;color:#030B18;padding:0;border:1px solid #D0DFF0;">
          <div style="background:#050F1E;padding:28px 32px;border-bottom:3px solid #0057FF;">
            <div style="font-family:Arial,sans-serif;font-size:11px;letter-spacing:5px;text-transform:uppercase;color:#0057FF;margin-bottom:8px;">ARCANE TRACER</div>
            <div style="font-size:10px;letter-spacing:3px;text-transform:uppercase;color:rgba(255,255,255,0.3);">FINANCIAL INTELLIGENCE PLATFORM</div>
          </div>
          <div style="padding:36px 32px;">
            <h2 style="font-size:24px;font-weight:800;letter-spacing:-0.5px;color:#030B18;margin:0 0 16px;">Hi ${name.split(' ')[0]}, we got your request.</h2>
            <p style="font-size:15px;color:#3A5470;line-height:1.75;margin:0 0 24px;">Thank you for reaching out about <strong style="color:#0057FF;">${level}</strong> access. We review every request personally and will follow up within one business day.</p>

            <div style="background:#EEF4FF;border:1px solid #D0DFF0;border-left:3px solid #0057FF;padding:16px 20px;margin-bottom:28px;">
              <div style="font-size:11px;font-weight:700;color:#0057FF;text-transform:uppercase;letter-spacing:1px;margin-bottom:8px;">What happens next</div>
              <div style="font-size:13px;color:#3A5470;line-height:1.7;">
                ${level === 'Professional'
                  ? 'If your use case is a good fit, we will reply with a secure Stripe payment link to activate your Professional account. Setup takes less than 5 minutes once approved.'
                  : 'Institutional access is scoped to your specific organization and needs. We will reach out to understand your requirements and put together the right setup for you.'
                }
              </div>
            </div>

            <div style="font-size:13px;color:#7090B0;margin-bottom:8px;text-transform:uppercase;letter-spacing:0.5px;">Your submission</div>
            <table style="width:100%;border-collapse:collapse;margin-bottom:28px;">
              <tr>
                <td style="padding:8px 0;border-bottom:1px solid #D0DFF0;color:#7090B0;font-size:12px;width:110px;">Name</td>
                <td style="padding:8px 0;border-bottom:1px solid #D0DFF0;font-size:13px;color:#030B18;">${name}</td>
              </tr>
              <tr>
                <td style="padding:8px 0;border-bottom:1px solid #D0DFF0;color:#7090B0;font-size:12px;">Organization</td>
                <td style="padding:8px 0;border-bottom:1px solid #D0DFF0;font-size:13px;color:#030B18;">${org || 'Not provided'}</td>
              </tr>
              <tr>
                <td style="padding:8px 0;color:#7090B0;font-size:12px;">Access Level</td>
                <td style="padding:8px 0;font-size:13px;color:#0057FF;font-weight:600;">${level}</td>
              </tr>
            </table>

            <p style="font-size:13px;color:#7090B0;line-height:1.7;margin:0 0 4px;">In the meantime, the live anomaly feed is available at <a href="https://arcanetracer.com" style="color:#0057FF;">arcanetracer.com</a> with no account required.</p>
          </div>
          <div style="background:#F2F7FF;border-top:1px solid #D0DFF0;padding:16px 32px;display:flex;justify-content:space-between;align-items:center;">
            <div style="font-size:11px;color:#B0C8E0;">© 2026 Arcane Tracer · Flavio DeOliveira LLC</div>
            <div style="font-size:11px;color:#B0C8E0;"><a href="https://arcanetracer.com" style="color:#0057FF;text-decoration:none;">arcanetracer.com</a></div>
          </div>
        </div>
      `
    });
    if (e2) console.error('Resend auto-reply error:', e2);
    else console.log('Auto-reply sent:', d2?.id);

    return res.json({ ok: true });

  } catch (err) {
    console.error('Access request error:', err);
    return res.status(500).json({ ok: false, error: 'Server error. Please try again or email hello@arcanetracer.com directly.' });
  }
});

// ── TERMS: CHECK ─────────────────────────────────────────────
// Frontend calls this on landing to decide whether to show the
// terms+methodology acceptance modal. Keyed on browser fingerprint
// and the current terms_version string.
app.get('/api/check-terms', async (req, res) => {
  try {
    const { fingerprint, version } = req.query;
    if (!fingerprint || !version) {
      return res.status(400).json({ error: 'missing required parameter' });
    }
    const { data, error } = await supabase
      .from('terms_acceptances')
      .select('terms_version, accepted_at')
      .eq('fingerprint', fingerprint)
      .eq('terms_version', version)
      .order('accepted_at', { ascending: false })
      .limit(1)
      .maybeSingle();
    if (error) {
      console.error('check-terms query error:', error);
      return res.status(500).json({ error: 'database error' });
    }
    if (!data) {
      return res.json({ accepted: false });
    }
    return res.json({
      accepted: true,
      version: data.terms_version,
      accepted_at: data.accepted_at
    });
  } catch (err) {
    console.error('check-terms handler error:', err);
    return res.status(500).json({ error: 'database error' });
  }
});

// ── TERMS: ACCEPT ────────────────────────────────────────────
// Records a terms+methodology acceptance event. We hash the caller
// IP with SHA-256 for anti-abuse only; the raw IP is never stored.
app.post('/api/accept-terms', async (req, res) => {
  try {
    const { fingerprint, terms_version, methodology_version } = req.body || {};
    if (!fingerprint || !terms_version) {
      return res.status(400).json({ error: 'missing required parameter' });
    }
    const rawIp = (req.headers['x-forwarded-for']?.split(',')[0]?.trim()) || req.socket.remoteAddress || '';
    const ipHash = rawIp ? crypto.createHash('sha256').update(rawIp).digest('hex') : null;
    const userAgent = req.headers['user-agent'] || null;

    const { data, error } = await supabase
      .from('terms_acceptances')
      .insert({
        fingerprint,
        terms_version,
        methodology_version: methodology_version || null,
        ip_hash: ipHash,
        user_agent: userAgent
      })
      .select('accepted_at')
      .single();
    if (error) {
      console.error('accept-terms insert error:', error);
      return res.status(500).json({ error: 'database error' });
    }
    return res.json({ ok: true, accepted_at: data.accepted_at });
  } catch (err) {
    console.error('accept-terms handler error:', err);
    return res.status(500).json({ error: 'database error' });
  }
});

// ── ADMIN AUTH MIDDLEWARE ────────────────────────────────────
function requireAdmin(req, res, next) {
  const key = (req.headers.authorization || '').replace(/^Bearer\s+/i, '');
  if (!key || key !== process.env.ADMIN_API_KEY) {
    return res.status(401).json({ ok: false, error: 'Unauthorized' });
  }
  next();
}

// ── INGESTION ENDPOINTS ─────────────────────────────────────
app.post('/api/ingest/usaspending', requireAdmin, async (req, res) => {
  try {
    const {
      fiscal_year = new Date().getFullYear(),
      award_type = 'contracts',
      max_pages = 10
    } = req.body || {};

    // Create pull row synchronously to return the ID
    const { data: pull, error: pullErr } = await supabase
      .from('data_pulls')
      .insert({
        source: 'usaspending',
        pull_type: 'incremental',
        status: 'running',
        metadata: { fiscal_year, award_type, max_pages }
      })
      .select('id')
      .single();

    if (pullErr) {
      return res.status(500).json({ ok: false, error: 'Failed to create pull record' });
    }

    // Kick off ingestion async (don't await)
    ingestUSASpendingWithPull(pull.id, { fiscal_year, award_type, max_pages }).catch(err => {
      console.error(`[USASpending] Async ingestion error for pull ${pull.id}:`, err.message);
    });

    return res.json({ ok: true, pull_id: pull.id, message: 'Ingestion started' });
  } catch (err) {
    console.error('Ingest endpoint error:', err);
    return res.status(500).json({ ok: false, error: 'Server error' });
  }
});

// Variant that uses an existing pull ID (for the async kickoff from the endpoint)
async function ingestUSASpendingWithPull(pullId, options = {}) {
  const {
    fiscal_year = new Date().getFullYear(),
    award_type = 'contracts',
    limit_per_page = 100,
    max_pages = 10
  } = options;

  let recordsFetched = 0;
  let recordsCreated = 0;
  let recordsUpdated = 0;

  console.log(`[USASpending] Pull ${pullId} started: FY${fiscal_year} ${award_type}, max ${max_pages} pages`);

  try {
    const typeMap = {
      contracts: ['A', 'B', 'C', 'D'],
      grants: ['02', '03', '04', '05'],
      loans: ['07', '08'],
      direct_payments: ['06', '10'],
      other: ['09', '11']
    };
    const awardTypes = typeMap[award_type] || typeMap.contracts;

    for (let page = 1; page <= max_pages; page++) {
      const body = {
        filters: {
          time_period: [{ start_date: `${fiscal_year}-10-01`, end_date: `${fiscal_year + 1}-09-30` }],
          award_type_codes: awardTypes
        },
        fields: [
          'Award ID', 'Recipient Name', 'Recipient DUNS Number',
          'Recipient UEI', 'Award Amount', 'Total Obligation',
          'Awarding Agency', 'Funding Agency', 'Description',
          'Start Date', 'End Date', 'Award Type',
          'recipient_id', 'internal_id',
          'Recipient State Code', 'Recipient Country Code'
        ],
        page,
        limit: limit_per_page,
        sort: 'Award Amount',
        order: 'desc'
      };

      const resp = await fetch('https://api.usaspending.gov/api/v2/search/spending_by_award/', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body)
      });

      if (!resp.ok) {
        const errText = await resp.text();
        throw new Error(`USASpending API error ${resp.status}: ${errText.slice(0, 200)}`);
      }

      const data = await resp.json();
      const results = data.results || [];

      if (results.length === 0) {
        console.log(`[USASpending] Page ${page}: no more results, stopping`);
        break;
      }

      recordsFetched += results.length;

      for (const award of results) {
        const recipientName = award['Recipient Name'] || award.recipient_name || 'Unknown';
        const uei = award['Recipient UEI'] || award.recipient_uei || null;
        const duns = award['Recipient DUNS Number'] || award.recipient_duns || null;
        const stateCode = award['Recipient State Code'] || null;
        const countryCode = award['Recipient Country Code'] || 'US';

        let entityId;
        const entityRow = {
          name: recipientName,
          entity_type: 'contractor',
          uei: uei || null,
          duns: duns || null,
          state: stateCode,
          country: countryCode,
          source: 'usaspending',
          updated_at: new Date().toISOString()
        };

        let existing = null;
        if (uei) {
          const { data: found } = await supabase
            .from('entities')
            .select('id')
            .eq('uei', uei)
            .maybeSingle();
          existing = found;
        }
        if (!existing && duns) {
          const { data: found } = await supabase
            .from('entities')
            .select('id')
            .eq('duns', duns)
            .maybeSingle();
          existing = found;
        }

        if (existing) {
          entityId = existing.id;
          await supabase.from('entities').update(entityRow).eq('id', entityId);
          recordsUpdated++;
        } else {
          const { data: inserted, error: entErr } = await supabase
            .from('entities')
            .insert(entityRow)
            .select('id')
            .single();
          if (entErr) {
            console.error(`[USASpending] Entity insert error for "${recipientName}":`, entErr.message);
            continue;
          }
          entityId = inserted.id;
          recordsCreated++;
        }

        const awardId = award['Award ID'] || award.internal_id || `usa-${Date.now()}-${Math.random()}`;
        const awardRow = {
          entity_id: entityId,
          award_type: award['Award Type'] || award_type,
          award_id: awardId,
          amount_obligated: parseFloat(award['Total Obligation']) || 0,
          total_value: parseFloat(award['Award Amount']) || 0,
          awarding_agency: award['Awarding Agency'] || null,
          funding_agency: award['Funding Agency'] || null,
          description: award['Description'] || null,
          period_start: award['Start Date'] || null,
          period_end: award['End Date'] || null,
          source: 'usaspending',
          source_url: `https://www.usaspending.gov/award/${awardId}`,
          raw_data: award
        };

        const { error: awdErr } = await supabase
          .from('awards')
          .upsert(awardRow, { onConflict: 'award_id' });

        if (awdErr) {
          console.error(`[USASpending] Award upsert error for "${awardId}":`, awdErr.message);
        }
      }

      if (page % 10 === 0 || page === max_pages || results.length < limit_per_page) {
        console.log(`[USASpending] Progress: page ${page}/${max_pages}, fetched=${recordsFetched}, created=${recordsCreated}, updated=${recordsUpdated}`);
      }

      if (page < max_pages && results.length > 0) {
        await new Promise(r => setTimeout(r, 200));
      }
    }

    await supabase.from('data_pulls').update({
      status: 'completed',
      completed_at: new Date().toISOString(),
      records_fetched: recordsFetched,
      records_created: recordsCreated,
      records_updated: recordsUpdated
    }).eq('id', pullId);

    console.log(`[USASpending] Pull ${pullId} completed: fetched=${recordsFetched}, created=${recordsCreated}, updated=${recordsUpdated}`);
    return { pullId, recordsFetched, recordsCreated, recordsUpdated };

  } catch (err) {
    console.error(`[USASpending] Pull ${pullId} failed:`, err.message);
    await supabase.from('data_pulls').update({
      status: 'failed',
      completed_at: new Date().toISOString(),
      error_message: err.message,
      records_fetched: recordsFetched,
      records_created: recordsCreated,
      records_updated: recordsUpdated
    }).eq('id', pullId);
    throw err;
  }
}

app.get('/api/ingest/status/:pull_id', requireAdmin, async (req, res) => {
  const { data, error } = await supabase
    .from('data_pulls')
    .select('*')
    .eq('id', req.params.pull_id)
    .maybeSingle();

  if (error) return res.status(500).json({ ok: false, error: 'Database error' });
  if (!data) return res.status(404).json({ ok: false, error: 'Pull not found' });
  return res.json({ ok: true, pull: data });
});

// ── PUBLIC ENTITY/AWARD ENDPOINTS ───────────────────────────
app.get('/api/entities', async (req, res) => {
  try {
    const page = Math.max(1, parseInt(req.query.page) || 1);
    const limit = Math.min(100, Math.max(1, parseInt(req.query.limit) || 20));
    const search = req.query.search || null;
    const offset = (page - 1) * limit;

    let query = supabase
      .from('entities')
      .select(`
        id, name, entity_type, uei, duns, state, country, source, created_at, updated_at,
        risk_scores ( total_score, flag_count )
      `, { count: 'exact' })
      .range(offset, offset + limit - 1)
      .order('created_at', { ascending: false });

    if (search) {
      query = query.ilike('name', `%${search}%`);
    }

    const { data, error, count } = await query;

    if (error) {
      console.error('Entities query error:', error);
      return res.status(500).json({ ok: false, error: 'Database error' });
    }

    // Flatten risk_scores (it comes as an array from the join)
    const entities = (data || []).map(e => {
      const rs = Array.isArray(e.risk_scores) ? e.risk_scores[0] : e.risk_scores;
      return {
        ...e,
        risk_scores: undefined,
        total_score: rs?.total_score ?? null,
        flag_count: rs?.flag_count ?? null
      };
    });

    return res.json({
      ok: true,
      entities,
      pagination: { page, limit, total: count, pages: Math.ceil((count || 0) / limit) }
    });
  } catch (err) {
    console.error('Entities endpoint error:', err);
    return res.status(500).json({ ok: false, error: 'Server error' });
  }
});

app.get('/api/awards/:entity_id', async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('awards')
      .select('*')
      .eq('entity_id', req.params.entity_id)
      .order('amount_obligated', { ascending: false });

    if (error) {
      console.error('Awards query error:', error);
      return res.status(500).json({ ok: false, error: 'Database error' });
    }

    return res.json({ ok: true, awards: data || [] });
  } catch (err) {
    console.error('Awards endpoint error:', err);
    return res.status(500).json({ ok: false, error: 'Server error' });
  }
});

// ── START ─────────────────────────────────────────────────────
app.listen(port, () => {
  console.log(`Arcane Tracer API running on port ${port}`);
});
