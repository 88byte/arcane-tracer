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

// ── INGESTION EMAIL NOTIFICATION ────────────────────────────
async function sendIngestionEmail(pullId, { source, pull_type, status, recordsFetched, recordsCreated, recordsUpdated, durationMs, errorMessage }) {
  try {
    const durationStr = durationMs
      ? `${Math.floor(durationMs / 60000)}m ${Math.floor((durationMs % 60000) / 1000)}s`
      : 'unknown';
    const statusEmoji = status === 'completed' ? 'OK' : 'FAILED';
    const subject = `[Arcane Tracer] Ingestion ${statusEmoji}: ${source} ${pull_type}`;
    const body = [
      `Pull ID: ${pullId}`,
      `Source: ${source}`,
      `Pull Type: ${pull_type}`,
      `Status: ${status}`,
      `Records Fetched: ${recordsFetched}`,
      `Records Created: ${recordsCreated}`,
      `Records Updated: ${recordsUpdated}`,
      `Duration: ${durationStr}`,
      errorMessage ? `Error: ${errorMessage}` : null
    ].filter(Boolean).join('\n');

    await resend.emails.send({
      from: 'Arcane Tracer <onboarding@resend.dev>',
      to: [process.env.OWNER_EMAIL],
      subject,
      text: body
    });
    console.log(`[Notification] Ingestion email sent for pull ${pullId}`);
  } catch (emailErr) {
    console.error(`[Notification] Failed to send ingestion email for pull ${pullId}:`, emailErr.message);
  }
}

// Variant that uses an existing pull ID (for the async kickoff from the endpoint)
async function ingestUSASpendingWithPull(pullId, options = {}) {
  const {
    fiscal_year = new Date().getFullYear(),
    award_type = 'contracts',
    limit_per_page = 100,
    max_pages = 10,
    page_delay_ms = 200
  } = options;

  const startTime = Date.now();
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
          'Award ID', 'Recipient Name', 'Recipient UEI',
          'Award Amount', 'Awarding Agency', 'Funding Agency',
          'Description', 'Start Date', 'End Date', 'Award Type'
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
        const recipientName = award['Recipient Name'] || 'Unknown';
        const uei = award['Recipient UEI'] || null;
        const duns = null; // DUNS not available in search results

        let entityId;
        const entityRow = {
          name: recipientName,
          entity_type: 'contractor',
          uei: uei || null,
          duns: duns || null,
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

        const awardId = award['Award ID'] || award.generated_internal_id || `usa-${Date.now()}-${Math.random()}`;
        const awardRow = {
          entity_id: entityId,
          award_type: award['Award Type'] || award_type,
          award_id: awardId,
          amount_obligated: parseFloat(award['Award Amount']) || 0,
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
        console.log(`[USASpending] ${options.pull_type || 'Pull'} FY${fiscal_year} ${award_type}: page ${page}/${max_pages}, ${recordsFetched} records so far`);
      }

      if (page < max_pages && results.length > 0) {
        await new Promise(r => setTimeout(r, page_delay_ms));
      }
    }

    await supabase.from('data_pulls').update({
      status: 'completed',
      completed_at: new Date().toISOString(),
      records_fetched: recordsFetched,
      records_created: recordsCreated,
      records_updated: recordsUpdated
    }).eq('id', pullId);

    const durationMs = Date.now() - startTime;
    console.log(`[USASpending] Pull ${pullId} completed: fetched=${recordsFetched}, created=${recordsCreated}, updated=${recordsUpdated}`);

    await sendIngestionEmail(pullId, {
      source: 'usaspending', pull_type: `FY${fiscal_year} ${award_type}`,
      status: 'completed', recordsFetched, recordsCreated, recordsUpdated, durationMs
    });

    return { pullId, recordsFetched, recordsCreated, recordsUpdated };

  } catch (err) {
    const durationMs = Date.now() - startTime;
    console.error(`[USASpending] Pull ${pullId} failed:`, err.message);
    await supabase.from('data_pulls').update({
      status: 'failed',
      completed_at: new Date().toISOString(),
      error_message: err.message,
      records_fetched: recordsFetched,
      records_created: recordsCreated,
      records_updated: recordsUpdated
    }).eq('id', pullId);

    await sendIngestionEmail(pullId, {
      source: 'usaspending', pull_type: `FY${fiscal_year} ${award_type}`,
      status: 'failed', recordsFetched, recordsCreated, recordsUpdated, durationMs,
      errorMessage: err.message
    });

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

// ── BACKFILL ENDPOINT ───────────────────────────────────────
app.post('/api/ingest/backfill', requireAdmin, async (req, res) => {
  try {
    const {
      start_year = 2020,
      end_year = 2025,
      award_types = ['contracts', 'grants'],
      max_pages_per_pull = 500
    } = req.body || {};

    const pulls = [];
    for (let year = start_year; year <= end_year; year++) {
      for (const awardType of award_types) {
        pulls.push({ fiscal_year: year, award_type: awardType });
      }
    }

    // Create all pull records up front
    const pullIds = [];
    for (const p of pulls) {
      const { data: pull, error: pullErr } = await supabase
        .from('data_pulls')
        .insert({
          source: 'usaspending',
          pull_type: 'backfill',
          status: 'queued',
          metadata: { fiscal_year: p.fiscal_year, award_type: p.award_type, max_pages: max_pages_per_pull }
        })
        .select('id')
        .single();
      if (pullErr) {
        console.error(`[Backfill] Failed to create pull for FY${p.fiscal_year} ${p.award_type}:`, pullErr.message);
        continue;
      }
      pullIds.push({ ...p, pullId: pull.id });
    }

    // Kick off sequential processing in background
    (async () => {
      for (const { fiscal_year, award_type, pullId } of pullIds) {
        try {
          console.log(`[Backfill] Starting FY${fiscal_year} ${award_type} (pull ${pullId})`);
          await supabase.from('data_pulls').update({ status: 'running' }).eq('id', pullId);
          await ingestUSASpendingWithPull(pullId, {
            fiscal_year,
            award_type,
            max_pages: max_pages_per_pull,
            limit_per_page: 100,
            page_delay_ms: 500,
            pull_type: 'Backfill'
          });
        } catch (err) {
          console.error(`[Backfill] FY${fiscal_year} ${award_type} failed:`, err.message);
        }
        // 500ms cooldown between pulls
        await new Promise(r => setTimeout(r, 500));
      }
      console.log(`[Backfill] All ${pullIds.length} pulls finished`);
    })().catch(err => console.error('[Backfill] Fatal error:', err.message));

    return res.json({
      ok: true,
      message: `Backfill started for ${pullIds.length} pulls`,
      pull_count: pullIds.length,
      pull_ids: pullIds.map(p => ({ pull_id: p.pullId, fiscal_year: p.fiscal_year, award_type: p.award_type }))
    });
  } catch (err) {
    console.error('Backfill endpoint error:', err);
    return res.status(500).json({ ok: false, error: 'Server error' });
  }
});

// ── RESUME BACKFILL ENDPOINT ────────────────────────────────
// Re-queues any backfill pulls stuck at "queued" after a worker restart.
app.post('/api/ingest/resume-backfill', requireAdmin, async (req, res) => {
  try {
    const { data: stuckPulls, error: qErr } = await supabase
      .from('data_pulls')
      .select('id, metadata')
      .eq('source', 'usaspending')
      .eq('pull_type', 'backfill')
      .eq('status', 'queued')
      .order('created_at', { ascending: true });

    if (qErr) return res.status(500).json({ ok: false, error: 'Database query failed' });
    if (!stuckPulls || stuckPulls.length === 0) {
      return res.json({ ok: true, message: 'No queued backfill pulls found', resumed: 0 });
    }

    const pullIds = stuckPulls.map(p => ({ pullId: p.id, ...p.metadata }));

    // Process sequentially in background
    (async () => {
      for (const { pullId, fiscal_year, award_type, max_pages } of pullIds) {
        try {
          console.log(`[Resume-Backfill] Starting pull ${pullId}: FY${fiscal_year} ${award_type}`);
          await supabase.from('data_pulls').update({ status: 'running' }).eq('id', pullId);
          await ingestUSASpendingWithPull(pullId, {
            fiscal_year,
            award_type,
            max_pages: max_pages || 500,
            limit_per_page: 100,
            page_delay_ms: 500,
            pull_type: 'Backfill'
          });
        } catch (err) {
          console.error(`[Resume-Backfill] Pull ${pullId} failed:`, err.message);
        }
        await new Promise(r => setTimeout(r, 500));
      }
      console.log(`[Resume-Backfill] All ${pullIds.length} resumed pulls finished`);
    })().catch(err => console.error('[Resume-Backfill] Fatal error:', err.message));

    return res.json({
      ok: true,
      message: `Resumed ${pullIds.length} queued backfill pulls`,
      resumed: pullIds.length,
      pull_ids: pullIds.map(p => ({ pull_id: p.pullId, fiscal_year: p.fiscal_year, award_type: p.award_type }))
    });
  } catch (err) {
    console.error('Resume-backfill endpoint error:', err);
    return res.status(500).json({ ok: false, error: 'Server error' });
  }
});

// ── CRON ENDPOINTS ──────────────────────────────────────────
function requireCronSecret(req, res, next) {
  const secret = req.query.secret || req.headers['x-cron-secret'] || '';
  if (!secret || secret !== process.env.CRON_SECRET) {
    return res.status(401).json({ ok: false, error: 'Unauthorized' });
  }
  next();
}

// Daily at 3 AM ET: incremental pull of current FY contracts + grants
app.get('/api/cron/daily', requireCronSecret, async (req, res) => {
  try {
    const now = new Date();
    const fy = now.getMonth() >= 9 ? now.getFullYear() + 1 : now.getFullYear();
    const awardTypes = ['contracts', 'grants'];
    const pullIds = [];

    for (const awardType of awardTypes) {
      const { data: pull, error: pullErr } = await supabase
        .from('data_pulls')
        .insert({
          source: 'usaspending',
          pull_type: 'daily_incremental',
          status: 'running',
          metadata: { fiscal_year: fy, award_type: awardType, max_pages: 10 }
        })
        .select('id')
        .single();
      if (pullErr) continue;
      pullIds.push(pull.id);

      ingestUSASpendingWithPull(pull.id, {
        fiscal_year: fy, award_type: awardType, max_pages: 10
      }).catch(err => console.error(`[Cron/Daily] ${awardType} failed:`, err.message));
    }

    return res.json({ ok: true, type: 'daily', fiscal_year: fy, pull_ids: pullIds });
  } catch (err) {
    console.error('Cron daily error:', err);
    return res.status(500).json({ ok: false, error: 'Server error' });
  }
});

// Weekly on Sunday at 2 AM ET: full refresh of current FY
app.get('/api/cron/weekly', requireCronSecret, async (req, res) => {
  try {
    const now = new Date();
    const fy = now.getMonth() >= 9 ? now.getFullYear() + 1 : now.getFullYear();
    const awardTypes = ['contracts', 'grants'];
    const pullIds = [];

    for (const awardType of awardTypes) {
      const { data: pull, error: pullErr } = await supabase
        .from('data_pulls')
        .insert({
          source: 'usaspending',
          pull_type: 'weekly_full',
          status: 'running',
          metadata: { fiscal_year: fy, award_type: awardType, max_pages: 200 }
        })
        .select('id')
        .single();
      if (pullErr) continue;
      pullIds.push(pull.id);

      ingestUSASpendingWithPull(pull.id, {
        fiscal_year: fy, award_type: awardType, max_pages: 200
      }).catch(err => console.error(`[Cron/Weekly] ${awardType} failed:`, err.message));
    }

    return res.json({ ok: true, type: 'weekly', fiscal_year: fy, pull_ids: pullIds });
  } catch (err) {
    console.error('Cron weekly error:', err);
    return res.status(500).json({ ok: false, error: 'Server error' });
  }
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

// ── OPENCORPORATES INGESTION ────────────────────────────────
async function ingestOpenCorporates(pullId) {
  const startTime = Date.now();
  let recordsFetched = 0;
  let recordsUpdated = 0;
  const MAX_REQUESTS = 190; // stay under 200/day free tier
  const DELAY_MS = 1200; // ~1.2s between requests to be safe

  console.log(`[OpenCorporates] Pull ${pullId} started`);

  try {
    // Get entities missing OpenCorporates data
    const { data: entities, error: qErr } = await supabase
      .from('entities')
      .select('id, name, incorporation_date, metadata')
      .or('incorporation_date.is.null,metadata->>opencorporates_id.is.null')
      .limit(MAX_REQUESTS);

    if (qErr) throw new Error(`Entity query failed: ${qErr.message}`);
    if (!entities || entities.length === 0) {
      console.log('[OpenCorporates] No entities need enrichment');
      await supabase.from('data_pulls').update({
        status: 'completed', completed_at: new Date().toISOString(),
        records_fetched: 0, records_updated: 0
      }).eq('id', pullId);
      await sendIngestionEmail(pullId, {
        source: 'opencorporates', pull_type: 'enrichment', status: 'completed',
        recordsFetched: 0, recordsCreated: 0, recordsUpdated: 0,
        durationMs: Date.now() - startTime
      });
      return { pullId, recordsFetched: 0, recordsUpdated: 0 };
    }

    let requestCount = 0;
    for (const entity of entities) {
      if (requestCount >= MAX_REQUESTS) {
        console.log(`[OpenCorporates] Hit rate limit ceiling (${MAX_REQUESTS}), stopping`);
        break;
      }

      try {
        const searchUrl = `https://api.opencorporates.com/v0.4/companies/search?q=${encodeURIComponent(entity.name)}&format=json`;
        const resp = await fetch(searchUrl);
        requestCount++;
        recordsFetched++;

        if (!resp.ok) {
          if (resp.status === 429) {
            console.log('[OpenCorporates] Rate limited, stopping');
            break;
          }
          console.warn(`[OpenCorporates] Search failed for "${entity.name}": ${resp.status}`);
          continue;
        }

        const data = await resp.json();
        const companies = data?.results?.companies || [];

        // Find best match: exact name match (case-insensitive)
        const match = companies.find(c => {
          const ocName = (c.company?.name || '').toLowerCase().trim();
          const entName = entity.name.toLowerCase().trim();
          return ocName === entName;
        });

        if (!match) continue;

        const co = match.company;
        const updates = {
          metadata: {
            ...(entity.metadata || {}),
            opencorporates_id: co.company_number,
            opencorporates_jurisdiction: co.jurisdiction_code,
            opencorporates_fetched_at: new Date().toISOString()
          },
          opencorporates_url: co.opencorporates_url || null,
          company_status: co.current_status || null,
          updated_at: new Date().toISOString()
        };

        if (co.incorporation_date && !entity.incorporation_date) {
          updates.incorporation_date = co.incorporation_date;
        }
        if (co.dissolution_date) {
          updates.dissolution_date = co.dissolution_date;
        }
        if (co.registered_address_in_full) {
          updates.registered_agent = co.registered_address_in_full;
        }

        // Fetch officers if we have a detail URL (costs 1 more request)
        if (co.jurisdiction_code && co.company_number && requestCount < MAX_REQUESTS) {
          try {
            const detailUrl = `https://api.opencorporates.com/v0.4/companies/${co.jurisdiction_code}/${co.company_number}?format=json`;
            const detResp = await fetch(detailUrl);
            requestCount++;
            if (detResp.ok) {
              const detData = await detResp.json();
              const officers = (detData?.results?.company?.officers || []).map(o => ({
                name: o.officer?.name,
                position: o.officer?.position,
                start_date: o.officer?.start_date,
                end_date: o.officer?.end_date
              }));
              if (officers.length > 0) {
                updates.officers = officers;
              }
              if (detData?.results?.company?.registered_agent) {
                updates.registered_agent = detData.results.company.registered_agent.name || updates.registered_agent;
              }
            }
          } catch (detErr) {
            console.warn(`[OpenCorporates] Detail fetch failed for ${co.company_number}:`, detErr.message);
          }
        }

        const { error: upErr } = await supabase
          .from('entities')
          .update(updates)
          .eq('id', entity.id);

        if (upErr) {
          console.warn(`[OpenCorporates] Update failed for entity ${entity.id}:`, upErr.message);
        } else {
          recordsUpdated++;
        }
      } catch (entityErr) {
        console.warn(`[OpenCorporates] Error processing "${entity.name}":`, entityErr.message);
      }

      await new Promise(r => setTimeout(r, DELAY_MS));
    }

    const durationMs = Date.now() - startTime;
    await supabase.from('data_pulls').update({
      status: 'completed', completed_at: new Date().toISOString(),
      records_fetched: recordsFetched, records_updated: recordsUpdated
    }).eq('id', pullId);

    console.log(`[OpenCorporates] Pull ${pullId} completed: fetched=${recordsFetched}, updated=${recordsUpdated}`);
    await sendIngestionEmail(pullId, {
      source: 'opencorporates', pull_type: 'enrichment', status: 'completed',
      recordsFetched, recordsCreated: 0, recordsUpdated, durationMs
    });

    return { pullId, recordsFetched, recordsUpdated };
  } catch (err) {
    const durationMs = Date.now() - startTime;
    console.error(`[OpenCorporates] Pull ${pullId} failed:`, err.message);
    await supabase.from('data_pulls').update({
      status: 'failed', completed_at: new Date().toISOString(),
      error_message: err.message, records_fetched: recordsFetched, records_updated: recordsUpdated
    }).eq('id', pullId);
    await sendIngestionEmail(pullId, {
      source: 'opencorporates', pull_type: 'enrichment', status: 'failed',
      recordsFetched, recordsCreated: 0, recordsUpdated, durationMs, errorMessage: err.message
    });
    throw err;
  }
}

app.post('/api/ingest/opencorporates', requireAdmin, async (req, res) => {
  try {
    const { data: pull, error: pullErr } = await supabase
      .from('data_pulls')
      .insert({
        source: 'opencorporates', pull_type: 'enrichment',
        status: 'running', metadata: { max_requests: 190 }
      })
      .select('id')
      .single();

    if (pullErr) return res.status(500).json({ ok: false, error: 'Failed to create pull record' });

    ingestOpenCorporates(pull.id).catch(err => {
      console.error(`[OpenCorporates] Async error for pull ${pull.id}:`, err.message);
    });

    return res.json({ ok: true, pull_id: pull.id, message: 'OpenCorporates enrichment started' });
  } catch (err) {
    console.error('OpenCorporates endpoint error:', err);
    return res.status(500).json({ ok: false, error: 'Server error' });
  }
});

// ── OFAC SDN INGESTION ──────────────────────────────────────
// Simple string similarity for fuzzy matching (Dice coefficient)
function diceCoefficient(a, b) {
  a = a.toLowerCase().trim();
  b = b.toLowerCase().trim();
  if (a === b) return 1;
  if (a.length < 2 || b.length < 2) return 0;
  const bigrams = new Map();
  for (let i = 0; i < a.length - 1; i++) {
    const bi = a.substring(i, i + 2);
    bigrams.set(bi, (bigrams.get(bi) || 0) + 1);
  }
  let matches = 0;
  for (let i = 0; i < b.length - 1; i++) {
    const bi = b.substring(i, i + 2);
    const count = bigrams.get(bi) || 0;
    if (count > 0) {
      bigrams.set(bi, count - 1);
      matches++;
    }
  }
  return (2.0 * matches) / (a.length - 1 + b.length - 1);
}

async function ingestOFAC(pullId) {
  const startTime = Date.now();
  let recordsFetched = 0;
  let recordsCreated = 0;
  let recordsUpdated = 0;
  let flagsCreated = 0;
  const MATCH_THRESHOLD = 0.85;

  console.log(`[OFAC] Pull ${pullId} started`);

  try {
    // Download consolidated CSV
    const csvUrl = 'https://www.treasury.gov/ofac/downloads/consolidated/cons_prim.csv';
    const resp = await fetch(csvUrl);
    if (!resp.ok) throw new Error(`OFAC CSV download failed: ${resp.status}`);

    const csvText = await resp.text();
    const lines = csvText.split('\n').filter(l => l.trim());
    // cons_prim.csv has no header row; columns:
    // 0: SDN_Name, 1: SDN_Type, 2: Program, 3: Title, 4: Call_Sign,
    // 5: Vess_Type, 6: Tonnage, 7: GRT, 8: Vess_Flag, 9: Vess_Owner, 10: Remarks

    console.log(`[OFAC] Downloaded ${lines.length} lines`);

    // Parse and upsert OFAC entries
    const ofacEntries = [];
    for (const line of lines) {
      // Simple CSV parse (handle quoted fields)
      const fields = [];
      let current = '';
      let inQuotes = false;
      for (let i = 0; i < line.length; i++) {
        const ch = line[i];
        if (ch === '"') { inQuotes = !inQuotes; continue; }
        if (ch === ',' && !inQuotes) { fields.push(current.trim()); current = ''; continue; }
        current += ch;
      }
      fields.push(current.trim());

      const sdnName = fields[0] || '';
      if (!sdnName) continue;

      ofacEntries.push({
        sdn_name: sdnName,
        sdn_type: fields[1] || null,
        program: fields[2] || null,
        title: fields[3] || null,
        remarks: fields[10] || null,
        source_url: csvUrl,
        raw_data: { call_sign: fields[4], vess_type: fields[5], tonnage: fields[6], grt: fields[7], vess_flag: fields[8], vess_owner: fields[9] }
      });
    }

    recordsFetched = ofacEntries.length;

    // Batch upsert into ofac_entries (clear and reload for simplicity)
    // Delete old entries first
    await supabase.from('ofac_entries').delete().neq('id', '00000000-0000-0000-0000-000000000000');

    // Insert in batches of 500
    for (let i = 0; i < ofacEntries.length; i += 500) {
      const batch = ofacEntries.slice(i, i + 500);
      const { error: insErr } = await supabase.from('ofac_entries').insert(batch);
      if (insErr) {
        console.warn(`[OFAC] Batch insert error at offset ${i}:`, insErr.message);
      } else {
        recordsCreated += batch.length;
      }
    }

    console.log(`[OFAC] Loaded ${recordsCreated} OFAC entries, starting cross-match`);

    // Cross-match against entities
    const { data: entities, error: entErr } = await supabase
      .from('entities')
      .select('id, name');
    if (entErr) throw new Error(`Entity query failed: ${entErr.message}`);

    // Build OFAC name lookup (lowercase)
    const ofacNames = ofacEntries.map(e => ({
      name: e.sdn_name,
      nameLower: e.sdn_name.toLowerCase().trim(),
      program: e.program,
      sdn_type: e.sdn_type
    }));

    for (const entity of (entities || [])) {
      const entNameLower = entity.name.toLowerCase().trim();

      for (const ofac of ofacNames) {
        const score = diceCoefficient(entNameLower, ofac.nameLower);
        if (score >= MATCH_THRESHOLD) {
          // Check if flag already exists
          const { data: existing } = await supabase
            .from('flags')
            .select('id')
            .eq('entity_id', entity.id)
            .eq('flag_type', 'debarment_match')
            .eq('citation_source', 'OFAC SDN List')
            .maybeSingle();

          if (!existing) {
            const { error: flagErr } = await supabase
              .from('flags')
              .insert({
                entity_id: entity.id,
                flag_type: 'debarment_match',
                severity: 'critical',
                score_contribution: 40,
                citation_source: 'OFAC SDN List',
                citation_url: 'https://www.treasury.gov/ofac/downloads/consolidated/cons_prim.csv',
                citation_detail: `Matched OFAC SDN entry "${ofac.name}" (${ofac.sdn_type || 'unknown type'}, program: ${ofac.program || 'N/A'}) with similarity score ${score.toFixed(3)}`,
                is_active: true
              });
            if (!flagErr) {
              flagsCreated++;
              console.log(`[OFAC] Flagged entity "${entity.name}" matching "${ofac.name}" (score: ${score.toFixed(3)})`);
            }
          }
          break; // one flag per entity is enough
        }
      }
    }

    const durationMs = Date.now() - startTime;
    await supabase.from('data_pulls').update({
      status: 'completed', completed_at: new Date().toISOString(),
      records_fetched: recordsFetched, records_created: recordsCreated,
      records_updated: recordsUpdated,
      metadata: { flags_created: flagsCreated }
    }).eq('id', pullId);

    console.log(`[OFAC] Pull ${pullId} completed: entries=${recordsCreated}, flags=${flagsCreated}`);
    await sendIngestionEmail(pullId, {
      source: 'ofac', pull_type: 'sdn_refresh', status: 'completed',
      recordsFetched, recordsCreated, recordsUpdated: flagsCreated, durationMs
    });

    return { pullId, recordsFetched, recordsCreated, flagsCreated };
  } catch (err) {
    const durationMs = Date.now() - startTime;
    console.error(`[OFAC] Pull ${pullId} failed:`, err.message);
    await supabase.from('data_pulls').update({
      status: 'failed', completed_at: new Date().toISOString(),
      error_message: err.message, records_fetched: recordsFetched,
      records_created: recordsCreated
    }).eq('id', pullId);
    await sendIngestionEmail(pullId, {
      source: 'ofac', pull_type: 'sdn_refresh', status: 'failed',
      recordsFetched, recordsCreated, recordsUpdated: 0, durationMs, errorMessage: err.message
    });
    throw err;
  }
}

app.post('/api/ingest/ofac', requireAdmin, async (req, res) => {
  try {
    const { data: pull, error: pullErr } = await supabase
      .from('data_pulls')
      .insert({
        source: 'ofac', pull_type: 'sdn_refresh',
        status: 'running', metadata: {}
      })
      .select('id')
      .single();

    if (pullErr) return res.status(500).json({ ok: false, error: 'Failed to create pull record' });

    ingestOFAC(pull.id).catch(err => {
      console.error(`[OFAC] Async error for pull ${pull.id}:`, err.message);
    });

    return res.json({ ok: true, pull_id: pull.id, message: 'OFAC SDN ingestion started' });
  } catch (err) {
    console.error('OFAC endpoint error:', err);
    return res.status(500).json({ ok: false, error: 'Server error' });
  }
});

// ── START ─────────────────────────────────────────────────────
app.listen(port, () => {
  console.log(`Arcane Tracer API running on port ${port}`);
});
