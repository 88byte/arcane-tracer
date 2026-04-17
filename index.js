require('dotenv').config();
const crypto  = require('crypto');
const fs      = require('fs');
const path    = require('path');
const https   = require('https');
const express  = require('express');
const cors     = require('cors');
const { Resend } = require('resend');
const { createClient } = require('@supabase/supabase-js');
const unzipper = require('unzipper');
const { parse: csvParseStream } = require('csv-parse');

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
      // Continue anyway, don't fail the user because of a DB issue
    }

    // ── NOTIFY YOU ────────────────────────────────────────────
    // Clean, formatted email delivered to your inbox immediately
    const { data: d1, error: e1 } = await resend.emails.send({
      from: 'Arcane Tracer <onboarding@resend.dev>',
      to: [process.env.OWNER_EMAIL],
      subject: `[Access Request] ${level}: ${name}`,
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
async function sendIngestionEmail(pullId, { source, pull_type, status, recordsFetched, recordsCreated, recordsUpdated, durationMs, errorMessage, apiCallsUsed, apiBudgetRemaining }) {
  try {
    const durationStr = durationMs
      ? `${Math.floor(durationMs / 60000)}m ${Math.floor((durationMs % 60000) / 1000)}s`
      : 'unknown';
    const statusEmoji = status === 'completed' ? 'OK' : 'FAILED';
    const subject = `[Arcane Tracer] Ingestion ${statusEmoji}: ${source} ${pull_type}`;
    // SAM.gov daily budget surfacing: only rendered when the caller
    // supplied apiCallsUsed (samgov, sam_exclusions, fapiis pulls).
    // Format: "X api_calls used, Y budget remaining for today".
    const budgetLine = (typeof apiCallsUsed === 'number')
      ? `SAM.gov Budget: ${apiCallsUsed} api_calls used${typeof apiBudgetRemaining === 'number' ? `, ${apiBudgetRemaining} budget remaining for today` : ''}`
      : null;
    const body = [
      `Pull ID: ${pullId}`,
      `Source: ${source}`,
      `Pull Type: ${pull_type}`,
      `Status: ${status}`,
      `Records Fetched: ${recordsFetched}`,
      `Records Created: ${recordsCreated}`,
      `Records Updated: ${recordsUpdated}`,
      `Duration: ${durationStr}`,
      budgetLine,
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

// ── RETRY HELPER ───────────────────────────────────────────
async function fetchWithRetry(url, fetchOptions, retries = 3) {
  const delays = [2000, 5000, 15000];
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const resp = await fetch(url, fetchOptions);
      if (!resp.ok) {
        const errText = await resp.text();
        throw new Error(`USASpending API error ${resp.status}: ${errText.slice(0, 200)}`);
      }
      return resp;
    } catch (err) {
      if (attempt < retries) {
        const delay = delays[attempt] || 15000;
        console.warn(`[USASpending] Fetch attempt ${attempt + 1} failed: ${err.message}. Retrying in ${delay}ms...`);
        await new Promise(r => setTimeout(r, delay));
      } else {
        console.error(`[USASpending] All ${retries + 1} fetch attempts failed: ${err.message}`);
        return null; // signal to skip this page
      }
    }
  }
  return null;
}

// Variant that uses an existing pull ID (for the async kickoff from the endpoint)
async function ingestUSASpendingWithPull(pullId, options = {}) {
  const {
    fiscal_year = new Date().getFullYear(),
    award_type = 'contracts',
    limit_per_page = 100,
    max_pages = 10,
    start_page = 1,
    page_delay_ms = 200
  } = options;

  const startTime = Date.now();
  let recordsFetched = 0;
  let recordsCreated = 0;
  let recordsUpdated = 0;
  const endPage = start_page + max_pages - 1;

  console.log(`[USASpending] Pull ${pullId} started: FY${fiscal_year} ${award_type}, pages ${start_page}-${endPage}`);

  try {
    const typeMap = {
      contracts: ['A', 'B', 'C', 'D'],
      grants: ['02', '03', '04', '05'],
      loans: ['07', '08'],
      direct_payments: ['06', '10'],
      other: ['09', '11']
    };
    const awardTypes = typeMap[award_type] || typeMap.contracts;

    for (let page = start_page; page <= endPage; page++) {
      // US federal FY N runs Oct 1 of (N-1) through Sep 30 of N.
      // e.g. FY2026 = 2025-10-01 to 2026-09-30.
      const body = {
        filters: {
          time_period: [{ start_date: `${fiscal_year - 1}-10-01`, end_date: `${fiscal_year}-09-30` }],
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

      const resp = await fetchWithRetry('https://api.usaspending.gov/api/v2/search/spending_by_award/', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body)
      });

      if (!resp) {
        console.warn(`[USASpending] Skipping page ${page} after all retries failed`);
        continue;
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

      if (page % 10 === 0 || page === endPage || results.length < limit_per_page) {
        console.log(`[USASpending] ${options.pull_type || 'Pull'} FY${fiscal_year} ${award_type}: page ${page}/${endPage}, ${recordsFetched} records so far`);
        // Checkpoint progress to DB every 10 pages
        await supabase.from('data_pulls').update({
          records_fetched: recordsFetched,
          records_created: recordsCreated,
          records_updated: recordsUpdated
        }).eq('id', pullId);
      }

      if (page < endPage && results.length > 0) {
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
      max_pages_per_pull = 100
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
// Re-queues any backfill pulls stuck at "queued" or "running" (0 records),
// and creates continuation pulls for "failed" ones starting from where they left off.
app.post('/api/ingest/resume-backfill', requireAdmin, async (req, res) => {
  try {
    // 1. Grab queued pulls (never started)
    const { data: queuedPulls, error: qErr } = await supabase
      .from('data_pulls')
      .select('id, metadata')
      .eq('source', 'usaspending')
      .eq('pull_type', 'backfill')
      .eq('status', 'queued')
      .order('created_at', { ascending: true });

    if (qErr) return res.status(500).json({ ok: false, error: 'Database query failed' });

    // 2. Grab failed pulls to create continuation pulls
    const { data: failedPulls, error: fErr } = await supabase
      .from('data_pulls')
      .select('id, metadata, records_fetched')
      .eq('source', 'usaspending')
      .eq('pull_type', 'backfill')
      .eq('status', 'failed')
      .order('created_at', { ascending: true });

    if (fErr) return res.status(500).json({ ok: false, error: 'Database query failed' });

    const resumeItems = [];

    // Add queued pulls directly
    for (const p of (queuedPulls || [])) {
      const maxPages = Math.min(p.metadata.max_pages || 500, 100);
      resumeItems.push({ pullId: p.id, isNew: false, ...p.metadata, max_pages: maxPages });
    }

    // For failed pulls, create new continuation pulls starting from where they left off
    for (const p of (failedPulls || [])) {
      const fetched = p.records_fetched || 0;
      const startPage = Math.floor(fetched / 100) + 1; // 100 = limit_per_page
      const originalMax = p.metadata.max_pages || 500;
      const remainingPages = Math.min(originalMax - startPage + 1, 100);
      if (remainingPages <= 0) continue; // already done

      const { data: newPull, error: npErr } = await supabase
        .from('data_pulls')
        .insert({
          source: 'usaspending',
          pull_type: 'backfill',
          status: 'queued',
          metadata: {
            fiscal_year: p.metadata.fiscal_year,
            award_type: p.metadata.award_type,
            max_pages: remainingPages,
            start_page: startPage,
            continued_from: p.id
          }
        })
        .select('id')
        .single();

      if (npErr) {
        console.error(`[Resume-Backfill] Failed to create continuation for ${p.id}:`, npErr.message);
        continue;
      }

      // Mark old failed pull as superseded
      await supabase.from('data_pulls').update({ status: 'superseded' }).eq('id', p.id);

      resumeItems.push({
        pullId: newPull.id,
        isNew: true,
        fiscal_year: p.metadata.fiscal_year,
        award_type: p.metadata.award_type,
        max_pages: remainingPages,
        start_page: startPage
      });
    }

    if (resumeItems.length === 0) {
      return res.json({ ok: true, message: 'No pulls to resume', resumed: 0 });
    }

    // Process sequentially in background
    (async () => {
      for (const item of resumeItems) {
        const { pullId, fiscal_year, award_type, max_pages, start_page } = item;
        try {
          console.log(`[Resume-Backfill] Starting pull ${pullId}: FY${fiscal_year} ${award_type}${start_page ? ` from page ${start_page}` : ''}`);
          await supabase.from('data_pulls').update({ status: 'running' }).eq('id', pullId);
          await ingestUSASpendingWithPull(pullId, {
            fiscal_year,
            award_type,
            max_pages: max_pages || 100,
            start_page: start_page || 1,
            limit_per_page: 100,
            page_delay_ms: 500,
            pull_type: 'Backfill'
          });
        } catch (err) {
          console.error(`[Resume-Backfill] Pull ${pullId} failed:`, err.message);
        }
        await new Promise(r => setTimeout(r, 500));
      }
      console.log(`[Resume-Backfill] All ${resumeItems.length} resumed pulls finished`);
    })().catch(err => console.error('[Resume-Backfill] Fatal error:', err.message));

    return res.json({
      ok: true,
      message: `Resumed ${resumeItems.length} backfill pulls`,
      resumed: resumeItems.length,
      pull_ids: resumeItems.map(p => ({
        pull_id: p.pullId,
        fiscal_year: p.fiscal_year,
        award_type: p.award_type,
        start_page: p.start_page || 1,
        is_continuation: p.isNew || false
      }))
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

// Daily at 3 AM ET: incremental pull of current FY contracts + grants,
// plus OpenCorporates enrichment, OFAC SDN refresh, and SAM.gov enrichment.
app.get('/api/cron/daily', requireCronSecret, async (req, res) => {
  try {
    const now = new Date();
    // US federal fiscal year N starts Oct 1 of (N-1).
    // In Oct-Dec we're in the NEXT calendar year's FY, so add 1.
    // Jan-Sep we're in the CURRENT calendar year's FY, so don't add.
    // The downstream ingestUSASpendingWithPull handles the Oct-1-to-Sep-30 window,
    // so fy here is just the FY label.
    const fy = now.getMonth() >= 9 ? now.getFullYear() + 1 : now.getFullYear();
    const awardTypes = ['contracts', 'grants'];
    const usaspendingPullIds = [];

    // 1. USASpending incremental pulls (contracts + grants)
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
      usaspendingPullIds.push(pull.id);

      ingestUSASpendingWithPull(pull.id, {
        fiscal_year: fy, award_type: awardType, max_pages: 10
      }).catch(err => console.error(`[Cron/Daily] ${awardType} failed:`, err.message));
    }

    // 2. OpenCorporates removed from daily cron on 2026-04-16.
    // Feature-flagged off pending commercial key (ODbL share-alike is
    // incompatible with a paid Professional product). Endpoint
    // /api/ingest/opencorporates is still reachable for manual runs but
    // will no-op unless OPENCORPORATES_ENABLED=true.

    // 3. OFAC SDN daily refresh
    let ofacPullId = null;
    {
      const { data: ofacPull, error: ofacErr } = await supabase
        .from('data_pulls')
        .insert({
          source: 'ofac',
          pull_type: 'daily_sdn_refresh',
          status: 'running',
          metadata: {}
        })
        .select('id')
        .single();
      if (!ofacErr && ofacPull) {
        ofacPullId = ofacPull.id;
        ingestOFAC(ofacPull.id).catch(err =>
          console.error('[Cron/Daily] OFAC failed:', err.message)
        );
      } else if (ofacErr) {
        console.error('[Cron/Daily] OFAC pull row create failed:', ofacErr.message);
      }
    }

    // 4. SAM.gov monthly Entity bulk extract (public V2 ZIP).
    // Source swapped from API to monthly bulk download on 2026-04-16.
    // The file publishes on the first Sunday of the month, so we fire
    // ingestSAMGov only on the 1st and 15th of each month. Firing twice
    // gives a safety net if the first attempt lands before the file is
    // posted.
    let samgovPullId = null;
    {
      const dayOfMonth = new Date().getUTCDate();
      if (dayOfMonth === 1 || dayOfMonth === 15) {
        const { data: samPull, error: samErr } = await supabase
          .from('data_pulls')
          .insert({
            source: 'samgov',
            pull_type: 'monthly_entity_extract',
            status: 'running',
            metadata: {}
          })
          .select('id')
          .single();
        if (!samErr && samPull) {
          samgovPullId = samPull.id;
          ingestSAMGov(samPull.id).catch(err =>
            console.error('[Cron/Daily] SAM.gov failed:', err.message)
          );
        } else if (samErr) {
          console.error('[Cron/Daily] SAM.gov pull row create failed:', samErr.message);
        }
      } else {
        console.log(`[Cron/Daily] Skipping SAM.gov monthly entity extract (day ${dayOfMonth}, fires only on day 1 and 15).`);
      }
    }

    // 5. SAM.gov Exclusions List daily refresh.
    // Source: daily bulk Public V2 Exclusions extract from sam.gov/data-services
    // (no API key, no login). Swapped from API on 2026-04-16.
    let samExclusionsPullId = null;
    {
      const { data: sePull, error: seErr } = await supabase
        .from('data_pulls')
        .insert({
          source: 'sam_exclusions',
          pull_type: 'daily_refresh',
          status: 'running',
          metadata: {}
        })
        .select('id')
        .single();
      if (!seErr && sePull) {
        samExclusionsPullId = sePull.id;
        ingestSAMExclusions(sePull.id).catch(err =>
          console.error('[Cron/Daily] SAM Exclusions failed:', err.message)
        );
      } else if (seErr) {
        console.error('[Cron/Daily] SAM Exclusions pull row create failed:', seErr.message);
      }
    }

    // 6. IRS 990 enrichment (ProPublica Nonprofit Explorer)
    let irs990PullId = null;
    {
      const { data: irsPull, error: irsErr } = await supabase
        .from('data_pulls')
        .insert({
          source: 'irs_990',
          pull_type: 'daily_enrichment',
          status: 'running',
          metadata: {}
        })
        .select('id')
        .single();
      if (!irsErr && irsPull) {
        irs990PullId = irsPull.id;
        ingestIRS990(irsPull.id).catch(err =>
          console.error('[Cron/Daily] IRS 990 failed:', err.message)
        );
      } else if (irsErr) {
        console.error('[Cron/Daily] IRS 990 pull row create failed:', irsErr.message);
      }
    }

    return res.json({
      ok: true,
      type: 'daily',
      fiscal_year: fy,
      pulls: {
        usaspending: usaspendingPullIds,
        ofac: ofacPullId,
        samgov: samgovPullId,
        sam_exclusions: samExclusionsPullId,
        irs_990: irs990PullId
      }
    });
  } catch (err) {
    console.error('Cron daily error:', err);
    return res.status(500).json({ ok: false, error: 'Server error' });
  }
});

// Weekly on Sunday at 2 AM ET: full refresh of current FY
app.get('/api/cron/weekly', requireCronSecret, async (req, res) => {
  try {
    const now = new Date();
    // US federal fiscal year N starts Oct 1 of (N-1).
    // In Oct-Dec we're in the NEXT calendar year's FY, so add 1.
    // Jan-Sep we're in the CURRENT calendar year's FY, so don't add.
    // The downstream ingestUSASpendingWithPull handles the Oct-1-to-Sep-30 window,
    // so fy here is just the FY label.
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

    // FAPIIS weekly refresh (federal contractor performance history).
    // As of 2026-04-16 FAPIIS is no longer a separately listed SAM.gov
    // data-services folder. FAPIIS-class signals (Responsibility/Qualification
    // proceedings) now arrive via the SAM.gov Entity extract, and debarments
    // via the SAM.gov Exclusions extract. The call below runs a no-op stub
    // that marks the pull completed with a skipped flag so cron history
    // stays consistent. See ingestFAPIIS for details.
    let fapiisPullId = null;
    {
      const { data: fapPull, error: fapErr } = await supabase
        .from('data_pulls')
        .insert({
          source: 'fapiis',
          pull_type: 'weekly_refresh',
          status: 'running',
          metadata: {}
        })
        .select('id')
        .single();
      if (!fapErr && fapPull) {
        fapiisPullId = fapPull.id;
        ingestFAPIIS(fapPull.id).catch(err =>
          console.error('[Cron/Weekly] FAPIIS failed:', err.message)
        );
      } else if (fapErr) {
        console.error('[Cron/Weekly] FAPIIS pull row create failed:', fapErr.message);
      }
    }

    return res.json({ ok: true, type: 'weekly', fiscal_year: fy, pull_ids: pullIds, fapiis: fapiisPullId });
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
// Feature-flagged off pending commercial key. Free tier is share-alike
// (ODbL) and incompatible with a paid Professional product. Do not enable
// without a non-share-alike commercial agreement.
async function ingestOpenCorporates(pullId) {
  if (process.env.OPENCORPORATES_ENABLED !== 'true') {
    console.log('OpenCorporates ingestion disabled by feature flag');
    await supabase.from('data_pulls').update({
      status: 'completed',
      completed_at: new Date().toISOString(),
      records_fetched: 0,
      records_updated: 0,
      metadata: { skipped: true, reason: 'feature_flag_off' }
    }).eq('id', pullId);
    return { skipped: true, reason: 'feature_flag_off' };
  }
  const startTime = Date.now();
  let recordsFetched = 0;
  let recordsUpdated = 0;
  const MAX_REQUESTS = 190; // stay under 200/day free tier
  const DELAY_MS = 1200; // ~1.2s between requests to be safe

  // Skip entities that were attempted (match or miss) in the last 30 days.
  // Without this, every daily run re-tries known misses and burns rate budget.
  const thirtyDaysAgoIso = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString();

  console.log(`[OpenCorporates] Pull ${pullId} started`);

  try {
    // Candidate entities: missing incorporation_date OR missing opencorporates_id.
    // Fetch a generous pool and filter attempted-recently in JS. PostgREST .or()
    // across jsonb key tests (is.null OR lt.<iso>) is finicky with mixed operators,
    // so we over-fetch candidates and filter in JS to keep the logic clear and safe.
    const FETCH_POOL = MAX_REQUESTS * 4;
    const { data: rawEntities, error: qErr } = await supabase
      .from('entities')
      .select('id, name, incorporation_date, metadata')
      .or('incorporation_date.is.null,metadata->>opencorporates_id.is.null')
      .limit(FETCH_POOL);

    if (qErr) throw new Error(`Entity query failed: ${qErr.message}`);

    // Filter out entities attempted in the last 30 days (miss cache).
    const entities = (rawEntities || []).filter(e => {
      const attempted = e.metadata?.opencorporates_attempted_at;
      if (!attempted) return true;
      return attempted < thirtyDaysAgoIso;
    }).slice(0, MAX_REQUESTS);
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

        if (!match) {
          // Record the miss so the next daily run skips this entity for 30 days.
          // Prevents rate-limit budget leak on known non-matches.
          const missMetadata = {
            ...(entity.metadata || {}),
            opencorporates_attempted_at: new Date().toISOString()
          };
          const { error: missErr } = await supabase
            .from('entities')
            .update({ metadata: missMetadata, updated_at: new Date().toISOString() })
            .eq('id', entity.id);
          if (missErr) {
            console.warn(`[OpenCorporates] Miss-mark failed for entity ${entity.id}:`, missErr.message);
          }
          continue;
        }

        const co = match.company;
        const updates = {
          metadata: {
            ...(entity.metadata || {}),
            opencorporates_id: co.company_number,
            opencorporates_jurisdiction: co.jurisdiction_code,
            opencorporates_fetched_at: new Date().toISOString(),
            opencorporates_attempted_at: new Date().toISOString()
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
// TODO: long-term fix is to move fuzzy matching into Postgres via the pg_trgm
// extension with a GIN index on entities.name_normalized (and a materialized
// ofac_entries.name_normalized). The first-letter bucketing below is a stopgap
// to avoid the O(N*M) blow-up on large tables.
//
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

    // Insert-then-prune pattern: never leave the table empty during refresh.
    // 1. Tag every new row with this run's batch_id (inside raw_data, no schema change).
    // 2. Insert all new rows first (table now contains old + new).
    // 3. Only after inserts succeed, delete rows NOT tagged with this batch_id.
    // Readers always see a complete snapshot. If inserts fail mid-batch,
    // old data survives.
    const thisBatchId = `batch_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
    for (const entry of ofacEntries) {
      entry.raw_data = { ...(entry.raw_data || {}), batch_id: thisBatchId };
    }

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

    // Prune previous-run rows only after new rows are in.
    if (recordsCreated > 0) {
      const { error: pruneErr } = await supabase
        .from('ofac_entries')
        .delete()
        .or(`raw_data->>batch_id.is.null,raw_data->>batch_id.neq.${thisBatchId}`);
      if (pruneErr) {
        console.warn('[OFAC] Prune of previous batch failed:', pruneErr.message);
      }
    } else {
      console.warn('[OFAC] No new rows inserted, skipping prune to preserve existing data');
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

    // First-letter bucket prefilter. At the 0.85 Dice threshold, matching names
    // share almost all bigrams, which means they almost always share their first
    // character. Bucketing by first letter drops search space by ~26x with no
    // meaningful loss of recall. This is a JS-side stopgap; see pg_trgm TODO above.
    const ofacByFirstChar = new Map();
    for (const ofac of ofacNames) {
      const key = ofac.nameLower.charAt(0);
      if (!key) continue;
      let bucket = ofacByFirstChar.get(key);
      if (!bucket) {
        bucket = [];
        ofacByFirstChar.set(key, bucket);
      }
      bucket.push(ofac);
    }

    for (const entity of (entities || [])) {
      const entNameLower = entity.name.toLowerCase().trim();
      const firstChar = entNameLower.charAt(0);
      if (!firstChar) continue;
      const candidates = ofacByFirstChar.get(firstChar) || [];

      for (const ofac of candidates) {
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

// ── SAM.GOV PUBLIC BULK EXTRACTS ────────────────────────────
// As of 2026-04-16 Arcane Tracer ingests SAM.gov via the public bulk
// extracts at https://sam.gov/data-services/ rather than the per-entity
// API. No API key, no rate limits, no SAM.gov user account required.
//
// Entity Registration / Public V2: monthly ZIP containing a CSV with the
// full public-tier entity view (legal business name, UEI, addresses,
// NAICS, registration status, business types, SAM exclusion status
// flag). Filename pattern SAM_PUBLIC_UTF-8_MONTHLY_V2_YYYYMMDD.ZIP,
// published on or around the first Sunday of each month.
//
// Exclusions / Public V2: daily ZIP containing a CSV of current
// debarments. Filename pattern SAM_Exclusions_Public_Extract_V2_YYDDD.ZIP
// where YY is the 2-digit year and DDD is the 3-digit day of year.
//
// Download host: https://s3.amazonaws.com/falextracts/ (public S3
// bucket fronted by sam.gov/data-services).
// TODO: if the falextracts S3 bucket hostname changes, fall back to
// scraping the sam.gov/data-services HTML listing to pull the current
// filename, then downloading via the SAM.gov file-extract proxy at
// https://sam.gov/api/prod/fileextractservices/v1/api/download/...
//
// Attribution: pre-2022 entity records originated in the legacy CAGE
// system populated by Dun and Bradstreet. For any record with a
// registrationDate before 2022-04-04, the frontend displays the D&B
// courtesy attribution on the methodology page.
//
// Feature flag: SAM_GOV_ENABLED (default true). Gates both ingestSAMGov
// and ingestSAMExclusions. Set to 'false' on Railway to bypass.

// Helpers shared by both SAM bulk ingestors.
const SAM_ENTITY_BUCKET_PREFIX = 'https://s3.amazonaws.com/falextracts/Entity%20Registration/Public%20V2/';
const SAM_EXCLUSIONS_BUCKET_PREFIX = 'https://s3.amazonaws.com/falextracts/Exclusions/Public%20V2/';

// Stream-download a URL to a local file path, following up to 5 redirects.
function downloadToFile(url, destPath, redirectsLeft = 5) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, (resp) => {
      if (resp.statusCode >= 300 && resp.statusCode < 400 && resp.headers.location) {
        resp.resume();
        if (redirectsLeft <= 0) return reject(new Error(`Too many redirects for ${url}`));
        return resolve(downloadToFile(resp.headers.location, destPath, redirectsLeft - 1));
      }
      if (resp.statusCode !== 200) {
        resp.resume();
        return reject(new Error(`Download failed for ${url}: HTTP ${resp.statusCode}`));
      }
      const out = fs.createWriteStream(destPath);
      resp.pipe(out);
      out.on('finish', () => out.close(() => resolve(destPath)));
      out.on('error', (err) => reject(err));
      resp.on('error', (err) => reject(err));
    });
    req.on('error', (err) => reject(err));
  });
}

// HEAD request to confirm a URL exists (200). Follows up to 5 redirects.
function urlExists(url, redirectsLeft = 5) {
  return new Promise((resolve) => {
    const u = new URL(url);
    const req = https.request({
      hostname: u.hostname,
      path: u.pathname + u.search,
      method: 'HEAD'
    }, (resp) => {
      if (resp.statusCode >= 300 && resp.statusCode < 400 && resp.headers.location) {
        if (redirectsLeft <= 0) return resolve(false);
        return resolve(urlExists(resp.headers.location, redirectsLeft - 1));
      }
      resolve(resp.statusCode === 200);
    });
    req.on('error', () => resolve(false));
    req.end();
  });
}

// Best-effort safe temp-file cleanup. Swallow errors so cleanup never
// masks the original error path.
function safeUnlink(p) {
  if (!p) return;
  try {
    if (fs.existsSync(p)) fs.unlinkSync(p);
  } catch (e) {
    console.warn(`[SAMBulk] Cleanup failed for ${p}: ${e.message}`);
  }
}

// Compute the first Sunday of a given UTC (year, monthZeroIndexed).
function firstSundayOfMonth(year, monthZeroIndexed) {
  const d = new Date(Date.UTC(year, monthZeroIndexed, 1));
  const dow = d.getUTCDay(); // 0 = Sunday
  const dayOfMonth = dow === 0 ? 1 : 1 + (7 - dow);
  return new Date(Date.UTC(year, monthZeroIndexed, dayOfMonth));
}

function formatYyyymmdd(date) {
  const y = date.getUTCFullYear();
  const m = String(date.getUTCMonth() + 1).padStart(2, '0');
  const d = String(date.getUTCDate()).padStart(2, '0');
  return `${y}${m}${d}`;
}

// Build the list of candidate Entity monthly filenames to try, most
// likely first. Order: first Sunday of current month, first Sunday of
// prior month, 1st of current month.
function candidateEntityFilenames() {
  const now = new Date();
  const year = now.getUTCFullYear();
  const month = now.getUTCMonth();
  const firstSunThisMonth = firstSundayOfMonth(year, month);
  const firstSunPriorMonth = firstSundayOfMonth(
    month === 0 ? year - 1 : year,
    month === 0 ? 11 : month - 1
  );
  const firstOfMonth = new Date(Date.UTC(year, month, 1));
  const dates = [firstSunThisMonth, firstSunPriorMonth, firstOfMonth];
  return dates.map(d => `SAM_PUBLIC_UTF-8_MONTHLY_V2_${formatYyyymmdd(d)}.ZIP`);
}

// Build the list of candidate Exclusions daily filenames to try, most
// likely first. Order: today, yesterday, day-before-yesterday.
function candidateExclusionsFilenames() {
  const filenames = [];
  for (let offset = 0; offset < 3; offset++) {
    const d = new Date();
    d.setUTCDate(d.getUTCDate() - offset);
    const yy = String(d.getUTCFullYear()).slice(-2);
    const startOfYear = Date.UTC(d.getUTCFullYear(), 0, 1);
    const dayOfYear = Math.floor((d.getTime() - startOfYear) / (24 * 60 * 60 * 1000)) + 1;
    const ddd = String(dayOfYear).padStart(3, '0');
    filenames.push(`SAM_Exclusions_Public_Extract_V2_${yy}${ddd}.ZIP`);
  }
  return filenames;
}

// Try each candidate filename. Returns { filename, url } of the first
// one that HEADs 200, or null if none of them exist.
async function resolveBulkFile(prefix, candidates) {
  for (const filename of candidates) {
    const url = prefix + filename;
    if (await urlExists(url)) {
      return { filename, url };
    }
  }
  return null;
}

async function ingestSAMGov(pullId) {
  // Feature flag: default on. Set SAM_GOV_ENABLED=false on Railway to bypass.
  if (process.env.SAM_GOV_ENABLED === 'false') {
    console.log('[SAMGov] Ingestion disabled by feature flag (SAM_GOV_ENABLED=false)');
    await supabase.from('data_pulls').update({
      status: 'completed',
      completed_at: new Date().toISOString(),
      records_fetched: 0,
      records_updated: 0,
      metadata: { skipped: true, reason: 'feature_flag_off' }
    }).eq('id', pullId);
    return { pullId, skipped: true, reason: 'feature_flag_off' };
  }

  const startTime = Date.now();
  let recordsFetched = 0;
  let recordsCreated = 0;
  let recordsUpdated = 0;
  let tempZipPath = null;

  console.log(`[SAMGov] Pull ${pullId} started (monthly bulk extract mode)`);

  try {
    // 1. Resolve the latest monthly filename.
    const candidates = candidateEntityFilenames();
    console.log(`[SAMGov] Candidate filenames: ${candidates.join(', ')}`);
    const resolved = await resolveBulkFile(SAM_ENTITY_BUCKET_PREFIX, candidates);
    if (!resolved) {
      throw new Error(`No SAM Entity monthly bulk file found. Tried: ${candidates.join(', ')}`);
    }
    console.log(`[SAMGov] Using bulk file: ${resolved.filename}`);

    await supabase.from('data_pulls').update({
      metadata: { source_file: resolved.filename, source_url: resolved.url }
    }).eq('id', pullId);

    // 2. Stream-download the ZIP to /tmp.
    tempZipPath = path.join('/tmp', `sam-entity-${Date.now()}.zip`);
    await downloadToFile(resolved.url, tempZipPath);
    const zipSizeBytes = fs.statSync(tempZipPath).size;
    console.log(`[SAMGov] Downloaded ${zipSizeBytes} bytes to ${tempZipPath}`);

    // 3 + 4. Stream-extract the first CSV from the ZIP through csv-parse.
    // Upsert in batches of 500. Match on UEI.
    const BATCH_SIZE = 500;
    const nowIso = new Date().toISOString();
    let batch = [];

    const flushBatch = async (rows) => {
      if (!rows.length) return;
      // Look up which UEIs already exist so we can split into
      // create vs update counters. Supabase upsert with onConflict
      // handles the write path in a single call.
      const ueis = rows.map(r => r.uei).filter(Boolean);
      let existingSet = new Set();
      if (ueis.length) {
        const { data: existing, error: selErr } = await supabase
          .from('entities')
          .select('uei')
          .in('uei', ueis);
        if (selErr) {
          console.warn(`[SAMGov] Existing-UEI lookup failed: ${selErr.message}`);
        } else {
          existingSet = new Set((existing || []).map(e => e.uei));
        }
      }
      const { error: upErr } = await supabase
        .from('entities')
        .upsert(rows, { onConflict: 'uei' });
      if (upErr) {
        console.warn(`[SAMGov] Batch upsert failed: ${upErr.message}`);
        return;
      }
      for (const r of rows) {
        if (existingSet.has(r.uei)) recordsUpdated++;
        else recordsCreated++;
      }
    };

    await new Promise((resolve, reject) => {
      const zipStream = fs.createReadStream(tempZipPath).pipe(unzipper.Parse());
      let csvPiped = false;

      zipStream.on('entry', (entry) => {
        const entryName = entry.path;
        if (!csvPiped && /\.csv$/i.test(entryName)) {
          csvPiped = true;
          console.log(`[SAMGov] Parsing CSV entry: ${entryName}`);
          const parser = entry.pipe(csvParseStream({
            columns: true,
            skip_empty_lines: true,
            relax_column_count: true,
            trim: true
          }));

          parser.on('data', async (row) => {
            recordsFetched++;
            const uei = row['UEI'] || row['ueiSAM'] || row['SAM UEI'] || row['uei'] || null;
            if (!uei) return;

            // Helper to pull a value by trying several possible column
            // name variants. SAM's monthly extract has historically
            // shifted column labels. Supporting a handful of names keeps
            // us resilient.
            const pick = (...keys) => {
              for (const k of keys) {
                if (row[k] != null && String(row[k]).trim() !== '') return String(row[k]).trim();
              }
              return null;
            };

            const legalName = pick('Legal Business Name', 'legalBusinessName', 'LEGAL_BUSINESS_NAME');
            const physicalAddress = {
              line1: pick('Physical Address Line 1', 'physicalAddressLine1'),
              line2: pick('Physical Address Line 2', 'physicalAddressLine2'),
              city: pick('Physical Address City', 'physicalAddressCity', 'City'),
              state: pick('Physical Address Province or State', 'physicalAddressStateOrProvinceCode', 'State'),
              zip: pick('Physical Address Zip/Postal Code', 'physicalAddressZipPostalCode', 'ZIP'),
              country: pick('Physical Address Country Code', 'physicalAddressCountryCode', 'Country')
            };
            const mailingAddress = {
              line1: pick('Mailing Address Line 1', 'mailingAddressLine1'),
              line2: pick('Mailing Address Line 2', 'mailingAddressLine2'),
              city: pick('Mailing Address City', 'mailingAddressCity'),
              state: pick('Mailing Address Province or State', 'mailingAddressStateOrProvinceCode'),
              zip: pick('Mailing Address Zip/Postal Code', 'mailingAddressZipPostalCode'),
              country: pick('Mailing Address Country Code', 'mailingAddressCountryCode')
            };
            const ein = pick('Taxpayer Identification Number', 'taxpayerIdentificationNumber', 'TIN');
            const naicsPrimary = pick('Primary NAICS', 'primaryNaics', 'NAICS Code');
            const registrationStatus = pick('Registration Status', 'registrationStatus');
            const exclusionFlagRaw = pick('SAM Exclusion Status Flag', 'samExclusionStatusFlag', 'Exclusion Status Flag');
            const exclusionFlag = exclusionFlagRaw == null ? null : /^(y|yes|true|1)$/i.test(exclusionFlagRaw);
            const registrationDate = pick('Registration Date', 'registrationDate', 'Initial Registration Date');
            const expirationDate = pick('Expiration Date', 'expirationDate', 'Registration Expiration Date');
            const state = physicalAddress.state || pick('State');
            const country = physicalAddress.country || pick('Country', 'Country Code') || 'US';

            batch.push({
              name: legalName || '(unknown)',
              entity_type: 'contractor',
              uei,
              state,
              country,
              source: 'samgov',
              source_id: uei,
              address: { physical: physicalAddress, mailing: mailingAddress },
              ein: ein || null,
              naics_primary: naicsPrimary || null,
              sam_registration_status: registrationStatus || null,
              sam_exclusion_flag: exclusionFlag,
              sam_registration_date: registrationDate || null,
              sam_expiration_date: expirationDate || null,
              metadata: {
                sam_fetched_at: nowIso,
                sam_source_file: resolved.filename
              },
              updated_at: nowIso
            });

            if (batch.length >= BATCH_SIZE) {
              parser.pause();
              const toFlush = batch;
              batch = [];
              try {
                await flushBatch(toFlush);
              } catch (e) {
                console.warn(`[SAMGov] Flush error: ${e.message}`);
              }
              parser.resume();
            }
          });

          parser.on('end', async () => {
            try {
              if (batch.length) {
                const toFlush = batch;
                batch = [];
                await flushBatch(toFlush);
              }
              resolve();
            } catch (e) {
              reject(e);
            }
          });

          parser.on('error', (err) => reject(err));
        } else {
          entry.autodrain();
        }
      });

      zipStream.on('close', () => {
        if (!csvPiped) reject(new Error('No CSV entry found in SAM Entity ZIP'));
      });
      zipStream.on('error', (err) => reject(err));
    });

    // 6. Cleanup temp file.
    safeUnlink(tempZipPath);
    tempZipPath = null;

    const durationMs = Date.now() - startTime;
    await supabase.from('data_pulls').update({
      status: 'completed',
      completed_at: new Date().toISOString(),
      records_fetched: recordsFetched,
      records_created: recordsCreated,
      records_updated: recordsUpdated,
      metadata: {
        source_file: resolved.filename,
        source_url: resolved.url,
        zip_size_bytes: zipSizeBytes
      }
    }).eq('id', pullId);

    console.log(`[SAMGov] Pull ${pullId} completed: fetched=${recordsFetched}, created=${recordsCreated}, updated=${recordsUpdated}`);
    await sendIngestionEmail(pullId, {
      source: 'samgov', pull_type: 'monthly_entity_extract', status: 'completed',
      recordsFetched, recordsCreated, recordsUpdated, durationMs
    });

    return { pullId, recordsFetched, recordsCreated, recordsUpdated };
  } catch (err) {
    safeUnlink(tempZipPath);
    const durationMs = Date.now() - startTime;
    console.error(`[SAMGov] Pull ${pullId} failed:`, err.message);
    await supabase.from('data_pulls').update({
      status: 'failed', completed_at: new Date().toISOString(),
      error_message: err.message,
      records_fetched: recordsFetched,
      records_created: recordsCreated,
      records_updated: recordsUpdated
    }).eq('id', pullId);
    await sendIngestionEmail(pullId, {
      source: 'samgov', pull_type: 'monthly_entity_extract', status: 'failed',
      recordsFetched, recordsCreated, recordsUpdated, durationMs, errorMessage: err.message
    });
    throw err;
  }
}

app.post('/api/ingest/samgov', requireAdmin, async (req, res) => {
  try {
    const { data: pull, error: pullErr } = await supabase
      .from('data_pulls')
      .insert({
        source: 'samgov', pull_type: 'monthly_entity_extract',
        status: 'running', metadata: {}
      })
      .select('id')
      .single();

    if (pullErr) return res.status(500).json({ ok: false, error: 'Failed to create pull record' });

    ingestSAMGov(pull.id).catch(err => {
      console.error(`[SAMGov] Async error for pull ${pull.id}:`, err.message);
    });

    return res.json({ ok: true, pull_id: pull.id, message: 'SAM.gov monthly entity extract ingestion started' });
  } catch (err) {
    console.error('SAMGov endpoint error:', err);
    return res.status(500).json({ ok: false, error: 'Server error' });
  }
});

// ── SAM.GOV EXCLUSIONS LIST (DAILY BULK EXTRACT) ────────────
// Authoritative federal debarment list (entities and individuals excluded
// from federal contracts, grants, and assistance). Public dataset, no
// commercial restrictions. Covers exclusions from all federal agencies
// (GSA, DoD, HHS OIG, State, USDA, etc.).
//
// Source: daily Public V2 bulk ZIP extract from sam.gov/data-services,
// served from https://s3.amazonaws.com/falextracts/Exclusions/Public%20V2/.
// Full replacement each run, protected by the insert-then-prune pattern
// borrowed from ingestOFAC so readers never see an empty table mid-refresh.
async function ingestSAMExclusions(pullId) {
  if (process.env.SAM_GOV_ENABLED === 'false') {
    console.log('[SAMExclusions] Ingestion disabled by feature flag (SAM_GOV_ENABLED=false)');
    await supabase.from('data_pulls').update({
      status: 'completed',
      completed_at: new Date().toISOString(),
      records_fetched: 0,
      records_created: 0,
      records_updated: 0,
      metadata: { skipped: true, reason: 'feature_flag_off' }
    }).eq('id', pullId);
    return { pullId, skipped: true, reason: 'feature_flag_off' };
  }

  const startTime = Date.now();
  let recordsFetched = 0;
  let recordsCreated = 0;
  let recordsUpdated = 0;
  let tempZipPath = null;

  console.log(`[SAMExclusions] Pull ${pullId} started (daily bulk extract mode)`);

  try {
    // 1. Resolve today's filename, falling back up to 2 days.
    const candidates = candidateExclusionsFilenames();
    console.log(`[SAMExclusions] Candidate filenames: ${candidates.join(', ')}`);
    const resolved = await resolveBulkFile(SAM_EXCLUSIONS_BUCKET_PREFIX, candidates);
    if (!resolved) {
      throw new Error(`No SAM Exclusions daily bulk file found. Tried: ${candidates.join(', ')}`);
    }
    console.log(`[SAMExclusions] Using bulk file: ${resolved.filename}`);

    await supabase.from('data_pulls').update({
      metadata: { source_file: resolved.filename, source_url: resolved.url }
    }).eq('id', pullId);

    // 2. Stream-download.
    tempZipPath = path.join('/tmp', `sam-exclusions-${Date.now()}.zip`);
    await downloadToFile(resolved.url, tempZipPath);
    const zipSizeBytes = fs.statSync(tempZipPath).size;
    console.log(`[SAMExclusions] Downloaded ${zipSizeBytes} bytes to ${tempZipPath}`);

    // 3. Stream-extract CSV and collect rows tagged with this run's batch_id.
    const thisBatchId = `batch_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
    const rows = [];

    await new Promise((resolve, reject) => {
      const zipStream = fs.createReadStream(tempZipPath).pipe(unzipper.Parse());
      let csvPiped = false;

      zipStream.on('entry', (entry) => {
        const entryName = entry.path;
        if (!csvPiped && /\.csv$/i.test(entryName)) {
          csvPiped = true;
          console.log(`[SAMExclusions] Parsing CSV entry: ${entryName}`);
          const parser = entry.pipe(csvParseStream({
            columns: true,
            skip_empty_lines: true,
            relax_column_count: true,
            trim: true
          }));

          parser.on('data', (row) => {
            recordsFetched++;
            const pick = (...keys) => {
              for (const k of keys) {
                if (row[k] != null && String(row[k]).trim() !== '') return String(row[k]).trim();
              }
              return null;
            };
            const exclusionId = pick('Exclusion ID', 'exclusionId', 'Classification', 'SAM Number', 'exclusionNumber');
            if (!exclusionId) return;

            const address = {
              line1: pick('Address 1', 'addressLine1'),
              line2: pick('Address 2', 'addressLine2'),
              city: pick('City'),
              state: pick('State / Province', 'State'),
              zip: pick('ZIP Code', 'Zip', 'zip'),
              country: pick('Country')
            };

            rows.push({
              exclusion_id: String(exclusionId),
              name: pick('Name', 'exclusionName', 'Classification Name') || null,
              dba_name: pick('DBA', 'dbaName') || null,
              address,
              exclusion_type: pick('Exclusion Type', 'exclusionType'),
              exclusion_program: pick('Exclusion Program', 'exclusionProgram'),
              excluding_agency: pick('Excluding Agency', 'excludingAgencyName', 'Agency'),
              active_date: pick('Active Date', 'activeDate') || null,
              termination_date: pick('Termination Date', 'terminationDate') || null,
              raw_data: { ...row, batch_id: thisBatchId },
              updated_at: new Date().toISOString()
            });
          });

          parser.on('end', () => resolve());
          parser.on('error', (err) => reject(err));
        } else {
          entry.autodrain();
        }
      });

      zipStream.on('close', () => {
        if (!csvPiped) reject(new Error('No CSV entry found in SAM Exclusions ZIP'));
      });
      zipStream.on('error', (err) => reject(err));
    });

    // 4. Insert-then-prune in batches of 500.
    for (let i = 0; i < rows.length; i += 500) {
      const batch = rows.slice(i, i + 500);
      const { error: insErr } = await supabase
        .from('sam_exclusions')
        .upsert(batch, { onConflict: 'exclusion_id' });
      if (insErr) {
        console.warn(`[SAMExclusions] Batch upsert at offset ${i} failed: ${insErr.message}`);
      } else {
        recordsCreated += batch.length;
      }
    }

    // Prune previous-run rows only after new rows are in. Rows not tagged
    // with this batch_id in raw_data are stale and can be removed.
    if (recordsCreated > 0) {
      const { error: pruneErr } = await supabase
        .from('sam_exclusions')
        .delete()
        .or(`raw_data->>batch_id.is.null,raw_data->>batch_id.neq.${thisBatchId}`);
      if (pruneErr) {
        console.warn('[SAMExclusions] Prune of previous batch failed:', pruneErr.message);
      }
    } else {
      console.warn('[SAMExclusions] No new rows inserted, skipping prune to preserve existing data');
    }

    safeUnlink(tempZipPath);
    tempZipPath = null;

    const durationMs = Date.now() - startTime;
    await supabase.from('data_pulls').update({
      status: 'completed',
      completed_at: new Date().toISOString(),
      records_fetched: recordsFetched,
      records_created: recordsCreated,
      records_updated: recordsUpdated,
      metadata: {
        source_file: resolved.filename,
        source_url: resolved.url,
        zip_size_bytes: zipSizeBytes,
        batch_id: thisBatchId
      }
    }).eq('id', pullId);

    console.log(`[SAMExclusions] Pull ${pullId} completed: fetched=${recordsFetched}, created=${recordsCreated}, updated=${recordsUpdated}`);
    await sendIngestionEmail(pullId, {
      source: 'sam_exclusions', pull_type: 'daily_refresh', status: 'completed',
      recordsFetched, recordsCreated, recordsUpdated, durationMs
    });

    return { pullId, recordsFetched, recordsCreated, recordsUpdated };
  } catch (err) {
    safeUnlink(tempZipPath);
    const durationMs = Date.now() - startTime;
    console.error(`[SAMExclusions] Pull ${pullId} failed:`, err.message);
    await supabase.from('data_pulls').update({
      status: 'failed', completed_at: new Date().toISOString(),
      error_message: err.message,
      records_fetched: recordsFetched,
      records_created: recordsCreated,
      records_updated: recordsUpdated
    }).eq('id', pullId);
    await sendIngestionEmail(pullId, {
      source: 'sam_exclusions', pull_type: 'daily_refresh', status: 'failed',
      recordsFetched, recordsCreated, recordsUpdated, durationMs, errorMessage: err.message
    });
    throw err;
  }
}

app.post('/api/ingest/samexclusions', requireAdmin, async (req, res) => {
  try {
    const { data: pull, error: pullErr } = await supabase
      .from('data_pulls')
      .insert({
        source: 'sam_exclusions', pull_type: 'daily_refresh',
        status: 'running', metadata: {}
      })
      .select('id')
      .single();

    if (pullErr) return res.status(500).json({ ok: false, error: 'Failed to create pull record' });

    ingestSAMExclusions(pull.id).catch(err => {
      console.error(`[SAMExclusions] Async error for pull ${pull.id}:`, err.message);
    });

    return res.json({ ok: true, pull_id: pull.id, message: 'SAM Exclusions ingestion started' });
  } catch (err) {
    console.error('SAMExclusions endpoint error:', err);
    return res.status(500).json({ ok: false, error: 'Server error' });
  }
});

// ── IRS 990 ENRICHMENT (VIA PROPUBLICA) ─────────────────────
// ProPublica Nonprofit Explorer API exposes the IRS Form 990 dataset.
// Free, public, no commercial restriction, no API key required.
// Docs: https://projects.propublica.org/nonprofits/api
//
// We enrich entities that have an EIN (tax_id populated) with officer
// names, compensation, filing status, and BMF revocation signals.
// Officer names land in entity_officers; top-level filing signals land
// in entities.metadata.
//
// Rate: ProPublica does not publish a strict limit but courtesy pacing
// is required. We sleep 600ms between calls and cap per-run volume.
//
// TODO: improve nonprofit detection heuristic. Today we iterate entities
// that already have an EIN-like tax_id. Longer-term we should also scan
// names containing nonprofit signals (Foundation, Fund, Society, Inc
// with 501c3 context) and attempt an EIN lookup.
async function ingestIRS990(pullId) {
  const startTime = Date.now();
  let recordsFetched = 0;
  let recordsUpdated = 0;
  const MAX_REQUESTS = 300;
  const DELAY_MS = 600;

  // Skip entities attempted (match or miss) in the last 30 days.
  // Mirrors the opencorporates_attempted_at miss-cache pattern.
  const thirtyDaysAgoIso = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString();

  console.log(`[IRS990] Pull ${pullId} started`);

  try {
    // Candidate entities: have a tax_id that looks like an EIN and have not
    // been attempted in the last 30 days. We over-fetch and filter in JS
    // because PostgREST .or() with jsonb timestamp comparison is finicky.
    const { data: rawEntities, error: qErr } = await supabase
      .from('entities')
      .select('id, name, tax_id, metadata')
      .not('tax_id', 'is', null)
      .limit(MAX_REQUESTS * 4);

    if (qErr) throw new Error(`Entity query failed: ${qErr.message}`);

    const candidates = (rawEntities || []).filter(e => {
      const attempted = e.metadata?.irs990_attempted_at;
      if (!attempted) return true;
      return attempted < thirtyDaysAgoIso;
    }).slice(0, MAX_REQUESTS);

    console.log(`[IRS990] ${candidates.length} entities eligible for IRS 990 enrichment`);

    let requestCount = 0;
    for (const entity of candidates) {
      if (requestCount >= MAX_REQUESTS) break;

      // Normalize EIN: ProPublica accepts digits only (strip dashes).
      const ein = String(entity.tax_id || '').replace(/[^0-9]/g, '');
      if (ein.length !== 9) {
        // Not an EIN shape; mark attempted so we do not re-check for 30 days.
        const missMeta = {
          ...(entity.metadata || {}),
          irs990_attempted_at: new Date().toISOString(),
          irs990_skip_reason: 'tax_id_not_ein_shape'
        };
        await supabase.from('entities')
          .update({ metadata: missMeta, updated_at: new Date().toISOString() })
          .eq('id', entity.id);
        continue;
      }

      try {
        const url = `https://projects.propublica.org/nonprofits/api/v2/organizations/${ein}.json`;
        const resp = await fetch(url);
        requestCount++;
        recordsFetched++;

        if (resp.status === 404) {
          // Not in the Nonprofit Explorer dataset. Mark attempted.
          const missMeta = {
            ...(entity.metadata || {}),
            irs990_attempted_at: new Date().toISOString(),
            irs990_skip_reason: 'not_in_propublica'
          };
          await supabase.from('entities')
            .update({ metadata: missMeta, updated_at: new Date().toISOString() })
            .eq('id', entity.id);
          await new Promise(r => setTimeout(r, DELAY_MS));
          continue;
        }

        if (!resp.ok) {
          console.warn(`[IRS990] ${ein} returned ${resp.status}`);
          await new Promise(r => setTimeout(r, DELAY_MS));
          continue;
        }

        const data = await resp.json();
        const org = data?.organization || {};
        const filings = data?.filings_with_data || [];
        const latestFiling = filings[0] || null;

        // Revocation signal from IRS BMF (organization.revocation_date).
        const revocationDate = org.revocation_date || null;
        const subseccd = org.subseccd || null; // 501(c) subsection

        // Officers: typically on each filing record. Extract from latest.
        // The ProPublica response nests officers differently depending on
        // filing year. Handle both shapes defensively.
        const officers = [];
        const filingForOfficers = latestFiling || {};
        const rawOfficers = filingForOfficers.officers
          || filingForOfficers.officer_names
          || [];
        for (const off of rawOfficers) {
          officers.push({
            name: off.name || off.officer_name || null,
            title: off.title || off.officer_title || null,
            compensation: off.compensation || off.total_compensation || null
          });
        }

        // Persist officers.
        for (const off of officers) {
          if (!off.name) continue;
          const { error: offErr } = await supabase
            .from('entity_officers')
            .insert({
              entity_id: entity.id,
              officer_name: off.name,
              title: off.title,
              compensation: off.compensation != null ? Number(off.compensation) : null,
              source: 'irs_990_propublica',
              filing_year: latestFiling?.tax_prd_yr || latestFiling?.tax_period || null,
              raw_data: off
            });
          if (offErr) {
            console.warn(`[IRS990] Officer insert failed for entity ${entity.id}: ${offErr.message}`);
          }
        }

        // Persist filing/BMF signals on the entity record.
        const updates = {
          metadata: {
            ...(entity.metadata || {}),
            irs990_attempted_at: new Date().toISOString(),
            irs990_fetched_at: new Date().toISOString(),
            irs990_latest_filing_date: latestFiling?.tax_prd || latestFiling?.tax_period || null,
            irs990_total_revenue: latestFiling?.totrevenue ?? null,
            irs990_total_expenses: latestFiling?.totfuncexpns ?? null,
            irs_revocation_date: revocationDate,
            irs_subsection: subseccd
          },
          updated_at: new Date().toISOString()
        };

        const { error: upErr } = await supabase
          .from('entities')
          .update(updates)
          .eq('id', entity.id);

        if (upErr) {
          console.warn(`[IRS990] Update failed for entity ${entity.id}: ${upErr.message}`);
        } else {
          recordsUpdated++;
        }
      } catch (entityErr) {
        console.warn(`[IRS990] Error processing "${entity.name}": ${entityErr.message}`);
      }

      await new Promise(r => setTimeout(r, DELAY_MS));
    }

    const durationMs = Date.now() - startTime;
    await supabase.from('data_pulls').update({
      status: 'completed',
      completed_at: new Date().toISOString(),
      records_fetched: recordsFetched,
      records_updated: recordsUpdated
    }).eq('id', pullId);

    console.log(`[IRS990] Pull ${pullId} completed: fetched=${recordsFetched}, updated=${recordsUpdated}`);
    await sendIngestionEmail(pullId, {
      source: 'irs_990', pull_type: 'enrichment', status: 'completed',
      recordsFetched, recordsCreated: 0, recordsUpdated, durationMs
    });

    return { pullId, recordsFetched, recordsUpdated };
  } catch (err) {
    const durationMs = Date.now() - startTime;
    console.error(`[IRS990] Pull ${pullId} failed:`, err.message);
    await supabase.from('data_pulls').update({
      status: 'failed', completed_at: new Date().toISOString(),
      error_message: err.message, records_fetched: recordsFetched, records_updated: recordsUpdated
    }).eq('id', pullId);
    await sendIngestionEmail(pullId, {
      source: 'irs_990', pull_type: 'enrichment', status: 'failed',
      recordsFetched, recordsCreated: 0, recordsUpdated, durationMs, errorMessage: err.message
    });
    throw err;
  }
}

app.post('/api/ingest/irs990', requireAdmin, async (req, res) => {
  try {
    const { data: pull, error: pullErr } = await supabase
      .from('data_pulls')
      .insert({
        source: 'irs_990', pull_type: 'enrichment',
        status: 'running', metadata: {}
      })
      .select('id')
      .single();

    if (pullErr) return res.status(500).json({ ok: false, error: 'Failed to create pull record' });

    ingestIRS990(pull.id).catch(err => {
      console.error(`[IRS990] Async error for pull ${pull.id}:`, err.message);
    });

    return res.json({ ok: true, pull_id: pull.id, message: 'IRS 990 enrichment started' });
  } catch (err) {
    console.error('IRS990 endpoint error:', err);
    return res.status(500).json({ ok: false, error: 'Server error' });
  }
});

// ── FAPIIS INGESTION (STUB) ─────────────────────────────────
// Federal Awardee Performance and Integrity Information System.
// As of 2026-04-16, FAPIIS is no longer a separately listed SAM.gov
// data-services folder. FAPIIS-class signals are now surfaced through
// two other feeds that Arcane Tracer already ingests:
//   1. Responsibility/Qualification proceedings arrive via the SAM.gov
//      Entity monthly extract (ingestSAMGov).
//   2. Debarments arrive via the SAM.gov daily Exclusions extract
//      (ingestSAMExclusions).
// fapiis.gov remains the record of truth for public browsing but is not
// offered as a bulk file. We keep this function and its endpoint as a
// thin stub so cron history, feature wiring, and downstream callers all
// continue to work without change. The stub marks the pull completed
// with a skipped flag.
async function ingestFAPIIS(pullId) {
  console.log('[FAPIIS] Standalone FAPIIS pull not required; signals are embedded in SAM Entity and Exclusions extracts.');
  await supabase.from('data_pulls').update({
    status: 'completed',
    completed_at: new Date().toISOString(),
    records_fetched: 0,
    records_updated: 0,
    metadata: {
      skipped: true,
      reason: 'fapiis_folded_into_sam_extracts',
      note: 'Responsibility/Qualification proceedings arrive via SAM Entity extract; debarments via SAM Exclusions extract.'
    }
  }).eq('id', pullId);
  return { pullId, skipped: true };
}

app.post('/api/ingest/fapiis', requireAdmin, async (req, res) => {
  try {
    const { data: pull, error: pullErr } = await supabase
      .from('data_pulls')
      .insert({
        source: 'fapiis', pull_type: 'weekly_refresh',
        status: 'running', metadata: {}
      })
      .select('id')
      .single();

    if (pullErr) return res.status(500).json({ ok: false, error: 'Failed to create pull record' });

    ingestFAPIIS(pull.id).catch(err => {
      console.error(`[FAPIIS] Async error for pull ${pull.id}:`, err.message);
    });

    return res.json({ ok: true, pull_id: pull.id, message: 'FAPIIS ingestion started' });
  } catch (err) {
    console.error('FAPIIS endpoint error:', err);
    return res.status(500).json({ ok: false, error: 'Server error' });
  }
});

// ── START ─────────────────────────────────────────────────────
app.listen(port, () => {
  console.log(`Arcane Tracer API running on port ${port}`);
});
