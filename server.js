require('dotenv').config();
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
    const { name, email, org, level, usecase } = req.body;
    const ip = req.headers['x-forwarded-for'] || req.socket.remoteAddress || 'unknown';

    // ── VALIDATION ────────────────────────────────────────────
    if (!name || !email || !level || !usecase) {
      return res.status(400).json({ ok: false, error: 'Required fields missing.' });
    }
    if (!email.includes('@') || !email.includes('.')) {
      return res.status(400).json({ ok: false, error: 'Invalid email address.' });
    }

    const submittedAt = new Date().toISOString();

    // ── LOG TO SUPABASE ───────────────────────────────────────
    const { error: dbError } = await supabase
      .from('access_requests')
      .insert({
        name,
        email,
        org: org || null,
        level,
        usecase,
        ip,
        submitted_at: submittedAt,
        status: 'pending' // pending → reviewed → approved → rejected
      });

    if (dbError) {
      console.error('Supabase insert error:', dbError);
      // Continue anyway — don't fail the user because of a DB issue
    }

    // ── NOTIFY YOU ────────────────────────────────────────────
    // Clean, formatted email delivered to your inbox immediately
    await resend.emails.send({
      from: 'Arcane Tracer <notifications@arcanetracer.com>',
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

    // ── AUTO-REPLY TO SUBMITTER ───────────────────────────────
    await resend.emails.send({
      from: 'Arcane Tracer <hello@arcanetracer.com>',
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

    return res.json({ ok: true });

  } catch (err) {
    console.error('Access request error:', err);
    return res.status(500).json({ ok: false, error: 'Server error. Please try again or email hello@arcanetracer.com directly.' });
  }
});

// ── START ─────────────────────────────────────────────────────
app.listen(port, () => {
  console.log(`Arcane Tracer API running on port ${port}`);
});
