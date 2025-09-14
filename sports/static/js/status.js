function fmtTimeAgo(ts) {
  try {
    if (!ts) return '—';
    const d = new Date(ts);
    if (isNaN(d.getTime())) return String(ts);
    const secs = Math.floor((Date.now() - d.getTime()) / 1000);
    if (secs < 60) return `${secs}s ago`;
    const mins = Math.floor(secs / 60);
    if (mins < 60) return `${mins}m ago`;
    const hrs = Math.floor(mins / 60);
    if (hrs < 24) return `${hrs}h ago`;
    const days = Math.floor(hrs / 24);
    return `${days}d ago`;
  } catch (_) { return '—'; }
}

function classifyAge(ts) {
  try {
    if (!ts) return 'warn';
    const d = new Date(ts);
    if (isNaN(d.getTime())) return 'warn';
    const mins = Math.floor((Date.now() - d.getTime()) / 60000);
    if (mins <= 10) return 'ok';
    if (mins <= 60) return 'warn';
    return 'err';
  } catch (_) { return 'warn'; }
}

function classifyAgeDaily(ts) {
  try {
    if (!ts) return 'warn';
    const d = new Date(ts);
    if (isNaN(d.getTime())) return 'warn';
    const hours = Math.floor((Date.now() - d.getTime()) / 3600000);
    if (hours <= 24) return 'ok';
    if (hours <= 48) return 'warn';
    return 'err';
  } catch (_) { return 'warn'; }
}

async function loadDataFreshness() {
  try {
    const res = await fetch('/api/status/data_freshness');
    const j = await res.json();
    const ev = j.events || {};
    const pr = j.products || {};
    const po = j.probable_odds || {};
    const ps = j.pool_snapshots || {};
    const set = (id, val) => { const el = document.getElementById(id); if (el) el.textContent = val; };
    set('events-today', ev.today ?? '0');
    set('events-last-fetch', fmtTimeAgo(ev.last));
    set('products-today', pr.today ?? '0');
    set('products-last-fetch', fmtTimeAgo(pr.last));
    set('odds-today', po.today ?? '0');
    set('odds-last-fetch', fmtTimeAgo(po.last));
    set('pools-today', ps.today ?? '0');
    set('pools-last-fetch', fmtTimeAgo(ps.last));

    // classify mini-cards by age freshness
    const setClass = (cardId, ts, classifier = classifyAge) => {
      const el = document.getElementById(cardId);
      if (!el) return;
      el.classList.remove('ok','warn','err');
      el.classList.add(classifier(ts));
    };
    setClass('card-events', ev.last, classifyAgeDaily);
    setClass('card-products', pr.last, classifyAgeDaily);
    setClass('card-odds', po.last);
    setClass('card-pools', ps.last);
  } catch (e) {
    console.error('freshness error', e);
  }
}

async function loadUpcoming() {
  try {
    const res = await fetch('/api/status/upcoming');
    const j = await res.json();
    const list = j.items || [];
    const root = document.getElementById('upcoming-races-list');
    if (!root) return;
    root.innerHTML = '';
    if (list.length === 0) {
      root.innerHTML = '<div class="small text-muted">No upcoming races found.</div>';
      return;
    }
    list.slice(0, 15).forEach(item => {
      const div = document.createElement('div');
      div.className = 'race-item';
      const left = document.createElement('div');
      let sportAndVenue = item.venue || '';
      if (item.sport) {
        sportAndVenue = `${item.sport} • ${sportAndVenue}`;
      }
      left.innerHTML = `<div><b>${item.event_name || item.product_id}</b></div><div class="small text-muted">${sportAndVenue} • ${item.country || ''}</div>`;
      const right = document.createElement('div');
      let qcInfo = '';
      if (item.avg_cov != null) {
        qcInfo = `<div class="small text-muted">QC: ${(item.avg_cov * 100).toFixed(1)}%</div>`;
      }
      right.innerHTML = `<div>${(item.status || '').toUpperCase()}</div><div class="small text-muted">${item.start_iso || ''}</div>${qcInfo}`;
      div.appendChild(left);
      div.appendChild(right);
      root.appendChild(div);
    });
  } catch (e) {
    console.error('upcoming error', e);
  }
}

function badge(ok, textIfOk = 'Ready', textIfNot = 'Not Ready') {
  const span = document.createElement('span');
  span.className = 'badge ' + (ok ? 'ok' : 'warn');
  span.textContent = ok ? textIfOk : textIfNot;
  return span;
}

async function loadGcp() {
  try {
    const res = await fetch('/api/status/gcp');
    const j = await res.json();
    const meta = document.getElementById('gcp-meta');
    if (meta && !j.error) {
      meta.textContent = `Project: ${j.project} • Region: ${j.region}`;
    }
    // Cloud Run
    const crTbody = document.querySelector('#cloudrun-table tbody');
    if (crTbody) {
      crTbody.innerHTML = '';
      const services = (j.cloud_run && j.cloud_run.services) || [];
      services.forEach(svc => {
        const tr = document.createElement('tr');
        const tdN = document.createElement('td'); tdN.textContent = svc.name; tr.appendChild(tdN);
        const tdR = document.createElement('td');
        const isReady = !svc.missing && !!svc.ready;
        tdR.appendChild(badge(isReady));
        if (!isReady && svc.conditions) {
            const details = document.createElement('div');
            details.className = 'small text-muted';
            details.textContent = JSON.stringify(svc.conditions, null, 2);
            tdR.appendChild(details);
        }
        tr.appendChild(tdR);
        const tdU = document.createElement('td');
        if (svc.uri) { const a = document.createElement('a'); a.href = svc.uri; a.textContent = svc.uri; a.target = '_blank'; tdU.appendChild(a); }
        else { tdU.textContent = '—'; }
        tr.appendChild(tdU);
        const tdT = document.createElement('td'); tdT.textContent = svc.updateTime || '—'; tr.appendChild(tdT);
        crTbody.appendChild(tr);
      });
    }
    // Scheduler
    const schTbody = document.querySelector('#scheduler-table tbody');
    if (schTbody) {
      schTbody.innerHTML = '';
      const jobs = (j.scheduler && j.scheduler.jobs) || [];
      jobs.forEach(job => {
        const tr = document.createElement('tr');
        const tdN = document.createElement('td'); tdN.textContent = (job.name || '').split('/').slice(-1)[0]; tr.appendChild(tdN);
        const tdS = document.createElement('td'); tdS.appendChild(badge(job.state === 'ENABLED', job.state || '—', job.state || '—')); tr.appendChild(tdS);
        const tdSch = document.createElement('td'); tdSch.textContent = job.schedule || '—'; tr.appendChild(tdSch);
        const tdL = document.createElement('td'); tdL.textContent = job.lastAttemptTime || '—'; tr.appendChild(tdL);
        schTbody.appendChild(tr);
      });
    }
    // Pub/Sub
    const psTbody = document.querySelector('#pubsub-table tbody');
    if (psTbody) {
      psTbody.innerHTML = '';
      const topic = j.pubsub && j.pubsub.topic;
      const sub = j.pubsub && j.pubsub.subscription;
      const add = (name, exists, detail) => {
        const tr = document.createElement('tr');
        const tdN = document.createElement('td'); tdN.textContent = name; tr.appendChild(tdN);
        const tdE = document.createElement('td'); tdE.appendChild(badge(!!exists, exists ? 'Yes' : 'No', exists ? 'Yes' : 'No')); tr.appendChild(tdE);
        const tdD = document.createElement('td'); tdD.textContent = detail || '—'; tr.appendChild(tdD);
        psTbody.appendChild(tr);
      };
      add('Topic ingest-jobs', topic && topic.exists, topic && topic.name);
      add('Subscription ingest-fetcher-sub', sub && sub.exists, sub && (sub.pushEndpoint || sub.topic));
    }

    // Recent Ingestion Jobs are loaded via separate endpoint
  
  } catch (e) {
    console.error('gcp status error', e);
  }
}

async function loadJobLog() {
  try {
    const res = await fetch('/api/status/job_log');
    const j = await res.json();
    const tbody = document.getElementById('job-log-body');
    if (!tbody) return;
    tbody.innerHTML = '';
    const items = j.items || [];
    items.forEach(r => {
      const tr = document.createElement('tr');
      const tdStatus = document.createElement('td');
      tdStatus.appendChild(badge((r.status || '').toUpperCase() === 'OK', r.status || '—', r.status || '—'));
      tr.appendChild(tdStatus);
      const tdTask = document.createElement('td');
      tdTask.textContent = `${r.component || ''}:${r.task || ''}`;
      tr.appendChild(tdTask);
      const tdStart = document.createElement('td');
      tdStart.textContent = r.started_ts ? new Date(Number(r.started_ts)).toISOString() : '—';
      tr.appendChild(tdStart);
      const tdDur = document.createElement('td');
      tdDur.textContent = (r.duration_ms != null) ? `${Math.round(Number(r.duration_ms))} ms` : '—';
      tr.appendChild(tdDur);
      tbody.appendChild(tr);
    });
  } catch (e) {
    console.error('job log error', e);
  }
}

window.addEventListener('DOMContentLoaded', () => {
  loadDataFreshness();
  loadUpcoming();
  loadGcp();
  loadJobLog();
  // Auto-refresh every 60s for light-weight endpoints
  setInterval(loadDataFreshness, 60000);
  setInterval(loadUpcoming, 60000);
  setInterval(loadGcp, 120000);
  setInterval(loadJobLog, 60000);

  // QC widget
  (async function loadQc(){
    try{
      const res = await fetch('/api/status/qc');
      const j = await res.json();
      const el = document.getElementById('qc-list');
      if (!el) return;
      el.innerHTML = '';
      const add = (label, value, goodWhenZero=false) => {
        const row = document.createElement('div');
        row.className = 'qc-row';
        const l = document.createElement('span'); l.textContent = label; row.appendChild(l);
        const v = document.createElement('span'); v.textContent = (value == null ? '—' : value);
        const ok = goodWhenZero ? (Number(value) === 0) : (Number(value) > 0);
        v.className = 'badge ' + (ok ? 'ok' : 'warn');
        row.appendChild(v);
        el.appendChild(row);
      };
      add('Missing runner numbers (today)', j.missing_runner_numbers, true);
      add('Missing bet rules', j.missing_bet_rules, true);
      add('Avg odds coverage', (j.probable_odds_avg_cov!=null? (j.probable_odds_avg_cov*100).toFixed(1)+'%':'—'));
      add('GB SF missing snapshots (today)', j.gb_sf_missing_snapshots, true);
    }catch(e){ console.error('qc error', e);} 
    // refresh QC periodically
    setTimeout(arguments.callee, 120000);
  })();

  // Quick actions wiring
  document.querySelectorAll('.quick-actions .btn').forEach(btn => {
    btn.addEventListener('click', async () => {
      const action = btn.getAttribute('data-action');
      const srcSel = btn.getAttribute('data-src');
      const payload = { action };
      if (srcSel) {
        const val = document.querySelector(srcSel)?.value?.trim();
        if (action === 'probable_odds_event') payload.event_id = val;
        if (action === 'single_product') payload.product_id = val;
      }
      const resEl = document.getElementById('qa-result');
      try {
        let res;
        if (action === 'ensure_views') {
          res = await fetch('/api/admin/ensure_views', { method: 'POST' });
        } else {
          res = await fetch('/api/trigger', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
        }
        const j = await res.json();
        if (res.ok) {
          const okMsg = (action === 'ensure_views')
            ? 'Views reloaded'
            : `${j.action} → ${Array.isArray(j.message_ids) ? j.message_ids.join(', ') : ''}`;
          resEl.textContent = `OK: ${okMsg}`;
          resEl.style.color = '#166534';
          // Refresh freshness soon after firing
          setTimeout(loadDataFreshness, 2000);
          setTimeout(loadJobLog, 3000);
        } else {
          resEl.textContent = `Error: ${j.error || res.status}`;
          resEl.style.color = '#991b1b';
        }
      } catch (e) {
        if (resEl) { resEl.textContent = `Error: ${e}`; resEl.style.color = '#991b1b'; }
      }
    });
  });
});
