<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>{{ title or 'Autobet' }}</title>
    <style>
      body { font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 0; background:#0b0f17; color:#e8eef9; }
      header { background:#111827; border-bottom:1px solid #1f2937; padding: 12px 16px; display:flex; gap:16px; align-items:center; }
      a { color:#93c5fd; text-decoration: none; }
      nav a { margin-right: 12px; }
      main { padding: 16px; max-width: 1100px; margin: 0 auto; }
      .card { background:#111827; border:1px solid #1f2937; border-radius:10px; padding:16px; margin-bottom:16px; }
      .table { width:100%; border-collapse: collapse; }
      .table th, .table td { padding:8px 10px; border-bottom:1px solid #1f2937; }
      .muted { color:#9ca3af; }
      .btn { background:#2563eb; color:white; border:none; padding:6px 10px; border-radius:8px; cursor:pointer; }
      .btn.secondary { background:#374151; }
      .row { display:flex; gap:16px; flex-wrap: wrap; }
      .col { flex:1 1 320px; }
      .flash { padding:10px 12px; border-radius:8px; margin:8px 0; }
      .flash.success { background:#064e3b; color:#d1fae5; }
      .flash.danger { background:#7f1d1d; color:#fee2e2; }
      input, select { background:#0b1220; color:#e5e7eb; border:1px solid #1f2937; border-radius:8px; padding:6px 8px; }
      label { display:block; font-size:12px; color:#9ca3af; margin-bottom:4px; }
      form.inline { display:flex; align-items:center; gap:8px; }
      .pill { display:inline-block; padding:2px 8px; border-radius:999px; background:#0f172a; border:1px solid #1f2937; font-size:12px; color:#a7b0c0; }
    </style>
  </head>
  <body>
    <header>
      <strong><a href="/">⚽ Autobet</a></strong>
      <nav>
        <a href="/fixtures">Fixtures</a>
        <a href="/suggestions">Suggestions</a>
        <a href="/bets">Bets</a>
        <a href="/bank">Bank</a>
      </nav>
      <span style="margin-left:auto" class="muted">DB: {{ config['DB_URL'] if config and config.get('DB_URL') else '' }}</span>
    </header>
    <main>
      {% with messages = get_flashed_messages(with_categories=true) %}
        {% if messages %}
          {% for category, message in messages %}
            <div class="flash {{ category }}">{{ message }}</div>
          {% endfor %}
        {% endif %}
      {% endwith %}
      {% block content %}{% endblock %}
    </main>
  </body>
</html>
{% extends 'base.html' %}
{% block content %}
<div class="row">
  <div class="col">
    <div class="card">
      <h2 style="margin:0 0 8px 0;">Today’s Fixtures <span class="pill">{{ date_iso }}</span></h2>
      {% if fixtures %}
      <table class="table">
        <thead><tr><th style="width:26%">Competition</th><th>Home</th><th>Away</th><th style="width:28%">Match ID</th></tr></thead>
        <tbody>
          {% for m in fixtures %}
          <tr>
            <td>{{ m['comp'] }}</td>
            <td>{{ m['home'] }}</td>
            <td>{{ m['away'] }}</td>
            <td class="muted">{{ m['match_id'] }}</td>
          </tr>
          {% endfor %}
        </tbody>
      </table>
      {% else %}
        <div class="muted">No fixtures found. Try ingesting via ESPN and refresh.</div>
      {% endif %}
    </div>
  </div>
  <div class="col">
    <div class="card">
      <h2 style="margin:0 0 8px 0;">Top Suggestions</h2>
      {% if suggestions %}
      <table class="table">
        <thead><tr><th>Fixture</th><th>Pick</th><th>Edge</th><th>Kelly</th></tr></thead>
        <tbody>
          {% for s in suggestions %}
          <tr>
            <td>{{ s['home'] }} vs {{ s['away'] }}</td>
            <td>{{ s['sel'] }}</td>
            <td>{{ '%.2f%%' % (100*s['edge']) }}</td>
            <td>{{ '%.2f' % s['kelly'] }}</td>
          </tr>
          {% endfor %}
        </tbody>
      </table>
      {% else %}
        <div class="muted">No suggestions yet.</div>
      {% endif %}
    </div>
    <div class="card">
      <h2 style="margin:0 0 8px 0;">Bank</h2>
      <div class="muted">Current balance</div>
      <div style="font-size:28px;">£{{ '%.2f' % balance }}</div>
    </div>
  </div>
</div>
{% endblock %}
{% extends 'base.html' %}
{% block content %}
<div class="card">
  <h2 style="margin:0 0 10px 0;">Fixtures <span class="pill">{{ date_iso }}</span></h2>
  <form class="inline" method="get" action="/fixtures">
    <label for="date">Date</label>
    <input type="date" id="date" name="date" value="{{ date_iso }}" />
    <button class="btn" type="submit">Go</button>
    <a class="btn secondary" href="/fixtures?date={{ prev_date }}">◀ {{ prev_date }}</a>
    <a class="btn secondary" href="/fixtures?date={{ next_date }}">{{ next_date }} ▶</a>
  </form>
  {% if fixtures %}
  <table class="table" style="margin-top:12px;">
    <thead><tr><th style="width:26%">Competition</th><th>Home</th><th>Away</th><th style="width:28%">Match ID</th><th>Quick bet</th></tr></thead>
    <tbody>
      {% for m in fixtures %}
      <tr>
        <td>{{ m['comp'] }}</td>
        <td>{{ m['home'] }}</td>
        <td>{{ m['away'] }}</td>
        <td class="muted">{{ m['match_id'] }}</td>
        <td>
          <form class="inline" method="post" action="/bets">
            <input type="hidden" name="match_id" value="{{ m['match_id'] }}" />
            <select name="sel">
              <option value="H">H</option>
              <option value="D">D</option>
              <option value="A">A</option>
            </select>
            <input type="number" step="0.01" name="stake" placeholder="Stake" style="width:90px" />
            <input type="number" step="0.01" name="price" placeholder="Price" style="width:90px" />
            <button class="btn" type="submit">Add</button>
          </form>
        </td>
      </tr>
      {% endfor %}
    </tbody>
  </table>
  {% else %}
    <div class="muted">No fixtures for this date.</div>
  {% endif %}
</div>
{% endblock %}
{% extends 'base.html' %}
{% block content %}
<div class="card">
  <h2 style="margin:0 0 10px 0;">Suggestions</h2>
  <form class="inline" method="get" action="/suggestions">
    <label for="min_edge">Min edge</label>
    <input type="number" step="0.01" id="min_edge" name="min_edge" value="{{ '%.2f' % min_edge }}" />
    <button class="btn" type="submit">Filter</button>
  </form>
  {% if suggestions %}
  <table class="table" style="margin-top:12px;">
    <thead><tr><th>Competition</th><th>Fixture</th><th>Pick</th><th>Prob</th><th>Book</th><th>Price</th><th>Edge</th><th>Kelly</th><th>Match ID</th></tr></thead>
    <tbody>
      {% for s in suggestions %}
      <tr>
        <td>{{ s['comp'] }}</td>
        <td>{{ s['home'] }} vs {{ s['away'] }}</td>
        <td>{{ s['sel'] }}</td>
        <td>{{ '%.1f%%' % (100*s['model_prob']) }}</td>
        <td>{{ s['book'] }}</td>
        <td>{{ '%.2f' % s['price'] }}</td>
        <td>{{ '%.2f%%' % (100*s['edge']) }}</td>
        <td>{{ '%.2f' % s['kelly'] }}</td>
        <td class="muted">{{ s['match_id'] }}</td>
      </tr>
      {% endfor %}
    </tbody>
  </table>
  {% else %}
    <div class="muted">No suggestions available.</div>
  {% endif %}
</div>
{% endblock %}
{% extends 'base.html' %}
{% block content %}
<div class="card">
  <h2 style="margin:0 0 10px 0;">Paper Bets</h2>
  <form class="inline" method="post" action="/bets">
    <input type="text" name="match_id" placeholder="match_id" style="width:280px" />
    <select name="sel">
      <option value="H">H</option>
      <option value="D">D</option>
      <option value="A">A</option>
    </select>
    <input type="number" step="0.01" name="stake" placeholder="Stake" style="width:120px" />
    <input type="number" step="0.01" name="price" placeholder="Price" style="width:120px" />
    <button class="btn" type="submit">Add bet</button>
  </form>
  <div style="height:8px"></div>
  {% if bets %}
  <table class="table">
    <thead><tr><th>When</th><th>Comp</th><th>Fixture</th><th>Sel</th><th>Stake</th><th>Price</th><th>Match ID</th></tr></thead>
    <tbody>
      {% for b in bets %}
      <tr>
        <td class="muted">{{ b['created_ts'] }}</td>
        <td>{{ b['comp'] }}</td>
        <td>{{ b['home'] }} vs {{ b['away'] }}</td>
        <td>{{ b['sel'] }}</td>
        <td>{{ '%.2f' % b['stake'] }}</td>
        <td>{{ '%.2f' % b['price'] }}</td>
        <td class="muted">{{ b['match_id'] }}</td>
      </tr>
      {% endfor %}
    </tbody>
  </table>
  {% else %}
    <div class="muted">No bets yet.</div>
  {% endif %}
</div>
{% endblock %}
{% extends 'base.html' %}
{% block content %}
<div class="card">
  <h2 style="margin:0 0 10px 0;">Bank</h2>
  <form method="post" action="/bank">
    <label for="balance">Balance (£)</label>
    <input type="number" step="0.01" id="balance" name="balance" value="{{ '%.2f' % balance }}" />
    <button class="btn" type="submit">Update</button>
  </form>
</div>
{% endblock %}
