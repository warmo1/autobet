def elo_update(r_a, r_b, score_a, k=20):
    exp_a = 1.0/(1.0+10**((r_b-r_a)/400.0))
    return r_a + k*(score_a-exp_a)

def fit_elo(matches):
    """Matches: list of dicts with keys home, away, result ('H','D','A').
    Returns ratings dict.
    """
    ratings = {}
    for m in matches:
        h = m['home']; a = m['away']; r = m['result']
        ra = ratings.get(h, 1500.0)
        rb = ratings.get(a, 1500.0)
        score_a = 1.0 if r=='H' else (0.5 if r=='D' else 0.0)
        ra_new = elo_update(ra, rb, score_a)
        rb_new = elo_update(rb, ra, 1.0-score_a)
        ratings[h] = ra_new; ratings[a] = rb_new
    return ratings
