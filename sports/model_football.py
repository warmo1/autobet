import math
import pandas as pd

def fit_poisson(df_matches: pd.DataFrame):
    """Fit a simple team attack/defence model using goals for/against.
    df must have columns: home, away, fthg, ftag
    Returns team attack/defence dicts and league average goals.
    """
    teams = sorted(set(df_matches['home']).union(df_matches['away']))
    att = {t: 0.0 for t in teams}
    deff = {t: 0.0 for t in teams}
    mu_home = df_matches['fthg'].mean() if len(df_matches) else 1.3
    mu_away = df_matches['ftag'].mean() if len(df_matches) else 1.1
    # Simple iterative scaling (not full MLE but ok for starter)
    for _ in range(10):
        for t in teams:
            gf_home = df_matches.loc[df_matches['home']==t, 'fthg']
            gf_away = df_matches.loc[df_matches['away']==t, 'ftag']
            ga_home = df_matches.loc[df_matches['home']==t, 'ftag']
            ga_away = df_matches.loc[df_matches['away']==t, 'fthg']
            gf = pd.concat([gf_home, gf_away]).mean() if len(gf_home)+len(gf_away)>0 else (mu_home+mu_away)/2
            ga = pd.concat([ga_home, ga_away]).mean() if len(ga_home)+len(ga_away)>0 else (mu_home+mu_away)/2
            att[t] = 0.7*att[t] + 0.3*(gf/((mu_home+mu_away)/2))
            deff[t] = 0.7*deff[t] + 0.3*(ga/((mu_home+mu_away)/2))
    return att, deff, mu_home, mu_away


def match_goal_rates(home_team, away_team, att, deff, mu_home, mu_away):
    lam_h = mu_home * att.get(home_team,1.0) / max(deff.get(away_team,1.0), 1e-6)
    lam_a = mu_away * att.get(away_team,1.0) / max(deff.get(home_team,1.0), 1e-6)
    return max(lam_h, 0.05), max(lam_a, 0.05)


def poisson_pmf(lam, k):
    return math.exp(-lam) * (lam**k) / math.factorial(k)


def match_probs_from_rates(lam_h, lam_a, max_goals=10):
    p_home = p_draw = p_away = 0.0
    for hg in range(0, max_goals+1):
        for ag in range(0, max_goals+1):
            p = poisson_pmf(lam_h, hg) * poisson_pmf(lam_a, ag)
            if hg > ag: p_home += p
            elif hg == ag: p_draw += p
            else: p_away += p
    # Normalise to 1
    s = p_home + p_draw + p_away
    if s <= 0: return 1/3, 1/3, 1/3
    return p_home/s, p_draw/s, p_away/s
