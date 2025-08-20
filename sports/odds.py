def implied_prob(odds: float) -> float:
    if not odds or odds <= 1e-9: return 0.0
    return 1.0/odds

def fair_probs_from_odds(odds_triplet):
    # Remove overround by normalising
    probs = [implied_prob(o) for o in odds_triplet]
    s = sum(probs)
    if s <= 0: return [1/3,1/3,1/3]
    return [p/s for p in probs]

def kelly_fraction(p: float, b: float) -> float:
    # b = decimal_odds - 1
    if b <= 0: return 0.0
    f = (p*(b+1)-1)/b
    return max(0.0, f)

def choose_back_or_lay(model_p, market_odds):
    """Return ('back' or 'lay', selection_edge)
    For lay, we consider edge on the complement probability.
    """
    p = model_p
    q = 1-p
    back_ev = p*market_odds - 1
    # Lay price decimal L: EV ~ (q*1 - p*(L-1)) after commission ignored; treat qualitatively here
    lay_edge = q - 1/market_odds
    back_edge = p - 1/market_odds
    if lay_edge > back_edge:
        return 'lay', lay_edge
    else:
        return 'back', back_edge
