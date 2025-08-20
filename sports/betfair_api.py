from .config import cfg

def place_bet_back(symbol: str, odds: float, stake: float):
    # Placeholder: requires betfairlightweight login/session etc.
    if not cfg.betfair_app_key or not cfg.betfair_username:
        raise RuntimeError("Betfair credentials not configured.")
    # TODO: implement API-NG session and placeOrders
    return {"status":"not-implemented", "symbol":symbol, "odds":odds, "stake":stake}
