import requests
import pandas as pd


def parse_cards(league):
    url = f"https://poe.ninja/poe1/api/economy/stash/current/item/overview?league={league}&type=DivinationCard"
    response = requests.get(url)
    if response.status_code != 200:
        return None

    data = response.json()
    lines = data.get('lines', [])

    result = []
    for line in lines:
        result.append({
            'league_name': league,
            'card_name': line.get('name'),
            'stack_size': line.get('stackSize'),
            'chaos_value': line.get('chaosValue'),
            'trade_count': line.get('tradeInfo', {}).get('count') if line.get('tradeInfo') else 0,
            'details_id': line.get('detailsId')
        })
    return pd.DataFrame(result)
