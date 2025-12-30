import requests
import pandas as pd


def parse_currency(league):
    url = f"https://poe.ninja/api/data/currencyoverview?league={league}&type=Currency"
    response = requests.get(url)
    if response.status_code != 200:
        return None

    data = response.json()
    lines = data.get('lines', [])

    # Формируем список словарей точно под структуру таблицы
    result = []
    for line in lines:
        result.append({
            'league_name': league,
            'currency_name': line.get('currencyTypeName'),
            'details_id': line.get('detailsId'),
            'chaos_equivalent': line.get('chaosEquivalent'),
            'pay_value': line.get('pay', {}).get('value') if line.get('pay') else None,
            'receive_value': line.get('receive', {}).get('value') if line.get('receive') else None,
            'trade_count': line.get('pay', {}).get('count') if line.get('pay') else 0
        })
    return pd.DataFrame(result)
