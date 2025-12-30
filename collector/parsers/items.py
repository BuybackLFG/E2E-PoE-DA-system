import requests
import pandas as pd


def parse_items(league):
    # Для примера берем уникальное оружие, можно добавить и другие типы
    url = f"https://poe.ninja/api/data/itemoverview?league={league}&type=UniqueWeapon"
    response = requests.get(url)
    if response.status_code != 200:
        return None

    data = response.json()
    lines = data.get('lines', [])

    result = []
    for line in lines:
        result.append({
            'league_name': league,
            'item_name': line.get('name'),
            'base_type': line.get('baseType'),
            'item_type': line.get('itemType'),
            'level_required': line.get('levelRequired'),
            'chaos_value': line.get('chaosValue'),
            'links': line.get('links'),
            'details_id': line.get('detailsId')
        })
    return pd.DataFrame(result)
