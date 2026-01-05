import requests
from bs4 import BeautifulSoup
from datetime import datetime

def get_latest_league():
    url = 'https://www.poewiki.net/wiki/League'
    response = requests.get(url)
    html = response.text
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find('table', {'class': 'cargoTable'})
    rows = table.find_all('tr')

    leagues = []
    for row in rows[1:]:
        cells = row.find_all('td')
        if cells:
            league_name = cells[0].text.strip()
            release_date = cells[1].text.strip()
            leagues.append({'League': league_name, 'Release Date': release_date})

    def parse_date(date_str):
        for fmt in ('%Y-%m-%d %I:%M:%S %p', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d'):
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        return datetime.min

    if leagues:
        latest = max(leagues, key=lambda x: parse_date(x['Release Date']))
        return latest['League'].split()[0]  # возвращаем только имя без "league"
    return None
