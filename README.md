# E2E-PoE-DA-system


## Структура ДБ

```
E2E-PoE-DA-system/
├── collector/
│   ├── collector.py              # Главный колектор
│   ├── league_manager.py         # Управление лигами в дб
│   ├── parsers/                  # парсеры
│   │   ├── currency.py           # валют
│   │   ├── cards.py              # карт
│   │   ├── items.py              # уников
│   │   ├── league_finder.py      # последней лиги
│   │   └── historical.py         # дампы старых лиг
│   ├── init-db/
│   │   └── init.sql              # схема дб
│   ├── Dockerfile
│   └── requirements.txt
├── docker-compose.yml
├── .env.example
└── README.md
```

## Используемые ресурсы

- [poe.ninja](https://poe.ninja) - Источник данных экономики PoE
- [poewiki.net](https://www.poewiki.net) - Информация по лигам