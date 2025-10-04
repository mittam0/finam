
##########################################
#Config настройка
SEED = 42
TIME_COLUMN = 'begin'
TICKER_COLUMN = 'ticker'
PRICE_COLUMNS = ['open', 'high', 'low', 'close', 'volume']
DATE_FOR_PREDICTIONS = '2025-09-08 00:00:00' # Дата на которую хотим получить прогноз
PREDICT_TYPE = 'WEEK_OFF' # Заполняем выходные дни пропуском


DATA_PATHS = {
    'candles': 'data/train/candles.csv',
    'candles_test': 'data/test/candles_test.csv',
    'news':  'data/train/news.csv',
    'news_test':  'data/test/news_test.csv'
}
