import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import sys

TICKER_COLUMN = 'ticker'

class DataLoader:
    def __init__(self):
        self.processed_data = {}
    
    def load_candles(self, file_path):
        df = pd.read_csv(file_path)
        # Преобразование времени
        df['begin'] = pd.to_datetime(df['begin'])
        
        # Сортировка по времени

        df = df.sort_values(['ticker', 'begin'])


        return df
    
    def load_news(self, file_path):
        df = pd.read_csv(file_path)
        
        # Преобразование времени
        df['publish_date'] = pd.to_datetime(df['publish_date'])
        df['publish_date'] = df['publish_date'].dt.tz_localize(None)
        
        # Очистка текста
        df['title'] = df['title'].fillna('').astype(str)
        df['publication'] = df['publication'].fillna('').astype(str)


        return df
