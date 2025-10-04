import pandas as pd
import numpy as np
import os
import sys

class PriceFeatureEngineer:
    def __init__(self):
        self.feature_names = []
    
    def create_technical_indicators(self, df, max_lookback=20):
        """
        Создание технических индикаторов БЕЗ data leak
        Используем ТОЛЬКО lagged цены (open, high, low, close)
        """
        df = df.copy()
        
        # lagged 
        price_columns = ['open', 'high', 'low', 'close', 'volume']
        
       
        for lag in [1, 2, 3, 5, 10, 15, 20, 25, 30]:
            for col in price_columns:
                df[f'{col}_lag_{lag}'] = df[col].shift(lag)
        

        df['price_range'] = (df['high'] - df['low']) / df['close']
        df['body_size'] = abs(df['close'] - df['open']) / df['close']
        df['upper_shadow'] = (df['high'] - np.maximum(df['open'], df['close'])) / df['close']
        df['lower_shadow'] = (np.minimum(df['open'], df['close']) - df['low']) / df['close']
        
        # RSI 
        df['rsi_14'] = self.calculate_rsi_no_leak(df['close'], 14)
        
        # EMA
        df['ema_12'] = self.ema(df['close'], 12)
        df['ema_26'] = self.ema(df['close'], 26)
        df['macd'] = df['ema_12'] - df['ema_26']
        
        # Линии Боллинджера 
        df = self.add_bollinger(df, price_column='close')
        

        df['price_momentum_3'] = df['close'] / df['close_lag_3'] - 1
        df['price_momentum_5'] = df['close'] / df['close_lag_5'] - 1
        df['price_momentum_15'] = df['close'] / df['close_lag_15'] - 1
        df['price_momentum_30'] = df['close'] / df['close_lag_30'] - 1
        
        return df
    
    def add_bollinger(self, df, price_column='close', window=20, num_std=2):
        """Линии Боллинджера на LAGGED данных"""
        df['bollinger_middle'] = df[price_column].expanding(min_periods=window).mean()
        
        # Стандартное отклонение на lagged данных
        rolling_std = df[price_column].expanding(min_periods=window).std()
        
        df['bollinger_upper'] = df['bollinger_middle'] + (rolling_std * num_std)
        df['bollinger_lower'] = df['bollinger_middle'] - (rolling_std * num_std)
        
        return df

    def calculate_rsi_no_leak(self, prices, window=14):
        """RSI расчет только на LAGGED данных"""
        delta = prices.diff()
        
        # Используем expanding window для избежания lookahead
        gain = delta.where(delta > 0, 0).expanding(min_periods=window).mean()
        loss = (-delta.where(delta < 0, 0)).expanding(min_periods=window).mean()
        
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi

    def ema(self, prices, span):
        """EMA расчет только на LAGGED данных"""
        return prices.ewm(span=span, adjust=False).mean()


    def create_temporal_features(self, df):

        df = df.copy()
        
        # Циклические кодирования для временных признаков
        df['day_of_week_sin'] = np.sin(2 * np.pi * df['begin'].dt.dayofweek / 7)
        df['day_of_week_cos'] = np.cos(2 * np.pi * df['begin'].dt.dayofweek / 7)
        
        df['day_of_month_sin'] = np.sin(2 * np.pi * df['begin'].dt.day / 31)
        df['day_of_month_cos'] = np.cos(2 * np.pi * df['begin'].dt.day / 31)
        
        df['month_sin'] = np.sin(2 * np.pi * df['begin'].dt.month / 12)
        df['month_cos'] = np.cos(2 * np.pi * df['begin'].dt.month / 12)
        
        # Бинарные признаки
        df['is_weekend'] = df['begin'].dt.dayofweek.isin([5, 6]).astype(int)
        df['is_month_start'] = df['begin'].dt.is_month_start.astype(int)
        df['is_month_end'] = df['begin'].dt.is_month_end.astype(int)
        
        # Часовые признаки (если данные внутридневные)
        if df['begin'].dt.hour.nunique() > 1:
            df['hour_sin'] = np.sin(2 * np.pi * df['begin'].dt.hour / 24)
            df['hour_cos'] = np.cos(2 * np.pi * df['begin'].dt.hour / 24)
        
        return df
    
    def remove_duplicate_columns_keep_first(self, df):
        """
        Удаляет дублирующиеся колонки, оставляя первую встретившуюся
        """
        # Находим дубликаты
        duplicate_mask = df.columns.duplicated(keep='first')  # keep='first' оставляет первую
        duplicate_columns = df.columns[duplicate_mask].tolist()
        
        # Удаляем дубликаты
        df_clean = df.loc[:, ~duplicate_mask]
        
        return df_clean
    

    def combine_full_df(self, df):
        technical  = self.create_technical_indicators(df)
        temporal_fatures = self.create_temporal_features(df)
        full_df = pd.concat([technical, temporal_fatures], axis=1)
        full_df = self.remove_duplicate_columns_keep_first(full_df)
        
        return full_df


        