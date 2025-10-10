import warnings
import os
import joblib
from datetime import datetime, timedelta
from tqdm import tqdm

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

from sklearn.model_selection import TimeSeriesSplit, RandomizedSearchCV
from sklearn.preprocessing import StandardScaler, MinMaxScaler, RobustScaler


from lightgbm import LGBMRegressor


# Локальные импорты
from pipline_create_train.pipe import _model_selection, create_pipeline
from config.config import DATA_PATHS, SEED, DATE_FOR_PREDICTIONS, PREDICT_TYPE, TICKER_COLUMN
from load_data import DataLoader

#Препроцессинг
from preprocessing_number_feature.number_preprocessing import PriceFeatureEngineer
from preprocessing_news_feature.main_preprocessor import feature_text_generate
from tk_.tk import *

# Настройка окружения
warnings.filterwarnings('ignore')
os.makedirs('models_weight', exist_ok=True)





# ========== Загрузка данных ========== 
print('[INFO] Загрузка датасетов')



# train_data
try:
    data_train = DataLoader().load_candles(DATA_PATHS['candles'])
except:
    try:
        path = select_file_with_prompt(prompt_text='Выберите тренировочный файл (КОТИРОВКИ)')
        data_train = pd.read_csv(path)
    except:
        print('Ошибка при загрузке трейна')

# train_news
try:
    data_train_news = DataLoader().load_news(DATA_PATHS['news'])
except:
    try:
        path = select_file_with_prompt(prompt_text='Выберите тренировочный файл (НОВОСТИ)')
        data_train_news = pd.read_csv(path)
    except:
        print('Ошибка при загрузке трейна')


# TEST data
try:
    data_test = DataLoader().load_candles(DATA_PATHS['candles_test'])
except:
    try:
        path = select_file_with_prompt(prompt_text='Выберите тестовый файл (КОТИРОВКИ)')
        data_test = pd.read_csv(path)
    except:
        print('Ошибка при загрузке тестовых котировок')
        data_test = pd.DataFrame()

# TEST NEWS
try:
    data_test_news = DataLoader().load_news(DATA_PATHS['news_test'])
except:
    try:
        path = select_file_with_prompt(prompt_text='Выберите тестовый файл (НОВОСТИ)')
        data_test_news = pd.read_csv(path)
    except:
        print('Ошибка при загрузке тестовых новостей')
        data_test_news = pd.DataFrame()




# ========== Предобработка данных ==========
print('[INFO] Предобработка данных')

#Объединил данные в одну выборку 
full_data = pd.concat([data_train, data_test])
#Преобразую новости

full_news = pd.concat([data_train_news, data_test_news])


def filter_last_6_months(group):
    max_date = group['begin'].max()
    six_months_ago = max_date - pd.DateOffset(months=6)
    return group[group['begin'] >= six_months_ago]

# Применяем фильтрацию для каждого тикера

#Оставляю даныне с 2024 гола
full_news = full_news[full_news['publish_date'] >= '2025-01-01']

full_news = feature_text_generate(full_news)



df = pd.DataFrame() 

for ticker in full_data[TICKER_COLUMN].unique():
    engineer = PriceFeatureEngineer()

    df_times = full_data[full_data[TICKER_COLUMN] == ticker]

    df = pd.concat([df, engineer.combine_full_df(df_times)])  


#Оставляю даныне с 2025 гола
df = df[df['begin'] >= '2025-01-01']



df = df.merge(full_news, how = 'left', left_on=['ticker', 'begin'], right_on=['ticker', 'publish_date'])

df  = df.drop('publish_date', axis = 1)
df[['sentiment_mean', 'sentiment_sum',
       'sentiment_count', 'word_count_mean', 'word_count_sum',
       'sanctions_count', 'central_bank_count', 'war_count', 'dividends_count',
       'news_count', 'sanctions_ratio', 'central_bank_ratio', 'war_ratio',
       'dividends_ratio']] = df[['sentiment_mean', 'sentiment_sum',
       'sentiment_count', 'word_count_mean', 'word_count_sum',
       'sanctions_count', 'central_bank_count', 'war_count', 'dividends_count',
       'news_count', 'sanctions_ratio', 'central_bank_ratio', 'war_ratio',
       'dividends_ratio']].fillna(0)



df['data_type'] = np.where(df['begin'] == DATE_FOR_PREDICTIONS , 'test', 'train')
df = df[df['begin'] <= DATE_FOR_PREDICTIONS]



# ========== ПОДГОТОВКА ДАННЫХ ДЛЯ ОБУЧЕНИЯ ==========
print('[INFO] Подготовка к обучению')
X_train, X_test, y_train = _model_selection(df)


models = {}
results = {}

horizons = range(1, 21)
models = {}


param_rand = [
    {
        'models': [
            LGBMRegressor(random_state=SEED, verbose=-1)
        ],
        'models__boosting_type' : ['gbdt', 'dart'],
        'models__max_depth': range(5, 7),
        'models__learning_rate': [0.01, 0.05, 0.1],
        'models__n_estimators': range(700, 1500, 100),
        'models__subsample': [0.8, 0.9, 1.0],
        'models__colsample_bytree': [0.7, 0.8, 0.9, 1.0],
        'models__min_child_samples':  [1, 5, 10], 
        'preprocessor__num__simple_num__strategy': ['mean', 'median', 'constant'],
        'preprocessor__num__scale': [StandardScaler(), MinMaxScaler(), RobustScaler(), 'passthrough']
    }
]




# # ========== ОБУЧЕНИЕ МОДЕЛИ ==========
# for i in tqdm(horizons, desc="Обучение моделей"):
#     print(f"\n{'='*50}")
#     print(f"Обучение модели для прогноза на {i} дней вперед")
#     print(f"{'='*50}")
    
#     # Выбираем соответствующий таргет
#     target_col = f'target_{i}'
    
#     # Убеждаемся, что данные очищены для этого таргета
#     mask = y_train[target_col].notna()
#     X_train_clean = X_train[mask]
#     y_train_clean = y_train.loc[mask, target_col]
    
#     print(f"Размер тренировочных данных для target_{i}: {X_train_clean.shape}")
    
#     current_pipe = create_pipeline(df)  

#     tscv = TimeSeriesSplit(n_splits=5, test_size=min(100, len(X_train_clean)//5))
    
#     rand_regression = RandomizedSearchCV(
#         estimator=current_pipe,
#         param_distributions=param_rand,
#         n_iter=5,
#         cv=tscv,
#         random_state=SEED,
#         n_jobs=-1,
#         scoring='neg_mean_absolute_error',
#         verbose=1
#     )
    
#     # Обучение модели
#     rand_regression.fit(X_train_clean, y_train_clean)
    
#     # Сохраняем модель
#     models[f'model_{i}'] = rand_regression.best_estimator_
    
#     # Сохраняем результаты
#     results[f'target_{i}'] = {
#         'best_score': -rand_regression.best_score_,
#         'best_params': rand_regression.best_params_,
#         'model': rand_regression.best_estimator_
#     }
    
#     joblib.dump(rand_regression.best_estimator_, f'models_weight/model_target_{i}.pkl')
#     print(f"Лучшая MAE: {-rand_regression.best_score_:.4f}")

def check_models_status(horizons):
    """Проверяет статус сохраненных моделей"""
    models_count = 0
    for i in horizons:
        model_path = f'models_weight/model_target_{i}.pkl'
        params_path = f'models_weight/params_target_{i}.pkl'
        if os.path.exists(model_path) and os.path.exists(params_path):
            models_count += 1
    
    print(f"Найдено сохраненных моделей: {models_count} из {len(horizons)}")
    if models_count == len(horizons):
        print("РЕЖИМ: Переобучение существующих моделей")
    elif models_count == 0:
        print("РЕЖИМ: Первоначальное обучение всех моделей") 
    else:
        print("РЕЖИМ: Смешанный - некоторые модели будут переобучены, некоторые созданы заново")
    return models_count

# Проверка статуса в начале
print("Проверка существующих моделей...")
existing_count = check_models_status(horizons)




import os
import joblib
from tqdm import tqdm

# ========== ИНИЦИАЛИЗАЦИЯ СЛОВАРЕЙ ДЛЯ МОДЕЛЕЙ ==========
models = {}  # Этот словарь будем использовать для предсказаний
results = {}

# ========== ПОДГОТОВКА ПАПКИ ДЛЯ СОХРАНЕНИЯ ==========
os.makedirs('models_weight', exist_ok=True)

# ========== ПРОВЕРКА СУЩЕСТВОВАНИЯ МОДЕЛЕЙ ==========
existing_models = {}
for i in horizons:
    model_path = f'models_weight/model_target_{i}.pkl'
    params_path = f'models_weight/params_target_{i}.pkl'
    
    # Проверяем, есть ли и модель, и параметры
    if os.path.exists(model_path) and os.path.exists(params_path):
        existing_models[i] = {
            'model_path': model_path,
            'params_path': params_path
        }

# ========== ФУНКЦИЯ ДЛЯ ПРОВЕРКИ СТАТУСА МОДЕЛЕЙ ==========
def check_models_status(horizons):
    """Проверяет статус сохраненных моделей"""
    models_count = 0
    for i in horizons:
        model_path = f'models_weight/model_target_{i}.pkl'
        params_path = f'models_weight/params_target_{i}.pkl'
        if os.path.exists(model_path) and os.path.exists(params_path):
            models_count += 1
    
    print(f"Найдено сохраненных моделей: {models_count} из {len(horizons)}")
    if models_count == len(horizons):
        print("РЕЖИМ: Переобучение существующих моделей")
    elif models_count == 0:
        print("РЕЖИМ: Первоначальное обучение всех моделей") 
    else:
        print("РЕЖИМ: Смешанный - некоторые модели будут переобучены, некоторые созданы заново")
    return models_count

# Проверка статуса в начале
print("Проверка существующих моделей...")
existing_count = check_models_status(horizons)

# ========== АЛГОРИТМ ОБУЧЕНИЯ ==========
for i in tqdm(horizons, desc="Обучение моделей"):
    print(f"\n{'='*50}")
    print(f"Обучение модели для прогноза на {i} дней вперед")
    print(f"{'='*50}")
    
    # Подготовка данных
    target_col = f'target_{i}'
    mask = y_train[target_col].notna()
    X_train_clean = X_train[mask]
    y_train_clean = y_train.loc[mask, target_col]
    
    print(f"Размер тренировочных данных для target_{i}: {X_train_clean.shape}")
    
    model_path = f'models_weight/model_target_{i}.pkl'
    params_path = f'models_weight/params_target_{i}.pkl'
    
    # СЦЕНАРИЙ 1: Модель и параметры уже существуют - переобучаем с фиксированными параметрами
    if i in existing_models:
        print(f"Найдены сохраненные параметры для target_{i}. Переобучаем модель...")
        
        try:
            # Загружаем сохраненные гиперпараметры
            best_params = joblib.load(params_path)
            print(f"Загружены гиперпараметры: {best_params}")
            
            # Создаем новый пайплайн с фиксированными параметрами
            current_pipe = create_pipeline(df)
            current_pipe.set_params(**best_params)
            
            # Обучаем на текущих данных
            current_pipe.fit(X_train_clean, y_train_clean)
            
            # Сохраняем модель в словарь для последующего использования
            model_key = f'model_{i}'
            models[model_key] = current_pipe
            
            # Сохраняем результаты
            results[f'target_{i}'] = {
                'best_params': best_params,
                'model': current_pipe,
                'retrained': True
            }
            
            # Обновляем сохраненную модель
            joblib.dump(current_pipe, model_path)
            print(f"Модель переобучена с сохраненными гиперпараметрами")
            
        except Exception as e:
            print(f"Ошибка при загрузке параметров: {e}. Запускаем поиск заново.")
            # Если ошибка - переходим к сценарию 2
            del existing_models[i]  # Удаляем из существующих для пересоздания
    
    # СЦЕНАРИЙ 2: Модели нет или произошла ошибка - проводим полный поиск
    if i not in existing_models:
        print(f"Поиск оптимальных гиперпараметров для target_{i}...")
        
        current_pipe = create_pipeline(df)  
        tscv = TimeSeriesSplit(n_splits=5, test_size=min(100, len(X_train_clean)//5))
        
        rand_regression = RandomizedSearchCV(
            estimator=current_pipe,
            param_distributions=param_rand,
            n_iter=5,
            cv=tscv,
            random_state=SEED,
            n_jobs=-1,
            scoring='neg_mean_absolute_error',
            verbose=1
        )
        
        # Обучение с поиском гиперпараметров
        rand_regression.fit(X_train_clean, y_train_clean)
        
        # Сохраняем лучшую модель и параметры
        best_model = rand_regression.best_estimator_
        best_params = rand_regression.best_params_
        
        # Сохраняем модель в словарь для последующего использования
        model_key = f'model_{i}'
        models[model_key] = best_model
        
        results[f'target_{i}'] = {
            'best_score': -rand_regression.best_score_,
            'best_params': best_params,
            'model': best_model,
            'retrained': False
        }
        
        # Сохраняем модель и параметры отдельно
        joblib.dump(best_model, model_path)
        joblib.dump(best_params, params_path)
        
        print(f"Новая модель сохранена. Лучшие параметры: {best_params}")

print(f"\n{'='*50}")
print("СТАТУС ОБУЧЕНИЯ:")
for i in horizons:
    model_key = f'model_{i}'
    if model_key in models:
        if i in existing_models and results.get(f'target_{i}', {}).get('retrained', False):
            print(f"target_{i}: ПЕРЕОБУЧЕНА с сохраненными параметрами")
        else:
            print(f"target_{i}: НОВАЯ МОДЕЛЬ с подобранными параметрами")
    else:
        print(f"target_{i}: ОШИБКА - модель не создана!")
print(f"{'='*50}")

# ========== ПРОВЕРКА ЧТО ВСЕ МОДЕЛИ В СЛОВАРЕ ==========
print(f"\nПроверка словаря моделей:")
print(f"Всего моделей в словаре: {len(models)}")
for key in models.keys():
    print(f"  - {key}")



import os 
import joblib 
# ========== ПРЕДСКАЗАНИЕ ==========
def predict_on_test_set(X_test, models_dict):
    """
    Делает прогнозы на тестовом наборе данных с использованием всех обученных моделей

    Parameters:
    X_test: тестовые данные
    models_dict: словарь с обученными моделями
    
    Returns:
    DataFrame с прогнозами для всех горизонтов
    """
    predictions = pd.DataFrame(index=X_test.index)
    predictions['ticker'] = X_test['ticker']
    
    print("Начинаем прогнозирование на тестовых данных...")
    for i in tqdm(horizons, desc="Прогнозирование"):
        model_key = f'model_{i}'
        target_col = f'p{i}'
        
        if model_key in models_dict:
            # Делаем прогноз
            pred = models_dict[model_key].predict(X_test)
            predictions[target_col] = pred/X_test['close'] - 1
        else:
            print(f"Внимание: Модель {model_key} не найдена!")
    
    predictions['ticker'] = X_test['ticker']

    
    return predictions

# Использование:
test_predictions = predict_on_test_set(X_test, models)


from datetime import datetime, timedelta

def find_weekend_dates(DATE_FOR_PREDICTIONS):
    base_date = datetime.strptime(DATE_FOR_PREDICTIONS, '%Y-%m-%d %H:%M:%S')
    weekend_numbers = []
    
    for i in range(1, 21):
        current_date = base_date + timedelta(days=i)
        if current_date.weekday() in [5, 6]: 
            weekend_numbers.append(f'p{i}')
    
    return weekend_numbers


week = find_weekend_dates(DATE_FOR_PREDICTIONS)

# ========== СОХРАНЕНИЕ РЕЗУЛЬТАТА ==========

def check_week(PREDICT_TYPE):
    if PREDICT_TYPE == 'WEEK_OFF':
        test_predictions[week] = np.nan
    else:
        pass

    test_predictions.to_csv('submit.csv', index = False)


check_week(PREDICT_TYPE)