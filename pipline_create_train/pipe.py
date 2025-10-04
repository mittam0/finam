from sklearn.preprocessing import StandardScaler, MinMaxScaler, RobustScaler, LabelEncoder, OrdinalEncoder
from sklearn.model_selection import TimeSeriesSplit, RandomizedSearchCV
from lightgbm import LGBMRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
import numpy as np
import pandas as pd


def _model_selection(df):
    df = df.sort_values(['begin'])
    df = df.set_index('begin')  


    X_train = df[df['data_type'] == 'train'].drop('data_type', axis=1)
    X_test = df[df['data_type'] == 'test'].drop('data_type', axis=1)


    y_train = pd.DataFrame(index=X_train.index)
    for i in range(1, 21):
        y_train[f'target_{i}'] = X_train.groupby('ticker')['close'].shift(-i)


    common_idx = X_train.index.intersection(y_train.dropna().index)
    X_train = X_train.loc[common_idx]
    y_train = y_train.loc[common_idx]

    return X_train, X_test, y_train



def create_pipeline(df):
    
    X_train, X_test, y_train = _model_selection(df)
    col_number = X_train.select_dtypes(include='number').columns.tolist()
    col_ordinal = ['ticker']
    ticker_categories = [X_train['ticker'].unique().tolist()]

    pipe_num = Pipeline([
        ('simple_num', SimpleImputer(missing_values=np.nan, strategy='constant', fill_value=0)),
        ('scale', StandardScaler())
    ])

    pipe_ord = Pipeline([
        ('simple_ord_before', SimpleImputer(missing_values=np.nan, strategy='constant', fill_value='NEW')),
        ('ord_encoder', OrdinalEncoder(categories=ticker_categories, 
                                       handle_unknown='use_encoded_value', unknown_value=-1)),
        ('simple_ord_after', SimpleImputer(missing_values=np.nan, strategy='most_frequent'))
    ])

    transform_features = ColumnTransformer([
        ('num', pipe_num, col_number),
        ('ord', pipe_ord, col_ordinal)
    ], remainder='passthrough')

    pipe_final = Pipeline([
        ('preprocessor', transform_features),
        ('models', LGBMRegressor(random_state=42, n_estimators=500, n_jobs=-1, max_depth=7))
    ])
    
    return pipe_final