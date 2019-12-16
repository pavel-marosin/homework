from werkzeug.exceptions import NotFound
import pandas as pd
from db import get_db


def analyze_data(query, stat):
    conn = get_db()
    sql_query = pd.read_sql_query(query, conn)
    df = pd.DataFrame(sql_query, columns=['device_uuid', 'type', 'value', 'date_created'])
    if df.empty:
        raise NotFound
    try:
        if stat == "median":
            return df['value'].median()
        if stat == 'max':
            return df['value'].max()
        if stat == 'min':
            return df['value'].min()
        if stat == 'mean':
            return df['value'].mean()
        if stat == 'mode':
            return df['value'].mode()
        if stat == 'quarterlies':
            return df['value'].quantile([.25, .75])
    except Exception as e:
        raise e
