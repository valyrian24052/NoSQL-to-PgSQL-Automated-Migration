import pandas as pd

pd.set_option('future.no_silent_downcasting', True)

def handleInt(series):
    return pd.to_numeric(series, errors='coerce').astype(pd.Int64Dtype())


def handleDatetime(column):
    column = pd.to_datetime(column, errors='coerce', utc=True)
    column = column.dt.tz_convert(None)
    return column