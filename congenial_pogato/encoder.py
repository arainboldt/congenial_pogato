from psycopg2 import Binary


def pg_encode(val):
    return Binary(val).getquoted()

def byte_encode_df(df,pg_dtypes):
    if (pg_dtypes == 'bytea').any():
        for col, series in df.loc[:,(pg_dtypes == 'bytea')].iteritems():
            df.loc[:, col] = series.apply(pg_encode)
    return df

