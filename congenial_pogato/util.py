import pandas as pd

py_col_dtypes = {
    'text':'str',
    'timestamp without time zone':'datetime',
    'timestamp with time zone':'tz_datetime',
    'integer':'int',
    'numeric':'float',
    'boolean':'bool',
    'character':'str',
    'date':'date',
    'json':'dict',
    'bigint':'int',
    'bytea':'bytes'
}

pg_col_dtypes = {
    
    'str':'text',
    'object':'text',
    'datetime[ns]':'timestamp without time zone',
    'int':'integer',
    'float':'numeric',
    'bool':'boolean',
    'date':'date',
    'dict':'json',


}


class PGTypeDict(dict):

    def __init__(self):
        self._dict_ = pd.Series(pg_col_dtypes)

    def __getitem__(self, item):
        if item in self._dict_:
            return self._dict_.at[item]
        elif 'datetime' in item:
            if '[ns,' in item:
                return 'timestamp with time zone'
            else:
                return 'timestamp without time zone'
        else:
            for true_key in ['int','float','bool','dict','object','str']:
                if true_key in item:
                    return self._dict_[true_key]






def swiss_typist(df,pydtypes):
    for col, dtype in pydtypes.iteritems():
        try:
            if 'date' in dtype:
                df[col] = pd.to_datetime(df[col],infer_datetime_format=True)
            else:
                df[col] = df[col].astype(dtype)
        except:
            pass
    return df
