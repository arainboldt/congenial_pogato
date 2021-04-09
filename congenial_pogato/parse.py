import pandas as pd
import datetime as dt

beginning_of_time = dt.datetime(1979,1,1)



def iterable(val):
    try:
        iter(val)
        return True
    except:
        return False

def datetime_like(val):
    if isinstance(val,(pd.Timestamp,dt.datetime,dt.date)):
        return True
    else:
        try:
            datish = pd.to_datetime(val, infer_datetime_format=True)
            return datish > beginning_of_time
        except:
            return False

def listlike(val):
    if isinstance(val,str):
        return False
    elif iterable(val):
        return True

def dtyper(dtype):
    if 'null' in dtype:
        return None
    elif 'list' in dtype:
        sub_typer = dtyper(dtype.replace('list',''))
        return type_lister(sub_typer)
    elif any(date_name in dtype for date_name in ['date','datetime','timestamp']):
        return datetimer
    elif 'int' in dtype:
        return int
    elif 'str' in dtype:
        return tostr
    elif 'float' in dtype:
        return float
    elif 'bool' in dtype:
        return bool
    elif 'bytes' in dtype:
        return bytearr

def null_pass(val):
    return None

def bytearr(val):
    return f"'{val}'::bytea"

def tostr(val):
    return f"'{val}'"

def type_lister(typer):
    def typecast(val):
        return '(' + ', '.join([typer(v) for v in val]) + ')'
    return typecast

def datetimer(val):
    try:
        date_val = pd.to_datetime(val, infer_datetime_format=True).strftime('%Y-%m-%d')
        return f"'{date_val}'" + '::date'
    except:
        return f"'{date_val}'" + '::date'

def rel_code(rel_val):
    if isinstance(rel_val,(int,float)):
        rel_val = int(rel_val)
        if rel_val == 0:
            return '='
        elif rel_val == 1:
            return '>'
        elif rel_val == 10:
            return '>='
        elif rel_val == -1:
            return '<'
        elif rel_val == -10:
            return '<='
        elif rel_val == 2:
            return 'IN'
        elif rel_val == 3:
            return 'LIKE'
        elif rel_val == 31:
            return 'ILIKE'
    if isinstance(rel_val,str):
        rel_val = rel_val.strip().lower()
        if 'eq' in rel_val:
            return '='
        elif 'gt' in rel_val:
            return '>'
        elif 'gte' in rel_val:
            return '>='
        elif 'lt' in rel_val:
            return '<'
        elif 'lte' in rel_val:
            return '<='
        elif 'in' in rel_val:
            return 'IN'
        elif 'like' in rel_val:
            return 'LIKE'
        elif 'ilike' in rel_val:
            return 'ILIKE'

def get_type(val):
    if listlike(val):
        if len(val) == 0:
            return 'list-null'
        subtype = get_type(val[0])
        return f'list-{subtype}'
    elif isinstance(val,float):
        return 'float'
    elif isinstance(val,bool):
        return 'bool'
    elif isinstance(val,int):
        return 'int'
    elif datetime_like(val):
        return 'date'
    elif isinstance(val,str):
        return 'str'
    elif isinstance(val,bytes):
        return 'bytes'

class Where(object):

    def __init__(self,*args,**kwargs):
        self.valid_cols = kwargs.pop('valid_cols',[])
        self.args = args
        self.kwargs = kwargs


    def __str__(self):
        arg_statement = []
        if (len(self.args) > 0) or (len(self.kwargs) > 0):
            if (len(self.args) > 0):
                arg_str = self.parse_args(self.args)
                if arg_str:
                    arg_statement.append( arg_str )
            if len(self.kwargs) > 0:
                kwarg_str = self.parse_arg_dict(self.kwargs)
                if kwarg_str:
                    arg_statement.append( kwarg_str )
            if len(arg_statement) > 0:
                arg_statement = ' AND '.join(arg_statement)
                return f""" WHERE {arg_statement} """
        return ''

    def parse_args(self,arg):
        if isinstance(arg,dict):
            return self.parse_arg_dict(arg)
        elif isinstance(arg,(list,tuple)):
            return self.parse_arg_list(arg)

    def parse_arg_dict(self,arg_dict):
        arg_statement = []
        for k,v in arg_dict.items():
            col, rel = self.parse_k(k)
            if not col in self.valid_cols:
                print(f'Dropping invalid column {col}')
                continue
            typer_func = dtyper(get_type(v))
            if typer_func:
                print(typer_func,v,typer_func(v))
                arg_statement.append( f"{col} {rel} " + str(typer_func(v)) )
        if len(arg_statement) > 0:
            return ' AND '.join(arg_statement)
        return None

    def parse_arg_list(self,arg_list):
        arg_statement = []
        for arg in arg_list:
            if not arg[0] in self.valid_cols:
                print(arg[0])
                continue
            if listlike(arg[2]):
                subtyper = dtyper(get_type(arg[2][0]))
                v = type_lister(subtyper)(v)
            else:
                typer = dtyper(get_type(arg[2]))
                v = typer(arg[2])
            arg_component = f"{arg[0]} {rel_code(arg[1])} " + v
            if len(arg_component) > 0:
                arg_statement.append( arg_component )
        if len(arg_statement) > 0:
            return ' AND '.join(arg_statement)
        return None

    def parse_k(self,k):
        k_split = k.split('__',-1)[:2]
        if len(k_split) == 2:
            return k_split[0], rel_code(k_split[1])
        elif len(k_split) == 1:
            return k_split[0], '='
        else:
            raise(f'Query argument {k} has too many components, max components is 2: col_name, relation, dtype')



