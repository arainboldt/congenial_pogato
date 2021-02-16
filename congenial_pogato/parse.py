


def datetime_like(val):
    if isinstance(val,(pd.Timestamp,dt.datetime,dt.date)):
        return val
    else:
        try:
            return pd.to_datetime(val,infer_datetime_format=True)
        except:
            return val

def dtyper(dtype):
    if dtype in ['date','datetime','timestamp']:
        return datetimer
    elif 'int' in dtype:
        return int
    elif 'str' in dtype:
        return str
    elif 'float' in dtype:
        return float

def datetimer(val):
    try:
        date_val = pd.to_datetime(val, infer_datetime_format=True)
        return f"'{date_val}'::date"
    except:
        return f"'{val}'::date"

class Where(object):

    def __init__(self,args=None,**kwargs):
        if isinstance(args,list):
            self.args = args
        elif isinstance(args,dict):
            self.args = [args]
        self.kwargs = kwargs

    def __str__(self):
        arg_statement = self.parse_args(self.args)
        if len(self.kwargs) > 0:
            arg_statement += f'& {self.parse_arg_dict(self.kwargs)}'
        return arg_statement

    def parse_args(self,arg):
        if isinstance(arg,dict):
            return self.parse_arg_dict(arg)
        elif isinstance(arg,(list,tuple)):
            return self.parse_arg_list(arg)

    def parse_arg_dict(self,arg_dict):
        arg_statement = []
        for k,v in arg_dict.items():
            col, rel, typer_func = self.parse_k(k)
            arg_statement.append(f"{col} {rel} {typer_func(v)}")
        return ' & '.join(arg_statement)

    def parse_arg_list(self,arg_list):
        arg_statement = []
        for arg in arg_list:
            arg_component = ''
            if isinstance(datetime_like(arg[2]),pd.Timestamp):
                v = dtyper('datetime')(arg[2])
            else:
                v = arg[2]
            arg_component = f""" {arg[0]} {rel_code(arg[1])} {v}"""
            arg_statement.append( arg_component )
        return ' & '.join(arg_statement)

    def parse_k(self,k):
        k_split = k.split('__',-1)
        if len(k_split) == 3:
            return k_split[0], rel_code(k_split[1]), dtyper(k_split[2])
        elif len(k_split) == 2:
            return k_split[0], rel_code(k_split[1]), None
        elif len(k_split) == 1:
            return k_split[0], '=', None
        else:
            raise(f'Query argument {k} has too many components, max components is 3: col_name, relation, dtype')



