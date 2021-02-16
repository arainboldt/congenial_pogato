import pandas as pd
from .util import *
from .command_templates import *
from .parse import *

pgtyper = PGTypeDict()

def check_exists(func):

    def wrap(cls,*args,**kwargs):
        if (not cls.bound) and (func.__name__ == 'write'):
            if ('data' in kwargs):
                data = kwargs['data']
            elif isinstance(args[0],pd.DataFrame):
                data = args[0]
            else:
                raise ValueError(f'Table {cls.name} does not exist, \
                        it must be created before it can be operated upon')
            schema = kwargs['schema']
            cls.create_from_df(data,schema)
        return func(cls,*args,**kwargs)

    return wrap

def gen_pg_conf(df):
    table_conf = ', '.join([f' {col} {pgtyper[str(series.dtype)]}' for col,series in df.iteritems()])
    return table_conf

class Table(object):

    def __init__(self,name,db,schema=None):
        self.name = name
        self.db = db
        self.schema_ = schema

    @property
    def schema(self):
        if self.schema_ is None:
            self.schema_ = self.db.get_schema(self.name)
        return self.schema_

    def _load_col_dtypes(self):
        cmd = f""" SELECT data_type,column_name FROM information_schema.columns 
                    WHERE table_name = '{self.name}'
        """
        self.columns_ = pd.DataFrame(self.db.execute(cmd, output=True), columns=['data_type', 'column_name'])
        self.columns_.index = self.columns_.column_name
        self.data_types_ = self.columns['data_type']
        self.py_dtypes_ = self.columns['data_type'].replace(py_col_dtypes)
        self.columns_ = self.columns['column_name']


    @property
    def columns(self):
        if not hasattr(self,'columns_'):
            self._load_col_dtypes()
        return self.columns_

    @property
    def dtypes(self):
        if not hasattr(self,'data_types_'):
            self._load_col_dtypes()
        return self.data_types_

    @property
    def py_dtypes(self):
        if not hasattr(self,'py_dtypes_'):
            self._load_col_dtypes()
        return self.py_dtypes_

    @property
    def conf(self):
        pass

    @property
    def bound(self):
        return self.db.is_table(self.name)

    def create_from_df(self,df,schema_name='unknown'):
        if (self.schema is None) and (schema_name is None):
            raise ValueError('Schema Must Be Provided')
        elif schema_name in [None,'unknown']:
            schema_name = self.schema
        conf = gen_pg_conf(df)
        cmd = create_table_cmd.format(schema_name=schema_name,table_name=self.name,table_conf=conf)
        self.schema_ = schema_name
        self.db.execute(cmd)
        self.db.tree.loc[self.schema].append(self.name)

    @check_exists
    def write(self,data,schema=None,overwrite=False,*args,**kwargs):
        if overwrite:
            self.delete(args,kwargs)
        df = self.rectify(data)
        self.db.insert(df,self.schema,self.name)

    @check_exists
    def update(self,data,args=[],kwargs={}):
        df = self.rectify(data)
        #todo: find the fastest and most elegant way to do this
        pass

    @check_exists
    def delete(self,*args, **kwargs):
        where = Where(valid_cols=self.columns,*args, **kwargs)
        cmd = delete_cmd.format(schema_name=self.schema,table_name=self.name,where=where)
        self.db.execute(cmd)

    @check_exists
    def grab(self,*args,**kwargs):
        where = Where(valid_cols=self.columns,*args, **kwargs)
        cmd = select_cmd.format(schema_name=self.schema, table_name=self.name, where=where)
        data = self.db.execute(cmd,output=True)
        return self.rectify( pd.DataFrame(data,columns=self.columns) )

    def _purge(self):
        pass

    def rectify(self,data):
        df = swiss_typist(data,self.py_dtypes)
        return df






