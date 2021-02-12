import pandas as pd
from .util import *
from .command_templates import *

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
        self.columns_ = pd.DataFrame(self.db.execute(cmd), columns=['data_type', 'column_name'])
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
    def conf(self):
        pass

    def create_from_df(self,df,schema_name='unknown'):
        if (self.schema is None) and (schema_name is None):
            raise ValueError('Schema Must Be Provided')
        elif schema_name in [None,'unknown']:
            schema_name = self.schema
        conf = gen_pg_conf(df)
        cmd = create_table_cmd.format(schema_name=schema_name,table_name=self.name,conf=conf)
        self.db.execute(cmd)
        self.db.tree.loc[self.schema].append(self.name)

    def write(self,data,args=[],kwargs={},overwrite=False):
        if overwrite:
            self.delete(args,kwargs)
        df = self.rectify(data)
        self.db.insert(df,self.schema,self.name)

    def update(self,data,args=[],kwargs={}):
        df = self.rectify(data)
        #todo: find the fastest and most elegant way to do this
        pass

    def delete(self,args=[],kwargs={}):
        where = parse_arg_statement(args, kwargs)
        if where:
            cmd = delete_where_cmd.format(schema_name=self.schema,table_name=self.name,where=where)
        else:
            cmd = delete_cmd.format(schema_name=self.schema, table_name=self.name)
        self.db.execute(cmd)

    def grab(self,args=[],kwargs={}):
        where = parse_arg_statement(args, kwargs)
        if where:
            cmd = select_where_cmd.format(schema_name=self.schema,table_name=self.name,where=where)
        else:
            cmd = select_cmd.format(schema_name=self.schema, table_name=self.name)
        self.db.execute(cmd)

    def _purge(self):
        pass

    def rectify(self,data):
        df = swiss_typist(data,self.py_dtypes_)
        return df




