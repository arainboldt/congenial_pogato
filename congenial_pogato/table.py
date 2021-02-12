import pandas as pd
from .util import *

class Table(object):

    def __init__(self,name,db,schema=None):
        self.name = name
        self.db = db
        self.schema_ = schema

    @classmethod
    def from_df(self,df):
        pass

    @property
    def schema(self):
        if self.schema_ is None:
            self.schema_ = self.db.get_schema(self.name)
        return self.schema_

    def _load_coldtypes(self):
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
            self._load_coldtypes()
        return self.columns_

    @property
    def dtypes(self):
        if not hasattr(self,'data_types_'):
            self._load_coldtypes()
        return self.data_types_

    @property
    def conf(self):
        pass

    def write(self,data,args=[],kwargs={},overwrite=False):
        if overwrite:
            self.delete(args,kwargs)
        df = self.rectify(data)
        self.db.insert(df,self.schema,self.name)

    def delete(self,args=[],kwargs={}):
        where = parse_arg_statement(args, kwargs)
        if where:
            cmd = self.delete_where_cmd.format(schema_name=self.schema,table_name=self.name,where=where)
        else:
            cmd = self.delete_cmd.format(schema_name=self.schema, table_name=self.name)
        self.db.execute(cmd)

    def grab(self,args=[],kwargs={}):
        where = parse_arg_statement(args, kwargs)
        if where:
            cmd = self.select_where_cmd.format(schema_name=self.schema,table_name=self.name,where=where)
        else:
            cmd = self.select_cmd.format(schema_name=self.schema, table_name=self.name)
        self.db.execute(cmd)

    def _purge(self):
        pass

    def rectify(self,data):
        df = swiss_typist(data,self.py_dtypes_)
        return df




