from .util import *
from .command_templates import *
from .parse import *
from .encoder import *


#todo: Table.get_val ; rectify/typify on Table.write

from dataclasses import dataclass
from typing import List, Dict

@dataclass
class TableDef:
    schema: str
    name: str
    columns: List[str]
    pydtypes: Dict
    primary_key: str=None
    foreign_key: str=None

@dataclass
class AbstractTableDef:
    schema: str
    name: str
    columns: List[str]
    pydtypes: Dict
    variables: List[str]


pgtyper = PGTypeDict()

def get_manual_insert_command(schema_name, table_name, columns, values):
    values_str = ','.join([str(rec) for rec in values])
    cols = ', '.join(columns)
    return old_school_insert.format(schema_name=schema_name,
                                    table_name=table_name,
                                    columns=cols,
                                    values=values_str)

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
            schema = kwargs.pop('schema',None)
            cls.create_from_df(data,schema)
        return func(cls,*args,**kwargs)

    return wrap

def gen_pg_conf(df):
    table_conf = ', '.join([f' {col} {pgtyper[str(series.dtype)]}' for col,series in df.items()])
    return table_conf



def gen_pg_conf_from_table_def(table_def):
    table_conf = ', '.join([f' {col} {pgtyper[str(dtype)]}' for col,dtype in table_def.pydtypes.items()])
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
        #todo
        return 'No Conf'

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
        self.db._map()
        #self.db.tree.loc[self.schema].append(self.name)

    @staticmethod
    def create_cmd_from_def(table_def):
        conf = gen_pg_conf_from_table_def(table_def=table_def)
        return create_table_cmd.format(schema_name=table_def.schema,table_name=table_def.name,table_conf=conf)

    @staticmethod
    def create_from_def(table_def, db):
        conf = gen_pg_conf_from_table_def(table_def=table_def)
        cmd = create_table_cmd.format(schema_name=table_def.schema,table_name=table_def.name,table_conf=conf)
        db.execute(cmd)
        db._map()
        #db.tree.loc[table_def.schema].append(table_def.name)

    @check_exists
    def write(self, data, schema=None, overwrite=False, *args, **kwargs):
        if overwrite:
            self.delete(*args,**kwargs)
        df = self.rectify(data)
        if (self.dtypes == 'bytea').any():
            #df = byte_encode_df(df, self.dtypes)
            cmd = get_manual_insert_command(schema_name=self.schema,
                                           table_name=self.name,
                                           columns=self.columns.tolist(),
                                           values=df[self.columns].to_records())
            self.db.execute(cmd)
            return
        self.db.insert(df,self.schema,self.name)

    @check_exists
    def update(self, data, args=[],kwargs={}):
        df = self.rectify(data)
        #todo: find the fastest and most elegant way to do this
        pass

    @check_exists
    def delete(self, *args, **kwargs):
        kwargs.pop('overwrite', None)
        kwargs.pop('schema', None)
        where = Where(valid_cols=self.columns, *args, **kwargs)
        where = str(where)
        cmd = delete_cmd.format(schema_name=self.schema,table_name=self.name,where=where)
        self.db.execute(cmd)

    @check_exists
    def grab(self,*args,**kwargs):
        where = Where(valid_cols=self.columns, *args, **kwargs)
        cmd = select_cmd.format(schema_name=self.schema, table_name=self.name, where=where)
        data = self.db.execute(cmd,output=True)
        return self.rectify( pd.DataFrame(data,columns=self.columns) )

    @check_exists
    def grab_cols(self,col_names,index_name,*args,**kwargs):
        where = Where(valid_cols=self.columns, *args, **kwargs)
        cols = ', '.join(col_names + [index_name])
        cmd = select_subset_cmd.format(schema_name=self.schema,
                                       table_name=self.name,
                                       cols=cols,
                                       where=where)
        data = self.db.execute(cmd,output=True)
        cols = self.columns.intersection(pd.Index(col_names))
        return self.rectify( pd.DataFrame(data,columns=cols), validate=False ).set_index(index_name)


    @check_exists # todo: fix this func
    def grab_col(self,col_name,index_name,*args,**kwargs):
        where = Where(valid_cols=self.columns, *args, **kwargs)
        cols = f'{index_name}, {col_name}'
        cmd = select_subset_cmd.format(schema_name=self.schema,
                                       table_name=self.name,
                                       cols=cols,
                                       where=where)
        data = self.db.execute(cmd,output=True)
        data = self.rectify( pd.DataFrame(data,columns=[index_name,col_name]), validate=False )
        data.index = data[index_name].iloc[:,0].copy()
        data.index.name = 'index'
        return data

    @check_exists
    def unique(self,col_name,*args,**kwargs):
        where = Where(valid_cols=self.columns, *args, **kwargs)
        cmd = select_distinct_from_col_cmd.format(schema_name=self.schema,
                                                  table_name=self.name,
                                                  col_name=col_name,
                                                  where=where)
        data = self.db.execute(cmd,output=True)
        return swiss_typist(pd.Series(data),self.py_dtypes[col_name])

    @check_exists
    def val_exists(self,col_name,val):
        cmd = val_exists_cmd.format(schema_name=self.schema,
                                    table_name=self.name,
                                    col_name=col_name,
                                    val=val)
        exists = self.db.execute(cmd, output=True)
        if exists:
            return exists[0][0]
        return None

    @check_exists
    def min(self,col_name,*args,**kwargs):
        where = Where(valid_cols=self.columns, *args, **kwargs)
        cmd = select_min_from_col_cmd.format(schema_name=self.schema,
                                                  table_name=self.name,
                                                  col_name=col_name,
                                                  where=where)
        data = self.db.execute(cmd,output=True)
        if data:
            return data[0][0]
        return None

    @check_exists
    def max(self,col_name,*args,**kwargs):
        where = Where(valid_cols=self.columns, *args, **kwargs)
        cmd = select_max_from_col_cmd.format(schema_name=self.schema,
                                                  table_name=self.name,
                                                  col_name=col_name,
                                                  where=where)
        data = self.db.execute(cmd,output=True)
        if data:
            return data[0][0]
        return None

    def purge(self):
        cmd = drop_table_cmd.format(schema_name=self.schema, table_name=self.name)
        self.db.execute(cmd,output=False)

    def rectify(self,data,validate=False):
        if validate:
            assert set(self.columns.tolist()).issubset(data.columns.tolist())
        cols = data.columns.intersection(self.columns)
        df = swiss_typist(data[cols],self.py_dtypes.loc[cols])
        return df






