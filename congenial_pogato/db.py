import psycopg2 as pg
import pandas as pd
from .schema import Schema
from .table import Table
from io import StringIO
from functools import wraps

def try_poll(conn):
    try:
        conn.poll()
        return True
    except:
        return False

def check_con(max_retry=3):
    def decorator(func):
        @wraps(func)
        def wrapper(self,*args,**kwargs):
            for _ in range(max_retry):
                try:
                    resp = func(self,*args,**kwargs)
                    return resp
                except pg.errors.ConnectionException as e:
                    print(e)
            raise e
        return wrapper
    return decorator

class DB(object):
    status_dict = {1: 'STATUS_READY', 2: 'STATUS_BEGIN', 5: 'STATUS_PREPARED', }
    special_attrs = ['tree', 'schema_tables', 'table_column_map']

    def __init__(self, 
                dbname=None, 
                user=None, 
                password=None, 
                host=None, 
                port=None, 
                sslmode=None, 
                sslrootcert=None,
                db_uri=None,
                application_name=None
        ):
        self.dbname = dbname
        self.name = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.sslmode = sslmode
        self.sslrootcert = sslrootcert
        self.db_uri = db_uri
        self.application_name = application_name
        self.cache = {'table':{},'schema':{}}
        if all([x is not None for x in [self.db_uri, self.application_name]]):
            self.conn_ = pg.connect(self.db_uri, application_name=self.application_name)
        else:
            self.conn_ = pg.connect(dbname=self.dbname, user=self.user, host=self.host, port=self.port,
                                password=self.password, sslmode=self.sslmode, sslrootcert=self.sslrootcert)
        self._map()

    @property
    def conn(self):
        if self.conn_.status == pg.extensions.STATUS_IN_TRANSACTION:
            self.conn_.rollback()
        if not try_poll(self.conn_):
            if all([x is not None for x in [self.db_uri, self.application_name]]):
                self.conn_ = pg.connect(self.db_uri, application_name=self.application_name)
            else:
                self.conn_ = pg.connect(dbname=self.dbname, user=self.user, host=self.host, port=self.port,
                                    password=self.password, sslmode=self.sslmode, sslrootcert=self.sslrootcert)
        return self.conn_

    def close(self):
        """ close connection nicely :) """
        try:
            self.conn_.close()
        except Exception as e:
            print(e)

    def _map(self):
        cmd = """SELECT table_schema, table_name FROM information_schema.tables
                    WHERE table_schema not in ('information_schema','pg_catalog')
        """
        self.schema_tables = pd.DataFrame(self.execute(cmd,output=True), columns=['schema', 'table'])
        self.tree = pd.Series({schema: group.table.tolist() \
                               for schema, group in self.schema_tables.groupby('schema')})
        return self

    @property
    def column_map(self):
        if not hasattr(self, 'table_column_map'):
            self._map_columns()
        return self.table_column_map
    
    def _map_columns(self):
        cmd_fmt = """SELECT table_schema as schema, table_name, column_name, data_type as pg_dtype
                    FROM information_schema.columns
                    WHERE table_schema in {schemas}
                    AND table_name in {tables};"""        
        tables_tuple = '(' + ','.join([f"'{tbl}'" for tbl in self.schema_tables.table.unique()]) + ')'
        schemas_tuple = '(' + ','.join([f"'{sch}'" for sch in self.schema_tables.schema.unique()]) + ')'
        cmd = cmd_fmt.format(schemas=schemas_tuple, tables=tables_tuple)
        self.table_column_map = pd.DataFrame(self.execute(cmd, output=True), columns=['table_schema', 'table_name', 'column_name', 'pg_dtype'])
        return self

    def get_schema(self,table_name):
        schema_check = self.tree.astype(str).str.contains(f"'{table_name}'",regex=True)
        if schema_check.any():
            return schema_check.idxmax()
        return None

    def is_schema(self,name):
        return name in self.tree

    def is_table(self,name):
        return self.tree.astype(str).str.contains(f"'{name}'",regex=True).any()

    def exists(self,name):
        if self.is_table(name):
            return True
        elif self.is_schema(name):
            return True
        return False

    @check_con(max_retry=3)
    def execute(self,cmd,output=False):
        conn = self.conn
        cur = conn.cursor()
        res = None
        try:
            cur.execute(cmd)
            conn.commit()
            if output:
                res = cur.fetchall()
        except Exception as e:
            print('PG Execution Error')
            print(e)
            print(cmd)
            conn.rollback()
        cur.close()
        return res

    @check_con(max_retry=3)
    def insert(self,df,schema,table):
        conn = self.conn
        cur = conn.cursor()
        output = StringIO()
        df.to_csv(output, sep='\t', header=False, index=False)
        cols = df.columns.tolist()
        output.seek(0)
        try:
            cur.execute(f'SET search_path TO {schema}, public')
            cur.copy_from(output, table, null="", columns=cols)
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(e)
        cur.close()
        return

    def __getattr__(self,attr):
        if attr in self.special_attrs:
            return self.__dict__.get(attr)
        if self.is_table(attr):
            if not attr in self.cache['table']:
                self.cache['table'][attr] = Table(attr,self)
            return self.cache['table'][attr]
        elif self.is_schema(attr):
            if not attr in self.cache['schema']:
                self.cache['schema'][attr] = Schema(attr,self)
            return self.cache['schema'][attr]
        else:
            self.cache['table'][attr] = Table(attr, self)
            return self.cache['table'][attr]









