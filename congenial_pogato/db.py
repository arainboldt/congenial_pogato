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

    def __init__(self, dbname=None, user=None, password=None, host=None, port=None, sslmode=None, sslrootcert=None):
        self.dbname = dbname
        self.name = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.sslmode = sslmode
        self.sslrootcert = sslrootcert
        self.cache = {'table':{},'schema':{}}
        self.conn_ = pg.connect(dbname=self.dbname, user=self.user, host=self.host, port=self.port,
                                password=self.password, sslmode=self.sslmode, sslrootcert=self.sslrootcert)
        self._map()

    @property
    def conn(self):
        if self.conn_.status == pg.extensions.STATUS_IN_TRANSACTION:
            self.conn_.rollback()
        if not try_poll(self.conn_):
            self.conn_ = pg.connect(dbname=self.dbname, user=self.user, host=self.host, port=self.port,
                                    password=self.password)
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
            cur.copy_from(output, f'{schema}.{table}', null="", columns=cols)
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(e)
        cur.close()
        return

    def __getattr__(self,attr):
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









