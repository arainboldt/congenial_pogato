import psycopg2 as pg
import pandas as pd
from .schema import Schema
from .table import Table
from io import StringIO

class DB(object):
    status_dict = {1: 'STATUS_READY', 2: 'STATUS_BEGIN', 5: 'STATUS_PREPARED'}

    def __init__(self,dbname=None, user=None, password=None, host=None, port=None):
        self.dbname = dbname
        self.name = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.cache = {'table':{},'schema':{}}
        self.conn_ = pg.connect(dbname=self.dbname, user=self.user, host=self.host, port=self.port,
                                password=self.password)
        self._map()

    @property
    def conn(self):
        return self.conn_

    def _map(self):
        cmd = """SELECT table_schema, table_name FROM information_schema.tables
                    WHERE table_schema not in ('information_schema','pg_catalog')
        """
        self.schema_tables = pd.DataFrame(self.execute(cmd), columns=['schema', 'table'])
        self.tree = pd.Series({schema: group.table.tolist() \
                               for schema, group in self.schema_tables.groupby('schema')})

    def get_schema(self,table_name):
        schema_check = self.tree.astype(str).str.contains(table_name,regex=True)
        if schema_check.any():
            return schema_check.idxmax()
        return None

    def is_schema(self,name):
        return name in self.tree

    def is_table(self,name):
        return self.tree.astype(str).str.contains(name,regex=True).any()

    def exists(self,name):
        if self.is_table(name):
            return True
        elif self.is_schema(name):
            return True
        return False

    def execute(self,cmd):
        conn = self.conn
        cur = conn.cursor()
        try:
            cur.execute(cmd)
            conn.commit()
            res = cur.fetchall()
        except Exception as e:
            print(e)
            res = None
            cur.rollback()
        cur.close()
        return res

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
            cur.rollback()
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







