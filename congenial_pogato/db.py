import psycopg2 as pg
import pandas as pd

class DB(object):
    status_dict = {1: 'STATUS_READY', 2: 'STATUS_BEGIN', 5: 'STATUS_PREPARED'}

    def __init__(self,dbname=None, user=None, password=None, host=None, port=None):

        self.dbname = dbname
        self.name = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self._map()

    @property
    def conn(self):
        if not hasattr(self,'conn_'):
            self.conn_ = pg.connect(dbname=self.name, user=self.user, host=self.host, port=self.port,
                              password=self.password)
        elif self.conn_.status == 2:
            self.conn_.rollback()

        return self.conn_

    def _map(self):
        cmd = """SELECT table_schema, table_name FROM information_schema.tables"""
        self.schema_tables = pd.DataFrame(self.execute(cmd), columns=['schema', 'table'])
        tree = {}
        for schema, group in self.schema_tables.groupby('schema'):
            tree[schema] = group.table.tolist()
        self.tree = pd.Series(tree)

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






