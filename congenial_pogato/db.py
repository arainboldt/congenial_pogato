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


def check_poll_status(conn):
    """
    extensions.POLL_OK == 0
    extensions.POLL_READ == 1
    extensions.POLL_WRITE == 2
    """

    if conn.poll() == pg.extensions.POLL_OK:
        print ("POLL: POLL_OK")
    if conn.poll() == pg.extensions.POLL_READ:
        print ("POLL: POLL_READ")
    if conn.poll() == pg.extensions.POLL_WRITE:
        print ("POLL: POLL_WRITE")
    return conn.poll()

def get_transaction_status(conn):

    # print the connection status
    print ("\nconn.status:", conn.status)

    # evaluate the status for the PostgreSQL connection
    if conn.status == pg.extensions.STATUS_READY:
        print ("psycopg2 status #1: Connection is ready for a transaction.")

    elif conn.status == pg.extensions.STATUS_BEGIN:
        print ("psycopg2 status #2: An open transaction is in process.")

    elif conn.status == pg.extensions.STATUS_IN_TRANSACTION:
        print ("psycopg2 status #3: An exception has occured.")
        print ("Use tpc_commit() or tpc_rollback() to end transaction")

    elif conn.status == pg.extensions.STATUS_PREPARED:
        print ("psycopg2 status #4:A transcation is in the 2nd phase of the process.")
    return conn.status

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
        if self.conn_.status == pg.extensions.STATUS_IN_TRANSACTION:
            self.conn_.rollback()
        if not try_poll(self.conn_):
            self.conn_ = pg.connect(dbname=self.dbname, user=self.user, host=self.host, port=self.port,
                                    password=self.password)
        return self.conn_

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









