

class Table(object):

    def __init__(self,name,db,schema=None):
        self.name = name
        self.db = db
        self.schema = schema

    @classmethod
    def from_df(self,df):
        pass

    @property
    def columns(self):
        pass

    @property
    def conf(self):
        pass

    def replace_records(self,data,args=[]):
        pass

    def delete_records(self,args=[]):
        pass

    def grab_records(self,args=[]):
        pass

    def execute(self,query_str):
        pass

    def _purge(self):
        pass



