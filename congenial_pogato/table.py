import pandas as pd

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
        if not hasattr(self,'columns_'):
            cmd = f""" SELECT data_type,column_name FROM information_schema.columns 
                        WHERE table_name = {self.name}
            """
            self.columns_ = pd.DataFrame(self.db.execute(cmd),columns=['data_type','column_name'])
            self.columns_.index = self.columns_.column_name
            self.data_types_ = self.columns['data_type']
            self.columns_ = self.columns['column_name']
        return self.columns_

    @property
    def dtypes(self):
        if not hasattr(self,'columns_'):
            cmd = f""" SELECT data_type,column_name FROM information_schema.columns 
                        WHERE table_name = {self.name}
            """
            self.columns_ = pd.DataFrame(self.db.execute(cmd),columns=['data_type','column_name'])
            self.columns_.index = self.columns_.column_name
            self.data_types_ = self.columns['data_type']
            self.columns_ = self.columns['column_name']
        return self.data_types_

    @property
    def conf(self):
        pass

    def write(self,data,args=[]):
        pass

    def delete(self,args=[]):
        pass

    def grab(self,args=[]):
        pass

    def execute(self,query_str):
        pass

    def _purge(self):
        pass



