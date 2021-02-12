

class Schema(object):

    def __init__(self,name,db):
        self.name = name
        self.db = db

    def create_table(self,table_name,df=None,conf=None):
        pass

    @property
    def table_names(self):
        pass

