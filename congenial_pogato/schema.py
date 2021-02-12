

class Schema(object):

    def __init__(self,name,db,bound=False):
        self.name = name
        self.db = db
        self.bound = bound

    def create_table(self,table_name,df=None,conf=None):
        pass

    @property
    def table_names(self):
        return self.db.tree.loc[self.name]


