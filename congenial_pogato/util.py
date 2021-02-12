def __getattr__(self, attr):
    if not hasattr(self, attr):
        print(f'Name {attr} not present in {self.name}')
        return
    return self.__dict__[attr]


def __getattribute__(self, attr):
    if not hasattr(self, attr):
        print(f'Name {attr} not present in {self.name}')
        return
    return self.__dict__[attr]