import pandas as pd
class data:
    def __init__(self,path):
        self.path = path
    def get_list(self):
        df = pd.read_excel(self.path)
        return list(df['list'])