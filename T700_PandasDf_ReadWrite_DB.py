import pandas as pd
import mysql.connector              
import sqlalchemy

df = pd.read_excel(r"T700.xlsx")

class T700:

    def __init__(self,df):
        self.df = df

    def extract(self, event_type):
        df = self.df.set_index('AssetEventTypeCode')
        if event_type in df.index:
            return df.loc[[event_type]]

    def predict(self):     
        
        TakeoffDA = self.extract('Takeoff-AircraftEngine-EPS-2')        
        
        if TakeoffDA is None:
            TakeoffDA['qi'] = 'No record'
            TakeoffDA['task_usage'] = None
        
        else:
            try:
                TakeoffDA = TakeoffDA[TakeoffDA['StartDatetime'] >= TakeoffDA['InstallationDate'].iloc[-1]]
            except IndexError:
                TakeoffDA['qi'] = 'No installation record'
                TakeoffDA['task_usage'] = None           
            
         
            TakeoffDA = TakeoffDA.sort_values(by = 'StartDatetime')
            TakeoffDA = TakeoffDA.dropna(axis = 0,subset = ['TGT__MAR_WC_DEGC'])               
            
            if len(TakeoffDA.index) < 10:
                TakeoffDA['qi'] = 'Less than required number of data points for TGT Margin calculaiton'
                TakeoffDA['task_usage'] = None
            
            else:    
                TakeoffDA['qi'] = ''
                TakeoffDA['task_usage'] = float(TakeoffDA.TGT__MAR_WC_DEGC.tail(10).mean())
                TakeoffDA = TakeoffDA.reset_index()
                TakeoffDA = TakeoffDA[['TaskId','EngineId','TaskStartDatetime','TaskEndDatetime','StartDatetime',
                                          'TGT__MAR_WC_DEGC','qi','task_usage']]
            
            
                
        return TakeoffDA

obj = T700(df) 
df = obj.predict()
print(df)

#Writing pandas df to MySql server DB (not local DB)
database_username = 'mysqluser'
database_password = 'beovomysql123' #Do not use @ in password
database_ip       = '104.237.2.219'
port = '5340'
database_name     = 'company1'

# import sqlalchemy
# from sqlalchemy import create_engine
# pymysqL- importing this not required

# {0} = database_username
# {1} = database_password
# {2} = database_ip
# {3} = port
# {4} = database_name

#                          {0}     :    {1} @{2}:{3} /   {4}
#{0}:{1}@{2}:{3}/{4} means username:password@ip:port/database_name

database_connection = sqlalchemy.create_engine('mysql+pymysql://{0}:{1}@{2}:{3}/{4}'.
                                               format(
                                                   database_username, 
                                                   database_password, 
                                                   database_ip,port,
                                                   database_name ))

df.to_sql(con=database_connection, name='T700', if_exists='replace')