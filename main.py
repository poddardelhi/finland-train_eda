import sqlite3
import csv
import pandas as pd

DB="/Users/nishant.poddar/Desktop/nishant-learning/test_excercise/finland-train_eda/sqlite_db/train.db"
csv_path="/Users/nishant.poddar/Desktop/nishant-learning/test_excercise/finland-train_eda/train-27-data-2020-01-01_to_2020-02-29.csv"
table={}

table['train_ic27']=(
    "CREATE TABLE traintable("
    "cancelled VARCHAR(200) NULL,"
    "commuterLineID VARCHAR(200) NULL,"
    "departureDate VARCHAR(200) NULL,"
    "operatorShortCode VARCHAR(200) NULL,"
    "operatorUICCode VARCHAR(200) NULL,"
    "runningCurrently VARCHAR(200) NULL,"
    "timetableAcceptanceDate VARCHAR(200) NULL," 
    "timetableType VARCHAR(200) NULL," 
    "trainCategory VARCHAR(200) NULL," 
    "trainNumber VARCHAR(200) NULL,"
    "trainType VARCHAR(200) NULL,"
    "version VARCHAR(200) NULL,"
    "timeTableRows_actualTime VARCHAR(200) NULL,"
    "timeTableRows_cancelled VARCHAR(200) NULL,"
    "timeTableRows_causes VARCHAR(200) NULL,"
    "timeTableRows_commercialStop VARCHAR(200) NULL,"
    "timeTableRows_commercialTrack VARCHAR(200) NULL,"
    "timeTableRows_countryCode VARCHAR(200) NULL,"
    "timeTableRows_differenceInMinutes VARCHAR(200) NULL,"
    "timeTableRows_estimateSource VARCHAR(200) NULL,"
    "timeTableRows_liveEstimateTime VARCHAR(200) NULL,"
    "timeTableRows_scheduledTime VARCHAR(200) NULL,"
    "timeTableRows_stationShortCode VARCHAR(200) NULL,"
    "timeTableRows_stationUICCode VARCHAR(200) NULL,"
    "timeTableRows_trainReady VARCHAR(200) NULL,"
    "timeTableRows_trainReady_accepted VARCHAR(200) NULL,"
    "timeTableRows_trainReady_source VARCHAR(200) NULL,"
    "timeTableRows_trainReady_timestamp VARCHAR(200) NULL,"
    "timeTableRows_trainStopping VARCHAR(200) NULL,"
    "timeTableRows_type VARCHAR(200) NULL)"

)


'''
Function to create the table inside the sqlite
'''
def create_table():
    try:
        with sqlite3.connect(DB) as conn:
            cursor=conn.cursor()
            print("Creating Table inside the train_ic27 \n")
            cursor.execute('DROP TABLE IF EXISTS traintable')
            cursor.execute(table['train_ic27'])
            
    except Exception as e:
        print('Error :', e)
    else:
        print("Table created \n")



'''
Funtion to insert the data into the table
'''
def insert_data():
    con=sqlite3.connect(DB)
    cur=con.cursor()

    try:
        with open(csv_path,'r') as f:
            csv_reader= csv.reader(f,delimiter=',')
            cur=con.cursor()
            no_records=0             
            try:
                for row in csv_reader:
                    cur.execute(
                        '''
                        INSERT INTO traintable(cancelled,commuterLineID,departureDate,operatorShortCode,operatorUICCode,runningCurrently,timetableAcceptanceDate,timetableType,trainCategory,trainNumber,trainType,version,timeTableRows_actualTime,timeTableRows_cancelled,timeTableRows_causes,timeTableRows_commercialStop,timeTableRows_commercialTrack,timeTableRows_countryCode,timeTableRows_differenceInMinutes,timeTableRows_estimateSource,timeTableRows_liveEstimateTime,timeTableRows_scheduledTime,timeTableRows_stationShortCode,timeTableRows_stationUICCode,timeTableRows_trainReady,timeTableRows_trainReady_accepted,timeTableRows_trainReady_source,timeTableRows_trainReady_timestamp,timeTableRows_trainStopping,timeTableRows_type) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',(row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13],row[14],row[15],row[16],row[17],row[18],row[19],row[20],row[21],row[22],row[23],row[24],row[25],row[26],row[27],row[28],row[29])
                        )
                    con.commit()
                    no_records+=1
            except Exception as e:
                print("error", e) 
        
        con.close()   
    except Exception as e :
        print("Error",e)
    else:
        print("\n***********data inserted and {} rows inserted************\n".format(no_records))

'''
Function to show the records inside the database created above
'''

def select_from_table():
    try:
        with sqlite3.connect(DB) as conn:
            cur = conn.cursor()
            print("\nPrinting first 3 records from the database \n")
            for row in cur.execute("SELECT * FROM traintable LIMIT 3"):
                print(row)
           
                
    except Exception as e :
        print("Error:", e)
'''
Funtion to convert the database to csv for Exploratory Analysis
'''
def sql_to_df():
    data_base=sqlite3.connect(DB)
    query=data_base.execute("SELECT * FROM traintable")
    col=[column[0] for column in query.description]
    df=pd.DataFrame.from_records(data=query.fetchall(),columns=col)
    #Converting the dataframe to csv
    try:
        print("\n *********Creating the CSV file ******* \n")
        df.to_csv("train_finland.csv",index=False)
    except Exception as e:
        print("Error:",e)
    else:
        print("\n******CSV is created********")

if __name__=="__main__":
    create_table()
    insert_data()
    select_from_table()
    sql_to_df()
 

