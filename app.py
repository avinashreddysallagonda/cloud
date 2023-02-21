from datetime import datetime        

from flask import Flask, request, render_template
from flask import Flask
import pyodbc
import pandas as pd
import math

app = Flask(__name__)
 
connection = pyodbc.connect('DRIVER={SQL Server};SERVER=seethamma.database.windows.net;DATABASE=chilipi;UID=mysql;PWD=classmate@1')
cursor = connection.cursor()


@app.route('/', methods=['GET','POST'])
def index():
    data = None
    if request.method == "POST":
        # input = request.form['input']
       
       
        if request.form['pop1']!='' and request.form['pop2']!='' and request.form['n1']!='':
            pop2 = int(request.form['pop2'])
            pop1 = int(request.form['pop1'])
            n1 = int(request.form['n1'])
            print(pop1,pop2,n1)
            sql1 = f"SELECT * FROM (SELECT *, ROW_NUMBER() OVER (ORDER BY Population DESC) as rn FROM datas WHERE Population BETWEEN {pop1} AND {pop2}) as t1 WHERE t1.rn <= {n1} UNION  SELECT * FROM(SELECT *, ROW_NUMBER() OVER (ORDER BY Population ASC) as rn FROM datas WHERE Population BETWEEN {pop1} AND {pop2}) as t2 WHERE t2.rn <= {n1} ORDER BY rn;"
            cursor.execute(sql1)
            data = cursor.fetchall()
            rows=[]
            for i in data:
                rows.append(i)
            print(rows)
            return render_template('largest.html', ci=rows)

        elif request.form['from_lat']!='' and request.form['to_lat']!='' and request.form['from_lon']!='' and request.form['to_lon']!='' :
            lat1 = float(request.form['from_lat'])
            lat2 = float(request.form['to_lat'])
            lon1 = float(request.form['from_lon'])
            lon2 = float(request.form['to_lon'])
            data = []
            sql11 = f"SELECT * FROM datas WHERE  lat between {lat1} AND {lat2} AND lon between {lon1} AND {lon2}"

            cursor.execute(sql11)
            data = cursor.fetchall()
            return render_template('latlon.html',ci=data)
        
        elif request.form['state']!='' and request.form['pop11']!='' and request.form['pop12']!='' and request.form['inc']!='':
            pop2 = int(request.form['pop12'])
            pop1 = int(request.form['pop11'])
            n1 = (request.form['state'])
            print(n1)
            inc = int(request.form['inc'])

            sql=f"UPDATE datas SET Population = Population + {inc} WHERE State = '{n1}' AND Population BETWEEN {pop1} AND {pop2}"
            cursor.execute(sql)


            sql1=f"SELECT City, State, Population FROM datas WHERE State = '{n1}' AND Population BETWEEN {pop1+inc} AND {pop2+inc}"
            cursor.execute(sql1)
            data = cursor.fetchall()
            lis=len(data)

            return render_template('magnitude.html',lis=lis,ci=data)

        elif request.form['state1']!='' and request.form['lat3']!='' and request.form['lon3']!='' and request.form['pop13']!='' and request.form['city']!='':
            pop = int(request.form['pop13'])
            state = (request.form['state1'])
            lat = float(request.form['lat3'])
            lon = float(request.form['lon3'])
            city = (request.form['city'])
           
            insert_query = f"INSERT INTO datas (City, State, Population, lat, lon) VALUES ('{city}', '{state}', {pop}, {lat}, {lon})"
            cursor.execute(insert_query)
            data = cursor.fetchall()
            return render_template('largest.html',ci=data)

        elif request.form['state2']!=''   and request.form['city1']!='':
            # pop = int(request.form['pop14'])
            state = (request.form['state3'])
            # lat = float(request.form['lat4'])
            # lon = float(request.form['lon4'])
            city = (request.form['city1'])
            insert_query = f"DELETE FROM datas WHERE City = '{city}' AND state_name = '{state}'"

            cursor.execute(insert_query)
            data = cursor.fetchall()
            return render_template('largest.html',ci=data)

        

        # convert the date strings to datetime objects
            
            mag = float(request.form['mag'])
            
            print(mag)
            query1 = f"SELECT time FROM earthquake where mag>= {mag}"
            day=0
            night=0
            cursor.execute(query1)
            res=cursor.fetchall()
            print(len(res))
            start=res[1]
            end=res[0]
            cur = start
            print(cur[0])
            cur=cur[0]    
            cur = datetime.strptime(cur, "%Y-%m-%dT%H:%M:%S.%fZ")
            query = f"SELECT COUNT(*) FROM earthquake WHERE datepart(hour, time) >= 6 AND datepart(hour, time) < 18 and mag>= {mag}"
            cursor.execute(query)
            day += cursor.fetchone()[0]
            query1 = f"SELECT COUNT(*) FROM earthquake WHERE datepart(hour, time) < 6 OR datepart(hour, time) >= 18 and mag>= {mag} "
            cursor.execute(query1)
            night += cursor.fetchone()[0]  
            print(night)          
            return render_template('magnitude.html',mag=mag,ci=day,cj=night)

       
    return render_template("index.html",data=data)

def parseCSV():
    print("parsing file")
    url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.csv"
    req = pd.read_csv(url)
    print(req)
    return req
    

    

def newTable():
    sql1 = "DROP TABLE IF EXISTS earthquake"
    cursor.execute(sql1)
    connection.commit()
    #time,latitude,longitude,depth,mag,type,gap,net,id,place
    sql2 = "create table  earthquake (time varchar(25), latitude varchar(25), longitude varchar(25), depth varchar(25), mag varchar(25),magType varchar(25),nst varchar(25),gap varchar(25),dmin varchar(25),rms varchar(50),net varchar(25),id varchar(25),updated varchar(25),place varchar(250),type varchar(25),horizontal varchar(25),depthError varchar(25),magError varchar(25),magNst varchar(25),status varchar(25),locationSource varchar(25),magSource varchar(25));" 
    cursor.execute(sql2)
    connection.commit()
    
def uploadData():
    req=parseCSV()
    newTable()
    col_names = ['time','latitude','longitude','depth','mag','magType','nst','gap','dmin','rms','net','id','updated','place','type','horizontal','depthError','magError','magNst','status','locationSource','magSource']
    # csvData = pd.read_csv('eq.csv',names=col_names,header=None)
    # csvData = csvData.where((pd.notnull(csvData)), 0)
    req=req.where((pd.notnull(req)),0)
    for i, row in req.iterrows():
        # print(i," ",row)
        if i > 0:
                try:
                    sql = "INSERT INTO earthquake (time,latitude,longitude,depth,mag,magType,nst,gap,dmin,rms,net,id,updated,place,type,horizontal,depthError,magError,magNst,status,locationSource,magSource) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
                    values = (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7],row[8], row[9],row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[17],row[18], row[19],row[20],row[21])
                    cursor.execute(sql, values)
                    connection.commit()
                except Exception as e:
                    print(e)
    cursor.close()  
# uploadData()
if __name__ == '__main__':
   app.run()



