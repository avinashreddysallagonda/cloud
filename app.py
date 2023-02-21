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
        input = request.form['input']
        date_from = request.form['from']
        date_to = request.form['to']
        scale1 = request.form['scale1']
        scale2 = request.form['scale2']
        if input:
            sql1 = f"SELECT TOP {input} * FROM earthquake ORDER BY mag DESC"
            cursor.execute(sql1)
            data = cursor.fetchall()
            rows=[]
            for i in data:
                rows.append(i)
            print(rows)
            return render_template('largest.html', ci=rows)

        elif date_from and date_to and scale1 and scale2:
            sql2 = f"SELECT * from earthquake where time between '{date_from}' and '{date_to}' and mag > {scale1} and mag<{scale2}"
            cursor.execute(sql2)
            data = cursor.fetchall()
            return render_template('magrange.html',ci=data)
       
        elif request.form.get('from_mag') and request.form.get('to_mag') and request.form.get('distance'):
            lat = float(request.form['from_mag'])
            lon = float(request.form['to_mag'])
            distance = float(request.form['distance'])
            data = []
            r = 6371  # radius of the Earth in kilometers
            lat_rad = math.radians(lat)
            lon_rad = math.radians(lon)

            a = 6378.137  # equatorial radius of Earth in km
            e = 0.0818191908426  # eccentricity of Earth's ellipsoid
            R = a * math.sqrt(1 - e**2 * math.sin(lat_rad)**2) / (1 - e**2 * math.sin(lon_rad)**2)

    # Calculate angular distance covered by the distance
            delta = distance / R

    # Calculate range of latitude and longitude values
            lat_min = lat - math.degrees(delta)
            lat_max = lat + math.degrees(delta)
            lon_min = lon - math.degrees(delta / math.cos(lat_rad))
            lon_max = lon + math.degrees(delta / math.cos(lat_rad))
            print(distance,lat_min,lon_min)
            sql11 = f"SELECT * FROM earthquake WHERE  latitude <= {lat_max} AND latitude>={lat_min} AND longitude>={lon_min} AND longitude<={lon_max}"

            cursor.execute(sql11)
            data = cursor.fetchall()
            return render_template('latlon.html',ci=data)

        elif request.form.get('mag')  :
            
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
   app.run(debug = True)



