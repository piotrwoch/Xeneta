#!/usr/bin/python
# -*- coding: UTF8 -*-

#Import all the necessary components
import os
from flask import Flask, request, session, g, redirect, url_for, abort, \
     render_template, flash
from flask_restless import APIManager
from flask_sqlalchemy import SQLAlchemy
from flask_uploads import UploadSet, TEXT, configure_uploads
from sqlalchemy import create_engine
from sqlalchemy.orm.session import sessionmaker
from sortedcontainers import SortedSet
import requests
import json
import csv
import pgdb

#Set up the app
from config import *

#Initialize vars storing hard-coded DB connection info and PostgreSQL session 
hostname = '192.168.99.100'
username = 'postgres'
password = ''
database = 'postgres'

engine = create_engine('postgresql://' + username + '@' + hostname + ':5432/' + database)
Session = sessionmaker(bind=engine)
mysession = Session()

#Initialize the API Manager
apimanager = APIManager(app, session=mysession)

#Query for an average price between two ports; if not enough data is present, return the avg price for all ports in the same slug/region as orig & dest ports
#
# Arguments:
#
# orig          - (string) code of the port of origin
# dest          - (string) code of the port of destination
# from_date     - (date)   the first day of the date range for the query
# to_date       - (date)   the last day of the date range for the query
# minDataPoints - (int)    the minimal number of price data points required for calculating average price

def doAvgPriceQuery( conn, orig, dest, from_date, to_date, minDataPoints ) :
    result = SortedSet({('','')})

    cur = conn.cursor()

    #prepare Having clause if the limit on the min number of data points was supplied
    if minDataPoints <> '':
        HavingClause = ' HAVING count(price) >= ' + str(minDataPoints)
    else:
        HavingClause = ''

    #query for the average price between the orig and dest ports
    cur.execute( "SELECT day, avg(price) FROM prices WHERE orig_code='" + orig + "' and dest_code='" + dest + "' and day >= '" + str(from_date) + "' and day <= '" + str(to_date) + "' GROUP BY day " + HavingClause + " ORDER BY day" )

    if cur.rowcount > 0:
        #prepare the result
        for day, price in cur.fetchall() :
            result.add((str(day), price))
    else:
        #the query at the port level did not return any data, query data for all the ports in the same slugs, to which the orig/dest ports belong to, respectively
        result = SortedSet({('','','','')})
        cur.execute( "SELECT day, avg(price),orig_code, dest_code FROM prices " +
                     "JOIN ports orig_ports ON prices.orig_code = orig_ports.code " +
                     "JOIN ports dest_ports ON prices.dest_code = dest_ports.code " + 
                     "WHERE orig_ports.parent_slug=(select parent_slug from ports where code = '" + orig + "')" +
                     "and dest_ports.parent_slug=(select parent_slug from ports where code = '" + dest + "')" +
                     "and day >= '" + str(from_date) + "' and day <= '" + str(to_date) + "'" +
                     "GROUP BY orig_code, dest_code,day " +
                     HavingClause + " " +
                     "ORDER BY day" )

        if cur.rowcount > 0:
            #prepare the result       
            for day, price, orig_code, dest_code in cur.fetchall() :
                result.add((str(day), price, orig_code, dest_code))

        else:
            #the query at the slug level did not return any data, query data for all the ports in the same regions, to which the orig/dest ports belong to, respectively
            cur.execute( "SELECT day, avg(price),orig_code, dest_code FROM prices " +
                         "JOIN ports orig_ports ON prices.orig_code = orig_ports.code " +
                         "JOIN regions orig_regions ON orig_ports.parent_slug = orig_regions.slug " +
                         "JOIN ports dest_ports ON prices.dest_code = dest_ports.code " + 
                         "JOIN regions dest_regions ON dest_ports.parent_slug = dest_regions.slug " +
                         "WHERE orig_regions.parent_slug=(select parent_slug from regions where slug = (select parent_slug from ports where code = '" + orig + "'))" +
                         "and dest_ports.parent_slug=(select parent_slug from regions where slug = (select parent_lug from ports where code = '" + dest + "'))" +
                         "and day >= '" + str(from_date) + "' and day <= '" + str(to_date) + "'" +
                         "GROUP BY orig_code, dest_code, day " +
                         HavingClause + " " +
                         "ORDER BY day" )

            for day, price, orig_code, dest_code in cur.fetchall() :
                result.add((str(day), price, orig_code, dest_code))
                        
    return result

#Query for an average price between ports in two slugs; if not enough data is present, return the avg price for all ports in the same region as orig & dest slugs
#
# Arguments:
#
# orig_slug     - (string) code of the slug of origin
# dest_slug     - (string) code of the slug of destination
# from_date     - (date)   the first day of the date range for the query
# to_date       - (date)   the last day of the date range for the query
# minDataPoints - (int)    the minimal number of price data points required for calculating average price

def doAvgSlugPriceQuery( conn, orig_slug, dest_slug, from_date, to_date, minDataPoints ) :
    result = SortedSet({('','','','')})

    cur = conn.cursor()

    #prepare Having clause if the limit on the min number of data points was supplied
    if minDataPoints <> '':
        HavingClause = ' HAVING count(price) >= ' + str(minDataPoints)
    else:
        HavingClause = ''

    #query for the average price between the ports in the orig and dest slugs
    cur.execute( "SELECT day, avg(price), orig_code, dest_code as avg_price FROM prices " + 
                 "JOIN ports orig_ports ON prices.orig_code = orig_ports.code " +
                 "JOIN ports dest_ports ON prices.dest_code = dest_ports.code " + 
                 "WHERE orig_ports.parent_slug='" + orig_slug + "' and dest_ports.parent_slug='" + dest_slug + "'" + 
                 "and day >= '" + str(from_date) + "' and day <= '" + str(to_date) + "'" +
                 "GROUP BY orig_code, dest_code,day " +
                 HavingClause + " " + 
                 "ORDER BY day")

    if cur.rowcount > 0:

        #prepare the result 
        for day, price, orig_code, dest_code in cur.fetchall() :
            result.add((orig_code, dest_code, str(day), price))

    else:
        #the query at the slug level did not return any data, query data for all the ports in the same regions, to which the orig/dest ports belong to, respectively
        cur.execute( "SELECT day, avg(price),orig_code, dest_code FROM prices " +
                     "JOIN ports orig_ports ON prices.orig_code = orig_ports.code " +
                     "JOIN regions orig_regions ON orig_ports.parent_slug = orig_regions.slug " +
                     "JOIN ports dest_ports ON prices.dest_code = dest_ports.code " + 
                     "JOIN regions dest_regions ON dest_ports.parent_slug = dest_regions.slug " +
                     "WHERE orig_regions.parent_slug=(select parent_slug from regions where slug = '" + orig_slug + "')" +
                     "and dest_ports.parent_slug=(select parent_slug from regions where slug = '" + dest_slug + "')" +
                     "and day >= '" + str(from_date) + "' and day <= '" + str(to_date) + "'" +
                     "GROUP BY orig_code, dest_code, day " +
                     HavingClause + " " +
                     "ORDER BY day" )

        for day, price, orig_code, dest_code in cur.fetchall() :
            result.add((orig_code, dest_code, str(day), price))

    return result

#Insert a price between orig/dest ports on a given date
#
# Arguments:
#
# orig          - (string) code of the port of origin
# dest          - (string) code of the port of destination
# date          - (date)   datestamp of the price
# price         - (int)    price in USD
def doInsertPrice( conn, orig, dest, date, price ) :
    result = 0

    cur = conn.cursor()

    cur.execute( "INSERT INTO prices (orig_code, dest_code, day, price) " +
                 "VALUES ('" + orig + "', '" + dest + "', '" + date + "', '" + price + "')")

#API endpoint for checking the avg price between two ports/slugs
@app.route('/avgprice',methods=['GET'])
def get_avgprice():
    result = {('','')}
    resultStr = ''
    minDataPoints = 0

    if 'min_data_pts' in request.args:
        minDataPoints = request.args["min_data_pts"]

    if (('orig' in request.args and 'dest' in request.args) or \
        ('orig_slug' in request.args and 'dest_slug' in request.args)) \
        and 'from' in request.args and 'to' in request.args:
        myConnection = pgdb.connect( host=hostname, user=username, password=password, database=database )
        if 'orig' in request.args:
            result = doAvgPriceQuery( myConnection, request.args["orig"], request.args["dest"], request.args["from"], request.args["to"], minDataPoints )
            resultStr = "Average prices on the route from " + request.args["orig"] + " to " + request.args["dest"] + ":<BR>"
            if minDataPoints > 0:
                resultStr = resultStr + "(results filtered for minimal sample size of " + str(minDataPoints) + ")<BR>"
            for i in result:
                if len(i) == 2:
                    resultStr = resultStr + str(i[0]) + ', ' + str(i[1]) + '<BR>'
                else:
                    resultStr = resultStr + str(i[0]) + ', ' + str(i[1]) + ', ' + str(i[2]) + ', ' + str(i[3]) + '<BR>'
        else:
            result = doAvgSlugPriceQuery( myConnection, request.args["orig_slug"], request.args["dest_slug"], request.args["from"], request.args["to"], minDataPoints )
            resultStr = "Average prices on the routes between slugs " + request.args["orig_slug"] + " and " + request.args["dest_slug"] + ":<BR>"
            if minDataPoints > 0:
                resultStr = resultStr + "(results filtered for minimal sample size of " + str(minDataPoints) + ")<BR>"   
            for i in result: resultStr = resultStr + str(i[0]) + ', ' + str(i[1]) + ', ' + str(i[2]) + ', ' + str(i[3]) + '<BR>'
        myConnection.close()
        return resultStr
    else:
        return 'Please supply the following arguments:<BR><BR>''orig'' - Port of Origin<BR>''dest'' - Port of Destination<BR><BR>or<BR><BR>''orig_slug'' - Slug of Origin<BR>''dest_slug'' - Slug of Destination<BR><BR>and<BR><BR>''from'' - start of date range<BR>''to'' - end of date range'

#API endpoint for uploading an individual price between two ports/slugs
@app.route("/upload_price", methods=['POST'])
def upload_price():
    result = 0
    resultStr = ''

    if 'orig' in request.args and 'dest' in request.args and 'date' in request.args and 'price' in request.args:

        price = float(request.args["price"])

        if 'curr' in request.args:
           
            url = 'https://openexchangerates.org/api/latest.json?app_id=817c7146e15241cd98820bc6ebe082d9&base=USD'
            headers = {'Content-Type': 'application/json'}

            filters = []
            params = dict(q=json.dumps(dict(filters=filters)))

            response = requests.get(url, params=params, headers=headers)
            assert response.status_code == 200
            price = price / response.json()["rates"][request.args["curr"]]

        myConnection = pgdb.connect( host=hostname, user=username, password=password, database=database )
        result = doInsertPrice( myConnection, request.args["orig"], request.args["dest"], request.args["date"], str(int(price)) )
        myConnection.commit()
        myConnection.close()
    else:
        return 'Please supply the following arguments:<BR><BR>"orig" - Port of Origin<BR>"dest" - Port of Destination<BR><BR>' + _
        '"date" - date stamp of the price<BR>"price" - freight price (FOB Port of Destination)<BR><BR>Optionally,<BR><BR>"curr" - currency code (price will be converted to USD)'

    resultStr = "Single price upload was successful."
    return resultStr

#API endpoint for batch price uploading form
@app.route("/upload")
def upload_form():
    if 'curr' in request.args:
        return '<form id="upload" method=POST enctype=multipart/form-data action="' + url_for('upload_batch') +'?curr=' + request.args["curr"] + '"><input type=file name=prices><button type="submit" form="upload" value="Submit">Submit</button></form>'
    else:
        return '<form id="upload" method=POST enctype=multipart/form-data action="' + url_for('upload_batch') +'"><input type=file name=prices><button type="submit" form="upload" value="Submit">Submit</button></form>'

#API endpoint for uploading file with a batch of prices
@app.route("/upload_batch", methods=['POST'])
def upload_batch():
    prices = UploadSet('prices', TEXT)
    configure_uploads(app, (prices,))
    xrate = 0;
    
    if 'prices' in request.files:

        if 'curr' in request.args:
           
            url = 'https://openexchangerates.org/api/latest.json?app_id=817c7146e15241cd98820bc6ebe082d9&base=USD'
            headers = {'Content-Type': 'application/json'}

            filters = []
            params = dict(q=json.dumps(dict(filters=filters)))

            response = requests.get(url, params=params, headers=headers)
            assert response.status_code == 200
            xrate = response.json()["rates"][request.args["curr"]]
            print "x-rate:" + str(xrate)

        filename = prices.save(request.files['prices'])
        print filename

        myConnection = pgdb.connect( host=hostname, user=username, password=password, database=database )
        
        with open(app.config['UPLOADED_PRICES_DEST'] + '\\' + filename, 'rb') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=',')
            for row in reader:
                #print row["orig_code"] + ", " + row["dest_code"] + ", " + row["day"] + ", " + row["price"] 
                result = doInsertPrice( myConnection, row["orig_code"], row["dest_code"], row["day"], row["price"] if xrate ==0 else str(int(int(row["price"])/xrate)) )
            print 'Uploaded ' + str(reader.line_num-1) + ' records.'

        myConnection.commit()
        myConnection.close()
    else:
        return 'Please supply the following arguments:<BR><BR>"file" - file with a batch of prices<BR><BR>Optionally,<BR><BR>"curr" - currency code (price will be converted to USD)'

    resultStr = "Batch price upload was successful."
    return resultStr

#main routine
if __name__ == "__main__":
    app.run()







