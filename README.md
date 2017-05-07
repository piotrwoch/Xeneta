# Xeneta
Home Assignment

The WochApp's prerequisites:

1) pip install sortedcontainers
2) pip install requests
3) pip install flask_uploads
4) pip install flask_sqlalchemy
5) pip install flask_restless
6) pip install PyGreSQL

API endpoints:

(send a request with no parameters to see a description of available parameters)

"/avgprice"      - (GET) checking the avg price between orig & dest ports, or ports in the same slug/region

"/upload_price"  - (POST) upload an individual price

"/upload"        - (GET) batch upload form

Open questions:

1) Re: "GET" reqs, pt.4: It was not specified if "following the
   region hierarchy "up" " should be by the origin or dest or both (assumed by both)
   
2) Re: "POST" reqs, pt.1: date_from, date_to are not relevant for uploading a single price (assumed only a single date param is required) 
