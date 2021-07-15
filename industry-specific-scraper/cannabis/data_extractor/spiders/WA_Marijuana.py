import json,csv
import os,sys,re
import scrapy
import logging
import requests
from datetime import datetime, timedelta
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher


#
class WaMarijuanaSpider(scrapy.Spider):    
    name = "pot_wa"
    #
    def __init__(self, *args, **kwargs):
        logger = logging.getLogger('scrapy.spidermiddlewares.httperror')
        logger.setLevel(logging.ERROR)  # set logging level here
        super().__init__(*args, **kwargs)
        #
        dispatcher.connect(self.spider_closed, signals.spider_closed)               
        #
        csv_encoding = 'utf-8'
        self.csvFilename  = 'WA_Marijuana_Businesses.csv'
        self.csv_file = open(self.csvFilename, 'w', newline='', encoding=csv_encoding) #, encoding='windows-1250', errors ='ignore')
        self.csvwriter = csv.writer(self.csv_file)
        if csv_encoding == 'utf-8': self.csv_file.write(u'\ufeff') #.write(codecs.BOM_UTF8)
        #
        column_headers = ['wa_ubi','business_name','license_number','license_type','license_status','create_date',
                          'address1','address2','city','state','zipcode','country','phone','sourceUrl','dateCollected']
        self.csvwriter.writerow(column_headers)
        self.totalCount = 0        

        # WA: Liquor and Cannabis Board Data Portal
        self.datasets_url = "https://data.lcb.wa.gov/browse?limitTo=datasets"
        # page for Licensed Businesses
        self.Licensed_Businesses_url = "https://data.lcb.wa.gov/Licensing/Licensed-Businesses/u3zh-ri66"
        # JSON API for Licensed Businesses
        self.Licensed_Businesses_api = "https://data.lcb.wa.gov/resource/bhbp-x4eb.json"
        # csv
        self.Licensed_Businesses_csv = "https://data.lcb.wa.gov/api/views/u3zh-ri66/rows.csv?accessType=DOWNLOAD"

    #
    def start_requests(self): 
        yield scrapy.Request(self.datasets_url, self.parse)
    #
    def parse(self, response):
        yield scrapy.Request(self.Licensed_Businesses_csv, self.parse_Licensing_csv)
    #
    def parse_Licensing_csv(self, response):
        date_collected = datetime.now().strftime('%Y-%m-%d %H:%M:%S')      
        #
        csv_data = response.text
        csvReader = csv.reader(csv_data.splitlines())
        # ['Organization', 'Active', 'License', 'UBI', 'Type', 'Address', 'Address Line 2', 'City', 'State', 'County', 'Zip', 'CreateDate', 'DayPhone']
        for rec in csvReader:
            if rec[0] == 'Organization': continue
            organization = rec[0]
            license_status = rec[1]
            license_number = rec[2]
            wa_ubi = rec[3]  # Washington State Unified Business Identifier (UBI)
            license_type = rec[4]
            address1 = rec[5]
            address2 = rec[6]
            city = rec[7]
            state = rec[8]
            county = rec[9]
            #
            zipcode = rec[10] # 989369706
            if zipcode and len(zipcode)==9: zipcode = "{}-{}".format(zipcode[:5], zipcode[5:])
            #
            create_date = rec[11]
            if create_date and len(create_date)==8: create_date = "{}-{}-{}".format(create_date[:4], create_date[4:6], create_date[6:])     
            #
            phone = rec[12]       
            if phone and len(phone)==10: phone = "{}-{}-{}".format(phone[:3], phone[3:6], phone[6:])                         
            #
            row = [wa_ubi, organization, license_number,license_type,license_status,create_date, address1,address2,city,state,zipcode,'USA',
                   phone,self.Licensed_Businesses_csv, date_collected]
            self.csvwriter.writerow(row)
            self.totalCount += 1
    #
    #
    def parse_V1(self, response):
        yield scrapy.Request(self.Licensed_Businesses_url, self.parse_Licensing_json)
    #
    def parse_Licensing_json(self, response):
        headers = {'Accept':'application/json, text/javascript, */*; q=0.01',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Accept-Language': 'en-US,en;q=0.8,zh-CN;q=0.6,zh;q=0.4',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}
        yield scrapy.Request(self.Licensed_Businesses_api, callback=self.parse_api, headers=headers)
    #
    def parse_api(self, response):
        #
        date_collected = datetime.now().strftime('%Y-%m-%d %H:%M:%S')      
        json_data = json.loads(response.text)
        #
        for rec in json_data:
            license_number = rec['license']
            license_status = rec['active']
            license_type = rec['type']
            address1 = rec['address']
            address2 = rec['address_line_2'] if 'address_line_2' in rec else ''
            city = rec['city']
            county = rec['county']
            create_date = rec['createdate']
            phone = rec['dayphone'] if 'dayphone' in rec else ''
            organization = rec['organization']
            state = rec['state']
            wa_ubi = rec['ubi']  # Washington State Unified Business Identifier (UBI)
            zipcode = rec['zip'] # 989369706
            if zipcode and len(zipcode)==9: zipcode = "{}-{}".format(zipcode[:5], zipcode[5:])
            #
            row = [wa_ubi, organization, license_number,license_type,license_status,create_date, address1,address2,city,state,zipcode,'USA',
                   self.Licensed_Businesses_api, date_collected]
            self.csvwriter.writerow(row)
            self.totalCount += 1
    #
    #
    def middleware_exception(self, request, middleware_class, exception_url):
        print("{}: {}: {}".format(request.url, middleware_class, exception_url)) 
    #
    #
    def spider_closed(self, spider, reason): 
        #
        print("totalCount={}".format(self.totalCount))
        self.log("Closing file {}...".format(self.csvFilename))
        self.csv_file.close()