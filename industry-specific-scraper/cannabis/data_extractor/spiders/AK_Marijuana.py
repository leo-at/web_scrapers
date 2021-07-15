import json,csv
import os,sys
import scrapy
import logging
from datetime import datetime, timedelta
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher


#
class AKMarijuanaSpider(scrapy.Spider):    
    name = "pot_ak"
    #
    def __init__(self, *args, **kwargs):
        logger = logging.getLogger('scrapy.spidermiddlewares.httperror')
        logger.setLevel(logging.ERROR)  # set logging level here
        super().__init__(*args, **kwargs)
        #
        dispatcher.connect(self.spider_closed, signals.spider_closed)               
        #
        self.today_date = "{date:%m/%d/%Y}".format(date=datetime.now())
        self.twoyearago = "{date:%m/%d/%Y}".format(date=datetime.now()-timedelta(days=2*365))
        #
        self.debug_mode = False
        self.totalCount = 0
        self.column_headers = ['business_name', 'doing_Business_As', 'License_Number','License_Status','License_Type','License_Issue_Date','License_Effective_Date','License_Expiration_Date',
                               'address1','address2','city','state','zipcode','country','phone','email','entity_Official_1','entity_Official_2','entity_Official_3',
                               'business_License_Number','sourceUrl','collectedDate']
        #        
        csv_encoding = 'utf-8' 
        self.csvFilename  = "AK_Marijuana_Businesses.csv"
        self.csv_file = open(self.csvFilename, 'w', newline='', encoding=csv_encoding) #, encoding='windows-1250', errors ='ignore')
        self.csvwriter = csv.writer(self.csv_file)
        if csv_encoding == 'utf-8': self.csv_file.write(u'\ufeff') #.write(codecs.BOM_UTF8)
        self.csvwriter.writerow(self.column_headers)       
        #                                       
    #
    #
    def start_requests(self): 
        debug_mode = getattr(self, 'debug', None)
        if debug_mode: 
            self.debug_mode = True
            print("debug_mode: {}".format(debug_mode))
        #
        yield scrapy.Request("https://www.commerce.alaska.gov/web/amco/", self.parse)
    #
    def parse(self, response):
        yield scrapy.Request("https://www.commerce.alaska.gov/abc/marijuana/Home/licensesearch", self.parse_search)
    #
    def parse_search(self, response):
        #
        post_action = response.xpath("//form[@method='post']/@action").extract_first()
        post_url = response.urljoin(post_action)

        for lic_value in response.xpath("//select[@id='SearchLicenseTypeID']/option[@value!='']/@value").extract():
            if self.debug_mode: print("lic_value={}".format(lic_value))
            form_data = {
                'SearchType':'License',
                'SearchLicenseLicNum':'',
                'SearchLicenseDBA':'',
                'SearchLicenseBusLicNum':'',
                'SearchLicenseTypeID':lic_value,
                'SearchLicenseAddr':''}
            #
            #if lic_value not in ['ac7aabdd-057a-4393-9cb7-e45600de8c76']: continue
            #
            yield scrapy.FormRequest( post_url, formdata=form_data, callback=self.parse_result, headers={'x-requested-with':'XMLHttpRequest'})
    #
    #
    def parse_result(self, response):
        # search only retrun partial HTML
        for tr in response.xpath("//table/tbody/tr[td]"):
            marijuana_url = tr.xpath("./td/a[contains(@href,'marijuana')]/@href").extract_first()
            #
            if marijuana_url:
                yield scrapy.Request(response.urljoin(marijuana_url), self.parse_detail)
    #
    #
    def parse_detail(self, response):
        sourceUrl = response.url
        date_collected = datetime.now().strftime('%Y-%m-%d %H:%M:%S')      
        #
        marijuana_License_Number = marijuana_License_Status = marijuana_License_Type = doing_Business_As = ''
        marijuana_License_Issue_Date = marijuana_License_Effective_Date = marijuana_License_Expiration_Date = ''
        address1=address2=city=state=zipcode=country = phone = email = business_License_Number = licensees = entity_number = ''
        entity_Officials = []
        affiliates = []
        #
        label_list = response.xpath("//dl[@class='deptLblValPair']/dt/text()").extract()
        if self.debug_mode: print("label_list={}".format(label_list))
        #
        for idx, dd in enumerate(response.xpath("//dl[@class='deptLblValPair']/dd")):
            label = label_list[idx]
            if label == 'License Number:':
                marijuana_License_Number = dd.xpath("./text()").extract_first()
            elif label == 'License Status:':
                marijuana_License_Status = dd.xpath("./text()").extract_first()
            elif label == 'License Type:':
                marijuana_License_Type = dd.xpath("./text()").extract_first()
            elif label == 'Doing Business As:':
                doing_Business_As = dd.xpath("./text()").extract_first()
            elif label == 'Issue Date:':
                marijuana_License_Issue_Date = dd.xpath("./text()").extract_first()
            elif label == 'Effective Date:':
                marijuana_License_Effective_Date = dd.xpath("./text()").extract_first()
            elif label == 'Expiration Date:':
                marijuana_License_Expiration_Date = dd.xpath("./text()").extract_first()
            elif label == 'Email Address:':
                email = dd.xpath("./a/text()").extract_first()
            elif label == 'Physical Address:':
                addr_lines = [ x.strip(' \t\r\n') for x in dd.xpath("./text()").extract() if x.strip(' \t\r\n') ]
                address1,address2,city,state,zipcode,country = self._parse_addr_lines(addr_lines)
            elif label == 'Licensees:':
                licensees = dd.xpath("./text()").extract_first()
                if licensees: licensees = licensees.strip(" \t\r\n")
                entity_number = dd.xpath("./a/text()").extract_first()
            elif label == 'Entity Officials:':
                entity_Officials = [ x.strip() for x in dd.xpath("./text()").extract() if x.strip() ]
            elif label == 'Business License Number:':
                business_License_Number = dd.xpath("./a/text()").extract_first()
                business_detail_url = dd.xpath("./a/@href").extract_first()
            elif label == 'Affiliates:':
                affiliates = [ x.strip() for x in dd.xpath("./text()").extract() if x.strip() ]
            else:
                raise ValueError("{}: Unexpected label: {}".format(response.url, label))
        #
        entity_Official_1 = entity_Officials[0] if len(entity_Officials) >= 1 else ''
        entity_Official_2 = entity_Officials[1] if len(entity_Officials) >= 2 else ''
        entity_Official_3 = entity_Officials[2] if len(entity_Officials) >= 3 else ''
        #
        row = [licensees, doing_Business_As, marijuana_License_Number,marijuana_License_Status,marijuana_License_Type,marijuana_License_Issue_Date,marijuana_License_Effective_Date,marijuana_License_Expiration_Date,                
               address1, address2,city,state,zipcode,country,phone,email, entity_Official_1,entity_Official_2,entity_Official_3,
               business_License_Number, sourceUrl, date_collected]
        self.csvwriter.writerow(row)
        self.totalCount += 1
        # #
        # yield scrapy.Request(response.urljoin(business_detail_url), callback=self.parse_business_detail, meta={
        #         'marijuana_License_Number':marijuana_License_Number, 'marijuana_License_Status':marijuana_License_Status, 'marijuana_License_Type':marijuana_License_Type, 
        #         'marijuana_License_Issue_Date':marijuana_License_Issue_Date, 'marijuana_License_Effective_Date':marijuana_License_Effective_Date, 'marijuana_License_Expiration_Date':marijuana_License_Expiration_Date,
        #         'doing_Business_As':doing_Business_As, 'business_License_Number':business_License_Number, 'licensees':licensees, 'entity_number':entity_number,
        #         'address1':address1, 'address2':address2, 'city':city, 'state':state, 'zipcode':zipcode, 'country':country, 'email':email, 'entity_Officials':entity_Officials, 'sourceUrl':response.url})
        # #    
    #
    # https://www.commerce.alaska.gov/robots.txt does not allow scraping for business detail page!!!
    def parse_business_detail(self, response):
        md = response.meta
        marijuana_License_Number = md['marijuana_License_Number']
        marijuana_License_Status = md['marijuana_License_Status']
        marijuana_License_Type = md['marijuana_License_Type']
        marijuana_License_Issue_Date = md['marijuana_License_Issue_Date']
        marijuana_License_Effective_Date = md['marijuana_License_Effective_Date']
        marijuana_License_Expiration_Date = md['marijuana_License_Expiration_Date']
        doing_Business_As = md['doing_Business_As']
        business_License_Number = md['business_License_Number']
        licensees = md['licensees']
        business_entity_number = md['entity_number']
        address1 = md['address1']
        address2 = md['address2']
        city = md['city']
        state = md['state'] 
        zipcode = md['zipcode']
        country = md['country']
        phone = ''
        email = md['email']
        entity_Officials = md['entity_Officials']
        sourceUrl = md['sourceUrl']
        #
        business_name = business_status = business_type = business_Issue_Date = business_Expiration_Date = ''
        primary_sic = primary_naics = secondary_sic = secondary_naics = ''
        mailing_address1=mailing_address2=mailing_city=mailing_state=mailing_zipcode = ''
        #
        label_list = response.xpath("//dl[@class='lblValPair']/dt/label/text()").extract()
        for idx, dd in enumerate(response.xpath("//dl[@class='lblValPair']/dd")):
            label = label_list[idx]
            if label == 'Business Name':
                business_name = dd.xpath("./text()").extract_first()  # seems to be the same as licensees on marijuana license page
            elif label == 'Status':
                business_status = dd.xpath("./text()").extract_first()
            elif label == 'Business Type':
                business_type = dd.xpath("./text()").extract_first()
            elif label == 'Issue Date':
                business_Issue_Date = dd.xpath("./text()").extract_first()
            elif label == 'Expiration Date':
                business_Expiration_Date = dd.xpath("./text()").extract_first()
            elif label == 'Primary Line Of Business':
                primary_sic = dd.xpath("./text()").extract_first()
            elif label == 'Primary NAICS':
                primary_naics = dd.xpath("./text()").extract_first()
            elif label == 'Secondary Line Of Business':
                secondary_sic = dd.xpath("./text()").extract_first()
            elif label == 'Secondary NAICS':
                secondary_naics = dd.xpath("./text()").extract_first()
            elif label == 'Mailing Address':
                mailing_address = dd.xpath("./text()").extract_first()
                mailing_address1,mailing_address2,mailing_city,mailing_state,mailing_zipcode = self._parse_mailing_address(mailing_address)
            elif label == 'Physical Address':
                physical_address = dd.xpath("./text()").extract_first() # The Physical Address on marijuana license page is better with address2 with available.
            else:
                raise ValueError("{}: Unexpected label: {}".format(response.url, label))
        #    
        row = [licensees, business_name, doing_Business_As, 
               marijuana_License_Number,marijuana_License_Status,marijuana_License_Type,marijuana_License_Issue_Date,marijuana_License_Effective_Date,marijuana_License_Expiration_Date,                
               business_entity_number,business_License_Number,business_status,business_type,business_Issue_Date,business_Expiration_Date,
               address1, address2,city,state,zipcode,country,phone,email,
               mailing_address1,mailing_address2,mailing_city,mailing_state,mailing_zipcode, sourceUrl, date_collected]
        self.csvwriter.writerow(row)
        self.totalCount += 1
    # 
    def _parse_mailing_address(self, mailing_address):
        idx = mailing_address.find(',')
        mailing_address1 = mailing_address[:idx]
        mailing_address2 = ''
        city_state_zip = mailing_address[idx+1:].strip()
        mailing_city,mailing_state,mailing_zipcode = self._parse_city_state_zip(city_state_zip)        
        return mailing_address1,mailing_address2,mailing_city,mailing_state,mailing_zipcode
    #
    #
    def _parse_addr_lines(self, addr_lines):
        country = 'USA'
        if addr_lines[-1].upper() in ['UNITED STATES']: 
            country = 'United States'
            addr_lines = addr_lines[:-1] # remove the last line
        address1 = addr_lines[0]
        address2 = addr_lines[1] if len(addr_lines) >= 3 else ''
        #
        city,state,zipcode = self._parse_city_state_zip(addr_lines[-1])      
        #
        if city == 'Alaska' and address2 == 'Anchorage':
            city = address2
            address2 = ''
        #
        return address1,address2,city,state,zipcode,country
    #
    def _parse_city_state_zip(self, city_state_zip):
        idx = city_state_zip.find(',')
        city = city_state_zip[:idx]
        state_zip = city_state_zip[idx+1:].strip()
        state = state_zip[:2]
        zipcode = state_zip[3:]
        return city,state,zipcode
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