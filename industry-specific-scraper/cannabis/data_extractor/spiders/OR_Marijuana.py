import json,csv
import os,sys
import scrapy
import logging
from datetime import datetime, timedelta
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher

#
class ORMarijuanaSpider(scrapy.Spider):    
    name = "pot_or"
    #
    def __init__(self, *args, **kwargs):
        logger = logging.getLogger('scrapy.spidermiddlewares.httperror')
        logger.setLevel(logging.ERROR)  # set logging level here
        super().__init__(*args, **kwargs)
        #
        dispatcher.connect(self.spider_closed, signals.spider_closed)               
        #
        self.debug_mode = False
        self.totalCount = 0
        self.column_headers = ['license_number', 'license_type', 'active', 'licensee_name', 'business_name', 'address1','address2','city','state','zipcode','country','phone',
                               'mailing_address1','mailing_address2','mailing_city','mailing_state','mailing_zipcode','mailing_country', 
                               'contact1_title','contact1_firstname','contact1_middlename','contact1_lastname', 
                               'contact2_title','contact2_firstname','contact2_middlename','contact2_lastname','contact3_title','contact3_firstname','contact3_middlename','contact3_lastname',
                               'entity_name', 'entity_type', 'entity_status', 'registry_number', 'sourceUrl','collectedDate']
        self.csv_encoding = 'utf-8' 
        self.csvFilename  = "OR_Marijuana_Businesses.csv"
        self.csv_file = open(self.csvFilename, 'w', newline='', encoding=self.csv_encoding) #, encoding='windows-1250', errors ='ignore')
        self.csvwriter = csv.writer(self.csv_file)
        if self.csv_encoding == 'utf-8': self.csv_file.write(u'\ufeff') #.write(codecs.BOM_UTF8)
        self.csvwriter.writerow(self.column_headers)       
        #     
        self.MarijuanaLicenses = []
        self.OR_SOS_search_home = "http://egov.sos.state.or.us/br/pkg_web_name_srch_inq.login"
        # http://egov.sos.state.or.us/br/pkg_web_name_srch_inq.do_name_srch?p_name=HOTBOX%20FARMS&p_regist_nbr=&p_srch=PHASE1PO&p_print=FALSE&p_entity_status=ACTINA
        # http://egov.sos.state.or.us/br/pkg_web_name_srch_inq.do_name_srch?p_name=HOTBOX%20FARMS&p_regist_nbr=&p_srch=PHASE1P&p_print=FALSE&p_entity_status=ACTINA
        # http://egov.sos.state.or.us/br/pkg_web_name_srch_inq.do_name_srch?p_name=HOTBOX%20FARMS&p_regist_nbr=&p_srch=PHASE1&p_print=FALSE&p_entity_status=ACTINA
        self.OR_SOS_search_url = "http://egov.sos.state.or.us/br/pkg_web_name_srch_inq.do_name_srch?p_name={}&p_regist_nbr=&p_srch={}&p_print=FALSE&p_entity_status=ACTINA"
    #
    #
    def start_requests(self):
        debug_mode = getattr(self, 'debug', None)
        if debug_mode: 
            self.debug_mode = True
            print("debug_mode: {}".format(debug_mode))
        #
        input_file = getattr(self, 'input', None)
        if not input_file: input_file = 'OR_MarijuanaLicenses_approved.csv'
        #
        self.MarijuanaLicenses = self._read_MarijuanaLicenses(input_file)
        #
        yield scrapy.Request(self.OR_SOS_search_home, self.parse)

    #
    def parse(self, response):
        #if self.debug_mode: self.log(response.text)
        #
        search_method = "PHASE1PO"  # Exact words in exact word order. (Only. As keyed) Fastest.
        # search_method = "PHASE1P"   # Exact words in exact word order. (Followed by anything else)
        # search_method = "PHASE1"    # Exact words in any word order.
        #
        for mj_rec in self.MarijuanaLicenses:
            business_name = mj_rec['business_name']
            #if business_name not in ["Hotbox Farms",'Wild Green Horizon','Amber Creek']: continue
            search_url = self.OR_SOS_search_url.format(business_name, search_method)                
            yield scrapy.Request(search_url, self.parse_search, meta=mj_rec)
    #
    #
    def parse_search(self, response):
        mr = response.meta
        mj_rec = { 'license_number':mr['license_number'], 'license_type':mr['license_type'], 'active':mr['active'], 'licensee_name':mr['licensee_name'], 'business_name':mr['business_name'] }
        #
        for tr in response.xpath("//form[@name='DO_NAME_SRCH']/table//tr[td[7]]"):
            if tr.xpath('./td/font'): continue
            td_list = tr.xpath("./td")
            record_no = td_list[0].xpath("./text()").extract_first()
            entity_type = td_list[1].xpath("./text()").extract_first()
            entity_status = td_list[2].xpath("./text()").extract_first()
            registry_number = td_list[3].xpath("./a/text()").extract_first()
            registry_link = td_list[3].xpath("./a/@href").extract_first()
            name_status = td_list[4].xpath("./text()").extract_first()
            entity_name = td_list[5].xpath("./a/text()").extract_first()
            assoc_search = td_list[6].xpath("./text()").extract_first()
            # print([record_no,entity_type,entity_status,name_status,entity_name,registry_number,registry_link,assoc_search])
            #
            mj_rec['entity_type'] = entity_type
            mj_rec['entity_status'] = entity_status
            mj_rec['registry_number'] = registry_number
            mj_rec['entity_name'] = entity_name
            #
            yield scrapy.Request(response.urljoin(registry_link), callback=self.parse_entity, meta=mj_rec)
    #
    def parse_entity(self, response):
        mr = response.meta
        mj_rec = { 'license_number':mr['license_number'], 'license_type':mr['license_type'], 'active':mr['active'], 'licensee_name':mr['licensee_name'], 'business_name':mr['business_name'], 
                   'entity_name':mr['entity_name'], 'entity_type':mr['entity_type'], 'entity_status':mr['entity_status'], 'registry_number':mr['registry_number'] }
        mailing_address = {'address1':'', 'address2':'', 'city':'', 'state':'', 'zipcode':'', 'country':''}
        #
        date_collected = datetime.now().strftime('%Y-%m-%d %H:%M:%S')      
        #
        name_records = []
        inside_record = False
        idx_registry_date = -1
        rec_type = country = address1 = ''            
        for idx, tr in enumerate(response.xpath("//form[@name='SHOW_DETL']/table//tr[td[2]]")):
            if tr.xpath("./td//a[text()='New Search']"): continue
            td_list = tr.xpath("./td")
            text_temp_list = [x.strip(' \t\r\n') for x in td_list[0].xpath(".//b/text()").extract() if x.strip(' \t\r\n')]
            text_column1 = text_temp_list[0] if len(text_temp_list) >= 1 else ''
            if text_column1 == "Business Entity Name": 
                break  # reach the end of the fields we want
            if text_column1 == "Registry Nbr": 
                idx_registry_date = idx + 1
                continue
            # print("{}: {}: {}".format(idx, len(td_list), text_column1))
            if idx == idx_registry_date: 
                if text_column1 != '': raise ValueError("parse_entity: expecting empty text_column1, but got {}".format(text_column1))
                if inside_record: raise ValueError("parse_entity: expecting start of a record and should not be inside a record!!!")
                registry_date = td_list[4].xpath("./text()").extract_first()
                next_renewal_date = td_list[5].xpath("./text()").extract_first()
            elif text_column1 == "Type":
                if inside_record: raise ValueError("parse_entity: expecting start of a record and should not be inside a record!!!")
                inside_record = True
                of_record_registry_number = of_record_registry_name = ''
                firstname = middletname = lastname = address1 = address2 = city = state = zipcode = country = ''
                text_temp_list = [x.strip(' \t\r\n') for x in td_list[2].xpath("./text()").extract() if x.strip(' \t\r\n')]
                rec_type = text_temp_list[0]
            elif text_column1 == "Name":
                if not inside_record: raise ValueError("parse_entity: expecting inside a record for text_column1={}".format(text_column1))
                firstname =  td_list[1].xpath("./text()").extract_first()
                middletname =  td_list[2].xpath("./text()").extract_first()
                lastname =  td_list[3].xpath("./text()").extract_first()
            elif text_column1 == 'Of Record':
                if not inside_record: raise ValueError("parse_entity: expecting inside a record for text_column1={}".format(text_column1))
                of_record_registry_number = td_list[1].xpath("./a/text()").extract_first()
                of_record_registry_name =  td_list[2].xpath("./text()").extract_first()
            elif text_column1 == "Addr 1":
                if not inside_record: raise ValueError("parse_entity: expecting inside a record for text_column1={}".format(text_column1))
                address1 = td_list[1].xpath("./text()").extract_first().strip()
            elif text_column1 == "Addr 2":
                if not inside_record: raise ValueError("parse_entity: expecting inside a record for text_column1={}".format(text_column1))
                address2 = td_list[1].xpath("./text()").extract_first().strip()
            elif text_column1 == "CSZ":
                if not inside_record: raise ValueError("parse_entity: expecting inside a record for text_column1={}".format(text_column1))                
                city = td_list[1].xpath("./text()").extract_first()
                state = td_list[2].xpath("./text()").extract_first()
                zipcode = td_list[3].xpath("./text()").extract_first()
                country = td_list[6].xpath("./text()").extract_first()
                if country in ['UNITED STATES OF AMERICA']: country = 'USA'
            # got a complete record:                 
            if rec_type and country and address1:
                inside_record = False
                if rec_type == "PRINCIPAL PLACE OF BUSINESS":
                    mj_rec['address1'] = address1
                    mj_rec['address2'] = address2
                    mj_rec['city'] = city
                    mj_rec['state'] = state
                    mj_rec['zipcode'] = zipcode
                    mj_rec['country'] = country
                elif rec_type == "MAILING ADDRESS":                    
                    mailing_address['address1'] = address1
                    mailing_address['address2'] = address2
                    mailing_address['city'] = city
                    mailing_address['state'] = state
                    mailing_address['zipcode'] = zipcode
                    mailing_address['country'] = country
                elif lastname:
                    name_records.append({'title': rec_type, 'firstname':firstname, 'middlename':middletname, 'lastname':lastname, 
                                        'address1':address1, 'address2':address2, 'city':city,'state':state, 'zipcode':zipcode, 'country':country})   
                # else:
                #     print("Ignore rec_type={} for {}".format(rec_type, response.url))                 
        #                
        contact1_title = contact1_firstname = contact1_middlename = contact1_lastname = ''
        contact2_title = contact2_firstname = contact2_middlename = contact2_lastname = ''
        contact3_title = contact3_firstname = contact3_middlename = contact3_lastname = ''
        if len(name_records) >= 1:
            rec1 = name_records[0]
            contact1_title = rec1['title']
            contact1_firstname = rec1['firstname']
            contact1_middlename = rec1['middlename']
            contact1_lastname = rec1['lastname']
            if 'address1' not in mj_rec:         
                mj_rec['address1'] = rec1['address1']
                mj_rec['address2'] = rec1['address2']
                mj_rec['city'] = rec1['city']
                mj_rec['state'] = rec1['state']
                mj_rec['zipcode'] = rec1['zipcode']
                mj_rec['country'] = rec1['country']
        if len(name_records) >= 2:
            rec1 = name_records[1]
            contact2_title = rec1['title']
            contact2_firstname = rec1['firstname']
            contact2_middlename = rec1['middlename']
            contact2_lastname = rec1['lastname']
        if len(name_records) >= 3:
            rec1 = name_records[2]
            contact3_title = rec1['title']
            contact3_firstname = rec1['firstname']
            contact3_middlename = rec1['middlename']
            contact3_lastname = rec1['lastname']
        #
        if 'address1' not in mj_rec:
            print("{}".format(mj_rec['license_number']))
        else:
            phone = ''
            row = [mj_rec['license_number'], mj_rec['license_type'], mj_rec['active'], mj_rec['licensee_name'], mj_rec['business_name'],         
                mj_rec['address1'], mj_rec['address2'], mj_rec['city'], mj_rec['state'], mj_rec['zipcode'], mj_rec['country'], phone,
                mailing_address['address1'], mailing_address['address2'], mailing_address['city'], mailing_address['state'], mailing_address['zipcode'], mailing_address['country'],               
                contact1_title,contact1_firstname,contact1_middlename,contact1_lastname, contact2_title,contact2_firstname,contact2_middlename,contact2_lastname,contact3_title,contact3_firstname,contact3_middlename,contact3_lastname,
                mj_rec['entity_name'], mj_rec['entity_type'], mj_rec['entity_status'], mj_rec['registry_number'], response.url, date_collected]
            self.csvwriter.writerow(row)
            self.totalCount += 1
                            
    #
    #
    def _read_MarijuanaLicenses(self, input_file):
        #
        # ,,OREGON LIQUOR CONTROL COMMISSION  ,,,,,
        # ,,Marijuana Business Licenses Approved as of 3/9/2018,,,,,
        # LICENSE NUMBER,LICENSEE NAME,BUSINESS NAME,LICENSE TYPE,ACTIVE,COUNTY,Retail Delivery,Medical Grade
        # 050 100037147CC,Hotbox Farms LLC,Hotbox Farms,Recreational Retailer,Yes,Baker,,Yes
        # 050 10011127277,"Scott, Inc",420VILLE,Recreational Retailer,Yes,Baker,,
        #
        # 050 10079928B63,CFA RETAIL LLC,Chalice Farms,Recreational Retailer,Yes,Yamhill,Yes,Yes
        # ,,,,,,,
        # ,,,,,,,
        toc_data = []
        with open(input_file, 'rt', encoding=self.csv_encoding) as f:
            reader = csv.reader(f, delimiter=',')
            idx_data = -1
            for idx,row in enumerate(reader):
                if row[0] == 'LICENSE NUMBER': idx_data = idx                
                if not row[0]: idx_data = -1
                if idx_data < 0: continue
                # print(row)
                toc_data.append( {'license_number':row[0].strip(),'licensee_name':row[1].strip(),'business_name':row[2].strip(),'license_type':row[3].strip(),
                                  'active':row[4].strip(),'county':row[5].strip(),'retail_delivery':row[6].strip(),'medical_grade':row[7].strip()} )
        #   
        print("_read_MarijuanaLicenses: Reade {} records from {}".format(len(toc_data), input_file))     
        #
        return toc_data
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