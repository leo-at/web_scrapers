import json,csv
import os,sys
import scrapy
import logging
from datetime import datetime, timedelta
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher

# Using https://codor.mylicense.com/med_verification/Search.aspx?facility=Y, search for All "License Type", and download file directly.
#
#
# The Marijuana Enforcement Division (MED) : 
# https://www.colorado.gov/pacific/enforcement/statistics-and-resources
class COMarijuanaFacilitySpider(scrapy.Spider):    
    name = "pot_co"
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
        self.column_headers = ['business_name','tradename','license_number','license_type','license_status','expiration_date',
                               'address1','address2','city','state','zipcode','country',
                               'linked_licensee','linked_lic_number','linked_lic_type','linked_lic_status','sourceUrl','collectedDate']
        #        
        self.csvFilename  = "CO_Marijuana_Businesses.csv"
        if os.path.exists(self.csvFilename): 
            append_write = 'a'    
        else:
            append_write = 'w'
        #    
        csv_encoding = 'utf-8' 
        self.csv_file = open(self.csvFilename, append_write, newline='', encoding=csv_encoding) #, encoding='windows-1250', errors ='ignore')
        self.csvwriter = csv.writer(self.csv_file)
        if csv_encoding == 'utf-8': self.csv_file.write(u'\ufeff') #.write(codecs.BOM_UTF8)
        if append_write == 'w': self.csvwriter.writerow(self.column_headers)       
        #
        self.license_type_list = self._get_facility_license_types()
        #
        self.search_pagers = []
        self.CO_Person_Search_Url = "https://codor.mylicense.com/MED_Verification/"
        self.CO_Facility_Search_Url = "https://codor.mylicense.com/MED_Verification/Search.aspx?facility=Y"
        self.license_type = 'Center - Type 1'
        #
        self.pager_post_queue = {}
        self.parsed_rec_count = 0
        
    #
    #
    def start_requests(self):
        debug_mode = getattr(self, 'debug', None)
        if debug_mode: 
            self.debug_mode = True
            print("debug_mode: {}".format(debug_mode))
        #
        lic_no = getattr(self, 'lic', None)
        if lic_no:
            if lic_no.upper() == 'ALL':
                self.license_type = ''
            elif lic_no.isdigit() and int(lic_no) >= 1: 
                self.license_type = self.license_type_list[int(lic_no)-1]
            else:
                raise ValueError("start_requests UNEXPECTED lic_no={}".format(lic_no))
            print("Input: {}; license_type: {}".format(lic_no, self.license_type))
        #
        yield scrapy.Request(self.CO_Person_Search_Url)   
    #
    def parse(self, response):
        yield scrapy.Request(self.CO_Facility_Search_Url, self.parse_search)
    #
    # the pagination of search results works in IE browser only (Chrome & Edge are not working)!!!
    def parse_search(self, response):
        #
        post_action = response.xpath("//form[@method='post']/@action").extract_first()
        post_url = response.urljoin(post_action)
        #
        license_type_list = [ x for x in response.xpath("//select[@name='t_web_lookup__license_type_name']/option/@value").extract() if x ]
        #print(license_type_list)
        viewstate = response.xpath("//form[@method='post']//input[@name='__VIEWSTATE']/@value").extract_first()
        viewstateGENERATOR = response.xpath("//form[@method='post']//input[@name='__VIEWSTATEGENERATOR']/@value").extract_first()
        eventVALIDATION = response.xpath("//form[@method='post']//input[@name='__EVENTVALIDATION']/@value").extract_first()
        #
        self.search_pagers.append('1')  # the first page
        self.search_pagers.append('...:datagrid_results$_ctl44$_ctl0')  # link to the previous pager.
        #
        if len(self.license_type) == 0:  # All
            form_data = self._get_post_form_data('', viewstate, viewstateGENERATOR, eventVALIDATION, '', None)
            #
            yield scrapy.FormRequest( post_url, formdata=form_data, callback=self.parse_result, 
                                      meta={'license_idx':'0','license_type':'', 'pager_text':'1', 'last_page':False} )
        else:
            for license_idx, license_type in enumerate(license_type_list):
                if license_type != self.license_type: continue
                #
                form_data = self._get_post_form_data(license_type, viewstate, viewstateGENERATOR, eventVALIDATION, '', None)
                #
                yield scrapy.FormRequest( post_url, formdata=form_data, callback=self.parse_result, 
                                        meta={'license_idx':str(license_idx),'license_type':license_type, 'pager_text':'1', 'last_page':False} )

    #
    def parse_result(self, response):
        license_type = response.meta['license_type']
        license_idx = response.meta['license_idx']
        pager_text  = response.meta['pager_text']
        # the predicte of td[5] requires atleast 5 td child nodes under tr
        tr_nodes = response.xpath("//table[@id='datagrid_results']/tr[td[5]]") 
        page_rec_count = len(tr_nodes)
        for tr in tr_nodes:
            td_list = tr.xpath("./td")
            #
            anchor = td_list[0].xpath(".//a")
            detail_link = anchor.xpath("./@href").extract_first()
            business_name = anchor.xpath("./text()").extract_first()
            #
            license_number = td_list[1].xpath("./span/text()").extract_first()
            license_type = td_list[2].xpath("./span/text()").extract_first()
            status = td_list[3].xpath("./span/text()").extract_first()
            business_address = td_list[4].xpath("./span/text()").extract_first()
            #
            toc_data = {'license_number':license_number, 'license_type':license_type, 'business_name':business_name, 
                        'business_address':business_address, 'status':status, 'detail_link':detail_link}
            #print(json.dumps(toc_data))
            #
            yield scrapy.Request(response.urljoin(detail_link), callback=self.parse_detail, meta={'toc_data':toc_data, 'page_rec_count':page_rec_count}) 
        #
        post_action = response.xpath("//form[@method='post']/@action").extract_first()
        post_url = response.urljoin(post_action)
        #
        viewstate = response.xpath("//form[@method='post']//input[@name='__VIEWSTATE']/@value").extract_first()
        viewstateGENERATOR = response.xpath("//form[@method='post']//input[@name='__VIEWSTATEGENERATOR']/@value").extract_first()
        eventVALIDATION = response.xpath("//form[@method='post']//input[@name='__EVENTVALIDATION']/@value").extract_first()
        currentPageIndex = response.xpath("//form[@method='post']//input[@name='CurrentPageIndex']/@value").extract_first()        
        #
        for anchor in response.xpath("//a[contains(@href,'datagrid_results$_ctl44$_ctl')]"):
            anc_text = anchor.xpath('./text()').extract_first()
            ctl_href = anchor.xpath("./@href").extract_first()            
            idx1 = ctl_href.find("'")
            idx2 = ctl_href.find("'", idx1+1)
            if idx1 == -1:
                idx1 = ctl_href.find('"')
                idx2 = ctl_href.find('"', idx1+1)
            ctl_tgt = ctl_href[idx1+1:idx2]   
            #
            if anc_text == '...':
                pager_key = "{}:{}".format(anc_text,ctl_tgt)
            else:
                pager_key = anc_text
            if pager_key in self.search_pagers: continue
            self.search_pagers.append(pager_key)
            print(pager_key)   
            #
            eventTarget = ctl_tgt
            form_data = self._get_post_form_data(license_type, viewstate, viewstateGENERATOR, eventVALIDATION, eventTarget, currentPageIndex)
            #print(form_data)
            self.pager_post_queue[pager_key] = [ post_url, form_data, {'license_idx':str(license_idx),'license_type':license_type, 'pager_text':anc_text, 'last_page':False} ]
    
    #
    #
    def parse_detail(self, response):
        #
        date_collected = datetime.now().strftime('%Y-%m-%d %H:%M:%S')      
        toc_data = response.meta['toc_data']
        # 
        page_rec_count = response.meta['page_rec_count']
        # 
        business_name = response.xpath("//span[@id='_ctl25__ctl1_full_name']/text()").extract_first()
        #
        address1 = response.xpath("//span[@id='_ctl30__ctl1_addr_line_1']/text()").extract_first()
        address2 = ''
        city_state_zip = response.xpath("//span[@id='_ctl30__ctl1_addr_line_4']/text()").extract_first()
        county = response.xpath("//span[@id='_ctl30__ctl1_label_county']/text()").extract_first()
        #
        city,state,zipcode = self._parse_city_state_zip(city_state_zip)
        #
        tradename = response.xpath("//span[@id='_ctl35__ctl1_dba']/text()").extract_first()
        license_type = response.xpath("//span[@id='_ctl35__ctl1_license_type']/text()").extract_first()
        license_number = response.xpath("//span[@id='_ctl35__ctl1_license_no']/text()").extract_first()
        license_status = response.xpath("//span[@id='_ctl35__ctl1_status']/text()").extract_first()
        expiration_date = response.xpath("//span[@id='_ctl35__ctl1_expiry']/text()").extract_first()
        #
        for tr in response.xpath("//td[@style='background-color:White;']/../../tr"):
            td_list = tr.xpath("./td//tr/td[@class='rdata']")
            linked_licensee = td_list[0].xpath("./span/text()").extract_first()
            linked_lic_type = td_list[1].xpath("./span/text()").extract_first()
            linked_lic_number = td_list[2].xpath("./a/text()").extract_first()
            linked_lic_status = td_list[3].xpath("./span/text()").extract_first()
            #
            row = [business_name,tradename, license_number,license_type,license_status,expiration_date, address1,address2,city,state,zipcode,'USA',
                   linked_licensee,linked_lic_number,linked_lic_type,linked_lic_status, self.CO_Facility_Search_Url, date_collected]
            self.csvwriter.writerow(row)
            self.totalCount += 1
        #
        self.parsed_rec_count += 1
        # 
        if self.parsed_rec_count == page_rec_count:
            # ready to load the next page if exists
            anc_text =  None
            for anc_text,objData in self.pager_post_queue.items():
                post_url = objData[0]
                form_data = objData[1]
                meta_data = objData[2]
                self.parsed_rec_count = 0 # reset
                print('POST to {} for pager: {}'.format(post_url, anc_text))
                yield scrapy.FormRequest( post_url, formdata=form_data, callback=self.parse_result, meta=meta_data )
                break
            if anc_text: # remove the anc_text key from the dict
                self.pager_post_queue.pop(anc_text)

    #
    def _parse_city_state_zip(self, city_state_zip):
        idx = city_state_zip.rfind(' ')
        zipcode = city_state_zip[idx+1:]
        if zipcode and len(zipcode) == 9: zipcode = "{}-{}".format(zipcode[:5], zipcode[5:])
        #
        city_state = city_state_zip[:idx].strip()
        state = city_state[-2:]
        city  = city_state[:-3].strip()
        #
        return city,state,zipcode

    #
    def _get_post_form_data(self, license_type, viewstate, viewstateGENERATOR, eventVALIDATION, eventTarget, currentPageIndex):
        if len(eventTarget) == 0:
            return { '__EVENTTARGET':'',
                    '__EVENTARGUMENT':'',
                    '__LASTFOCUS':'',
                    '__VIEWSTATE':viewstate,
                    '__VIEWSTATEGENERATOR':viewstateGENERATOR,
                    '__EVENTVALIDATION':eventVALIDATION,
                    't_web_lookup__license_type_name':license_type,
                    't_web_lookup__addr_city':'',
                    't_web_lookup__license_no':'',
                    't_web_lookup__addr_state':'CO',
                    't_web_lookup__license_status_name':'',
                    't_web_lookup__addr_county':'',
                    't_web_lookup__doing_business_as':'',
                    't_web_lookup__addr_zipcode':'',
                    't_web_lookup__full_name':'',
                    'v_corp_personnel__corp_first_name':'',
                    'v_corp_personnel__corp_last_name':'',
                    'sch_button':'Search'
                    }
        elif currentPageIndex:
            return {'__EVENTTARGET':eventTarget,
                    '__EVENTARGUMENT':'',
                    '__VIEWSTATE':viewstate,
                    '__VIEWSTATEGENERATOR':viewstateGENERATOR,
                    '__EVENTVALIDATION':eventVALIDATION,
                    'CurrentPageIndex': currentPageIndex
                    }
        else:
            return {'__EVENTTARGET':eventTarget,
                    '__EVENTARGUMENT':'',
                    '__VIEWSTATE':viewstate,
                    '__VIEWSTATEGENERATOR':viewstateGENERATOR,
                    '__EVENTVALIDATION':eventVALIDATION
                    }
    #
    # list of facility license types: 25 for now
    #   search for each licence type must be done in separate spider run!
    def _get_facility_license_types(self):
        return [
            'Center - Type 1', 
            'Center - Type 2', 
            'Center - Type 3', 
            'Infused Product Manufacturer',             
            'MMJ Off- Premises Storage', 
            'MMJ Operator',
            'MMJ R&D Cultivation',
            'MMJ R&D Facility',
            'MMJ Testing Facility', 
            'MMJ Transporter',
            'MMJ Transporter- NP', # NP Indicates that the transporter does not have a Licensed Premises where marijuana can be stored. 
            'MMJ Transporter Off-Premises Storage',
            'Optional Premises',      
            'Qualified Institutional Investor (QII)',
            'Responsible Vendor',
            'Retail Marijuana Cultivation Facility', 
            'Retail Marijuana Products Mfg', 
            'Retail Marijuana Store', 
            'Retail Marijuana Testing Facility', 
            'Retail Off-Premises Storage',
            'Retail Operator', 
            'Retail Transporter', 
            'RMJ Transporter- NP', # NP Indicates that the transporter does not have a Licensed Premises where marijuana can be stored. 
            'RMJ Transporter Off-Premises Storage',
            'Vendor'  # https://www.colorado.gov/pacific/enforcement/responsible-vendor-training-program-providers     
        ]
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