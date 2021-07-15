import json,csv
import os,sys
import scrapy
import logging
from datetime import datetime, timedelta
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher

#
# The Marijuana Enforcement Division (MED) : 
# https://www.colorado.gov/pacific/enforcement/statistics-and-resources
class COMarijuanaFacilitySpider(scrapy.Spider):    
    name = "pot_co_person"
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
        self.column_headers = ['firstname','middlename','lastname','suffix', 'license_number','license_type','license_status','expiration_date', 
                               'business_licensee','business_license_type','business_license_number','business_license_status', 'sourceUrl','collectedDate']
        #        
        self.csvFilename  = "CO_Marijuana_People.csv"
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
        self.search_pagers = []
        self.CO_Person_Search_Url = "https://codor.mylicense.com/MED_Verification/"
        self.license_type = 'Associated Key - Non-Resident'
        #
        self.pager_post_queue = {}
        self.parsed_rec_count = 0

    #
    def start_requests(self):
        debug_mode = getattr(self, 'debug', None)
        if debug_mode: 
            self.debug_mode = True
            print("debug_mode: {}".format(debug_mode))
        #
        lic_type = getattr(self, 'lic', None)
        if lic_type: 
            if lic_type[0] in ['R','r']:
                self.license_type = 'Associated Key - Resident'
            elif lic_type[0] in ['P','p']:
                self.license_type = 'Associated Person'
            elif lic_type[0] in ['N','n']:
                self.license_type = 'Associated Key - Non-Resident'
            else:
                raise ValueError("start_requests UNEXPECTED lic_type={}".format(lic_type))
            print("Input: {}; license_type: {}".format(lic_type, self.license_type))
        #
        yield scrapy.Request(self.CO_Person_Search_Url, self.parse_search)
    #
    # the pagination of search results works in IE browser only (Chrome & Edge are not working)!!!
    def parse_search(self, response):
        #
        post_action = response.xpath("//form[@method='post']/@action").extract_first()
        post_url = response.urljoin(post_action)
        #
        license_type_list = [ x for x in response.xpath("//select[@name='t_web_lookup__license_type_name']/option/@value").extract() if x ]
        status_name_list  = [ x for x in response.xpath("//select[@name='t_web_lookup__license_status_name']/option/@value").extract() if x ]
        #print(license_type_list)
        viewstate = response.xpath("//form[@method='post']//input[@name='__VIEWSTATE']/@value").extract_first()
        viewstateGENERATOR = response.xpath("//form[@method='post']//input[@name='__VIEWSTATEGENERATOR']/@value").extract_first()
        eventVALIDATION = response.xpath("//form[@method='post']//input[@name='__EVENTVALIDATION']/@value").extract_first()
        #
        self.search_pagers.append('1')  # the first page
        self.search_pagers.append('...:datagrid_results$_ctl44$_ctl0')  # link to the previous pager.
        # Associated Key - Non-Resident
        # Associated Key - Resident
        # Associated Person
        for license_idx, license_type in enumerate(license_type_list):
            #print('license_type={}'.format(license_type))
            #if license_type != 'Associated Key - Non-Resident': continue  # only these three types are associated with facilities
            #if license_type != 'Associated Key - Resident': continue  # only these three types are associated with facilities
            #if license_type != 'Associated Person': continue  # only these three types are associated with facilities
            if license_type != self.license_type: continue
            #
            # for status_idx, status_name in enumerate(status_name_list):
            #     if status_name not in ['Approved']: continue 
            status_name = ''
            form_data = self._get_post_form_data(license_type, status_name, viewstate, viewstateGENERATOR, eventVALIDATION, '', None)
            # #
            yield scrapy.FormRequest( post_url, formdata=form_data, callback=self.parse_result, 
                                    meta={'license_idx':str(license_idx),'license_type':license_type,'status_name':status_name, 'pager_text':'1', 'last_page':False} )
            
    #
    def parse_result(self, response):
        license_type = response.meta['license_type']
        license_idx = response.meta['license_idx']
        status_name = response.meta['status_name']
        pager_text  = response.meta['pager_text']
        # the predicate of td[5] requires atleast 5 td child nodes under tr
        tr_nodes = response.xpath("//table[@id='datagrid_results']/tr[td[5]]")
        page_rec_count = len(tr_nodes)
        for tr in tr_nodes:
            td_list = tr.xpath("./td")
            #
            anchor = td_list[0].xpath(".//a")
            detail_link = anchor.xpath("./@href").extract_first()
            person_name = anchor.xpath("./text()").extract_first()            
            idx = person_name.find(',')
            lastname = person_name[:idx]
            remaining_name = person_name[idx+1:].strip()
            idx = remaining_name.find(',')
            if idx == -1:
                firstname = remaining_name
                suffix = ''
            else:
                firstname = remaining_name[:idx]
                suffix = remaining_name[idx+1:].strip()
            #
            middlename = ''
            name_list = firstname.split(' ')
            if len(name_list) > 1:
                firstname = name_list[0]
                middlename = ' '.join(name_list[1:])
            #
            license_number = td_list[1].xpath("./span/text()").extract_first()
            license_type = td_list[2].xpath("./span/text()").extract_first()
            status = td_list[3].xpath("./span/text()").extract_first()
            #
            toc_data = {'license_number':license_number,'license_type':license_type,'lastname':lastname,'firstname':firstname,'middlename':middlename,'suffix':suffix,'status':status}
            #
            yield scrapy.Request(response.urljoin(detail_link), callback=self.parse_detail, meta={'toc_data':toc_data, 'page_rec_count':page_rec_count}) 
        #
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
            form_data = self._get_post_form_data(license_type, status_name, viewstate, viewstateGENERATOR, eventVALIDATION, eventTarget, currentPageIndex)
            #print(form_data)
            self.pager_post_queue[pager_key] = [ post_url, form_data, {'license_idx':str(license_idx),'license_type':license_type,'status_name':status_name, 'pager_text':anc_text, 'last_page':False} ]
    #
    #
    def parse_detail(self, response):
        #
        date_collected = datetime.now().strftime('%Y-%m-%d %H:%M:%S')      
        toc_data = response.meta['toc_data']
        lastname = toc_data['lastname']
        firstname = toc_data['firstname']
        middlename = toc_data['middlename']
        suffix  = toc_data['suffix']
        # 
        page_rec_count = response.meta['page_rec_count']
        #
        license_type = response.xpath("//span[@id='_ctl32__ctl1_license_type']/text()").extract_first()
        license_number = response.xpath("//span[@id='_ctl32__ctl1_license_no']/text()").extract_first()
        license_status = response.xpath("//span[@id='_ctl32__ctl1_status']/text()").extract_first()
        expiration_date = response.xpath("//span[@id='_ctl32__ctl1_expiry']/text()").extract_first()
        #
        # we are ignoring person with no 'Linked Business Information'
        for table in response.xpath("//table[@role='presentation']//tr[td[span[text()='Licensee:']]]/.."):
            tr_list = table.xpath('./tr[td[2]]')
            if len(tr_list) != 4: raise ValueError("{}: ERROR...unexpected len(tr_list)={}".format(sourceUrl, len(tr_list)))
            business_licensee = tr_list[0].xpath("./td[2]/span/text()").extract_first()
            business_license_type = tr_list[1].xpath("./td[2]/span/text()").extract_first()
            business_license_number = tr_list[2].xpath("./td[2]/a/text()").extract_first()
            business_license_status = tr_list[3].xpath("./td[2]/span/text()").extract_first()
            #
            row = [firstname,middlename,lastname,suffix, license_number,license_type,license_status,expiration_date, 
                   business_licensee, business_license_type, business_license_number, business_license_status, self.CO_Person_Search_Url, date_collected]
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
    #
    def _get_post_form_data(self, license_type, status_name, viewstate, viewstateGENERATOR, eventVALIDATION, eventTarget, currentPageIndex):
        if len(eventTarget) == 0:
            return {'__EVENTTARGET':'',
                    '__EVENTARGUMENT':'',
                    '__LASTFOCUS':'',
                    '__VIEWSTATE':viewstate,
                    '__VIEWSTATEGENERATOR':viewstateGENERATOR,
                    '__EVENTVALIDATION':eventVALIDATION,
                    't_web_lookup__license_type_name':license_type,
                    't_web_lookup__first_name':'',
                    't_web_lookup__license_no':'',
                    't_web_lookup__last_name':'',
                    't_web_lookup__license_status_name':status_name,
                    'sch_button':'Search'
                    }
        elif currentPageIndex:
            return {'__EVENTTARGET':eventTarget,
                    '__EVENTARGUMENT':'',
                    '__LASTFOCUS':'',
                    '__VIEWSTATE':viewstate,
                    '__VIEWSTATEGENERATOR':viewstateGENERATOR,
                    '__EVENTVALIDATION':eventVALIDATION,
                    'CurrentPageIndex': currentPageIndex
                    }
        else:
            return {'__EVENTTARGET':eventTarget,
                    '__EVENTARGUMENT':'',
                    '__LASTFOCUS':'',
                    '__VIEWSTATE':viewstate,
                    '__VIEWSTATEGENERATOR':viewstateGENERATOR,
                    '__EVENTVALIDATION':eventVALIDATION
                    }

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
        #
        #print(self.pager_post_queue)