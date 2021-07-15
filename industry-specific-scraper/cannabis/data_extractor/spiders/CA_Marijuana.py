import json,csv
import os,sys
import scrapy
import logging
from datetime import datetime, timedelta
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher


#
class CAMarijuanaSpider(scrapy.Spider):    
    name = "pot_ca"
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
        self.permit_pagers = {}
        self.totalCount = 0
        self.column_headers = ['record_number','business_name','trade_name','start_date','record_type','expires_on','status',
                               'address1','address2','city','state','zipcode','country','phone','phone2','email','website',
                               'owner1_title','owner1_fname','owner1_lname','owner2_title','owner2_fname','owner2_lname','owner3_title','owner3_fname','owner3_lname',
                               'sourceUrl','collectedDate']
        #        
        csv_encoding = 'utf-8' 
        self.csvFilename  = "CA_Marijuana_Businesses.csv"
        self.csv_file = open(self.csvFilename, 'w', newline='', encoding=csv_encoding) #, encoding='windows-1250', errors ='ignore')
        self.csvwriter = csv.writer(self.csv_file)
        if csv_encoding == 'utf-8': self.csv_file.write(u'\ufeff') #.write(codecs.BOM_UTF8)
        self.csvwriter.writerow(self.column_headers)       
        #                                       
    #
    #
    def start_requests(self): 
        yield scrapy.Request("https://aca5.accela.com/bcc/Welcome.aspx", self.parse)
    #
    def parse(self, response):
        yield scrapy.Request("https://aca5.accela.com/bcc/Cap/CapHome.aspx?module=Licenses&ShowMyPermitList=N", self.parse_search)
    #
    def parse_search(self, response):
        #
        post_action = response.xpath("//form[@method='post']/@action").extract_first()
        post_url = response.urljoin(post_action)
        permit_type_list = [x for x in response.xpath("//select[@id='ctl00_PlaceHolderMain_generalSearchForm_ddlGSPermitType']/option/@value").extract() if x]
        #
        viewstate = response.xpath("//form[@method='post']//input[@name='__VIEWSTATE']/@value").extract_first()
        viewstateGENERATOR = response.xpath("//form[@method='post']//input[@name='__VIEWSTATEGENERATOR']/@value").extract_first()
        ACA_CS_FIELD = response.xpath("//form[@method='post']//input[@name='ACA_CS_FIELD']/@value").extract_first()
        #
        eventTarget = 'ctl00$PlaceHolderMain$btnNewSearch'
        scriptManager = 'ctl00$PlaceHolderMain$updatePanel|ctl00$PlaceHolderMain$btnNewSearch'
        for permit_idx, permit_type in enumerate(permit_type_list):
            form_data = self._get_post_form_data(permit_type, eventTarget, scriptManager, viewstate, viewstateGENERATOR, ACA_CS_FIELD)
            #if permit_type not in ['Licenses/Adult Use Cannabis/Distributor/License']: continue
            #if permit_type not in ['Licenses/Adult Use Cannabis/Retailer/Temporary License','Licenses/Adult Use Cannabis/Distributor/Temporary License']: continue
            #
            yield scrapy.FormRequest( post_url, formdata=form_data, callback=self.parse_result, 
                                      meta={'post_url':post_url,'permit_idx':str(permit_idx),'permit_type':permit_type, 'more_pager_index':0, 'last_page':False} 
                                    , headers={'ADRUM':'isAjax:true', 'X-MicrosoftAjax':'Delta=true','X-Requested-With':'XMLHttpRequest'})
           
    #
    def parse_result(self, response):
        post_url = response.meta['post_url']
        last_page = response.meta['last_page']
        permit_idx = response.meta['permit_idx']
        permit_type = response.meta['permit_type']
        more_pager_index = response.meta['more_pager_index']
        first_ctl_tgt = 'ctl00$PlaceHolderMain$dgvPermitList$gdvPermitList$ctl23$ctl02'
        first_pager_key = '{}:0:{}'.format(permit_idx, first_ctl_tgt)
        if permit_idx not in self.permit_pagers: self.permit_pagers[permit_idx] = [first_pager_key]
        search_pagers = self.permit_pagers[permit_idx]
        #
        for tr in response.xpath("//tr[contains(@class,'ACA_TabRow_Odd') or contains(@class,'ACA_TabRow_Even')]"):
            td_list = tr.xpath("./td[@class='ACA_AlignLeftOrRightTop']")
            if len(td_list) != 9: raise ValueError("ERROR...searching {} for {}".format(permit_type, response.url))
            start_date = td_list[1].xpath(".//span/text()").extract_first()
            record_link   = td_list[2].xpath(".//a/@href").extract_first()
            record_number = td_list[2].xpath(".//a//span/text()").extract_first()
            record_type   = td_list[3].xpath(".//span/text()").extract_first()
            business_name = td_list[4].xpath(".//span/text()").extract_first()
            business_address = td_list[5].xpath(".//span/text()").extract_first()
            expires_on = td_list[6].xpath(".//span/text()").extract_first()
            status = td_list[7].xpath(".//span/text()").extract_first()
            toc_data = {'start_date':start_date, 'record_number':record_number, 'record_type':record_type, 'permit_type':permit_type, 
                        'business_name':business_name, 'business_address':business_address, 'expires_on':expires_on, 'status':status}
            #print(json.dumps(toc_data))
            #
            yield scrapy.Request(response.urljoin(record_link), callback=self.parse_detail, meta={'toc_data':toc_data}) 
        #
        if last_page: return  # stop looking for more pages: this is neceesary because the ctl_tgt format changes on the last page somehow!!!
        #
        response_text = response.text
        txt_len = len('|__VIEWSTATE|')
        idx1 = response_text.find('|__VIEWSTATE|')
        idx2 = response_text.find('|', idx1+txt_len+1)
        viewstate = response_text[idx1+txt_len:idx2]
        #print("__VIEWSTATE: {} - {}".format(idx1+txt_len, idx2))
        #
        txt_len = len('|__VIEWSTATEGENERATOR|')
        idx1 = response_text.find('|__VIEWSTATEGENERATOR|')
        idx2 = response_text.find('|', idx1+txt_len+1)
        viewstateGENERATOR = response_text[idx1+txt_len:idx2]
        #print("__VIEWSTATEGENERATOR={}".format(viewstateGENERATOR))
        #
        txt_len = len('|ACA_CS_FIELD|')
        idx1 = response_text.find('|ACA_CS_FIELD|')
        idx2 = response_text.find('|', idx1+txt_len+1)
        ACA_CS_FIELD = response_text[idx1+txt_len:idx2]
        #print("ACA_CS_FIELD={}".format(ACA_CS_FIELD))
        #
        # the @class equality check excludes the Prev/Next links.
        anchor_list = response.xpath("//tr[contains(@class,'ACA_Table_Pages')]//td[@class='aca_pagination_td']/a")
        for ncr_id, anchor in enumerate(anchor_list):
            anc_text = anchor.xpath("./text()").extract_first()
            ctl_href = anchor.xpath("./@href").extract_first() # javascript:__doPostBack('ctl00$PlaceHolderMain$dgvPermitList$gdvPermitList$ctl23$ctl03','');var p = new ProcessLoading();p.showLoading(false);
            # Skip the first page since already clicked by the '...' on the previous pager.  The first page of initial pager is already handled above.
            last_page = False                
            if anc_text != '...':
                if ncr_id == len(anchor_list)-1: last_page = True
                if int(anc_text) % 10 == 1: continue                 
            # 
            idx1 = ctl_href.find("'")
            idx2 = ctl_href.find("'", idx1+1)
            if idx1 == -1:
                idx1 = ctl_href.find('"')
                idx2 = ctl_href.find('"', idx1+1)
            ctl_tgt = ctl_href[idx1+1:idx2]   
            #            
            if anc_text == '...': 
                if first_ctl_tgt == ctl_tgt: continue # don't going back to the previous pager
                more_pager_index += 1
            #
            pager_key = "{}:{}:{}".format(permit_idx, more_pager_index, ctl_tgt)
            if pager_key in search_pagers: continue
            search_pagers.append(pager_key)
            print(pager_key)            
            #
            #if not ctl_tgt.endswith('03'): continue
            eventTarget = ctl_tgt
            scriptManager = 'ctl00$PlaceHolderMain$dgvPermitList$updatePanel|{}'.format(ctl_tgt)
            form_data = self._get_post_form_data(permit_type, eventTarget, scriptManager, viewstate, viewstateGENERATOR, ACA_CS_FIELD)
            #print(form_data)
            #            
            yield scrapy.FormRequest( post_url, formdata=form_data, callback=self.parse_result, 
                                      meta={'post_url':post_url,'permit_idx':permit_idx,'permit_type':permit_type, 'more_pager_index':more_pager_index,'last_page':last_page} 
                                     ,headers={'ADRUM':'isAjax:true', 'X-MicrosoftAjax':'Delta=true','X-Requested-With':'XMLHttpRequest'})
    #
    #
    def parse_detail(self, response):
        #
        date_collected = datetime.now().strftime('%Y-%m-%d %H:%M:%S')      
        toc_data = response.meta['toc_data']
        #
        businessname = response.xpath("//span[@class='contactinfo_businessname']/text()").extract_first()
        trade_name = response.xpath("//span[@class='contactinfo_tradename']/text()").extract_first()
        addressline3 = response.xpath("//span[@class='contactinfo_addressline3']/text()").extract_first()  # the website url
        if addressline3 and addressline3.find('@') == -1 and addressline3.find('.') > 0 and not addressline3.endswith('.'):
            website = addressline3
        else: website = ''
        country = response.xpath("//span[@class='contactinfo_country']/text()").extract_first()
        phone = response.xpath("//span[@class='contactinfo_phone1']//td/div[@class='ACA_PhoneNumberLTR']/text()").extract_first()
        phone2 = response.xpath("//span[@class='contactinfo_phone2']//td/div[@class='ACA_PhoneNumberLTR']/text()").extract_first()
        email = response.xpath("//span[@class='contactinfo_email']//td//tr/td/text()").extract_first()
        #
        business_owners = []
        for owner in response.xpath("//tr[@id='trASITList']//tr[td//span]"):
            value_list = owner.xpath(".//span/text()").extract()
            #print(value_list)
            #if len(value_list) != 6: raise ValueError("parse_detail: ERROR...enexpted value_list={} for {}".format(value_list, response.url))
            if len(value_list) == 6:
                title = value_list[1]
                firstname = value_list[3]
                lastname = value_list[5]
            elif len(value_list) == 4:
                title = ''
                firstname = value_list[1]
                lastname = value_list[3]
            else:
                raise ValueError("parse_detail: ERROR...enexpted value_list={} for {}".format(value_list, response.url))
            #
            business_owners.append({'title':title, 'firstname':firstname, 'lastname':lastname})
        #print(business_owners)
        #
        address1,address2,city,state,zipcode = self._parse_address_line(toc_data['business_address'])
        owner1_title,owner1_fname,owner1_lname,owner2_title,owner2_fname,owner2_lname,owner3_title,owner3_fname,owner3_lname = self._parse_business_owners(business_owners)
        #
        if trade_name == toc_data['business_name']: trade_name = ''
        row = [toc_data['record_number'],toc_data['business_name'],trade_name,toc_data['start_date'],toc_data['record_type'],toc_data['expires_on'],toc_data['status'],
               address1, address2,city,state,zipcode,country,phone,phone2,email,website,owner1_title,owner1_fname,owner1_lname,owner2_title,owner2_fname,owner2_lname,owner3_title,owner3_fname,owner3_lname,
               response.url, date_collected]
        self.csvwriter.writerow(row)
        self.totalCount += 1
    #
    # 
    def _parse_address_line(self, addr_line):
        # 66292 PIERSON BLVD W, DESERT HOT SPRINGS CA 92240
        # 4345 Sonoma BLVD, SUITE D-4, VALLEJO CA 94589
        # 18448 Oxnard ST, TARZANA CA 91356 United States
        if addr_line.endswith('United States'): addr_line = addr_line[:-len('United States')].strip()
        #
        idx = addr_line.rfind(' ')
        zipcode = addr_line[idx+1:]
        if zipcode and len(zipcode) == 9: zipcode = "{}-{}".format(zipcode[:5], zipcode[5:])
        #
        state = addr_line[idx-2:idx]
        id1 = addr_line.find(',')
        address1 = addr_line[:id1]
        id2 = addr_line.find(',', id1+1)
        if id2 > id1+1: 
            address2 = addr_line[id1+1:id2].strip()
        else: 
            address2 = ''
            id2 = id1
        city = addr_line[id2+1:idx-2].strip()
        #
        return address1,address2,city,state,zipcode
    #
    def _parse_business_owners(self, business_owners):
        owner1_title=owner1_fname=owner1_lname=owner2_title=owner2_fname=owner2_lname=owner3_title=owner3_fname=owner3_lname = ''
        idx = 0
        for owner in business_owners:
            if idx == 0:
                owner1_title = owner['title']
                owner1_fname = owner['firstname']
                owner1_lname = owner['lastname']
            elif idx == 1:
                owner2_title = owner['title']
                owner2_fname = owner['firstname']
                owner2_lname = owner['lastname']
            elif idx == 2:
                owner3_title = owner['title']
                owner3_fname = owner['firstname']
                owner3_lname = owner['lastname']
            else:
                break
            idx += 1
        #                
        return owner1_title,owner1_fname,owner1_lname,owner2_title,owner2_fname,owner2_lname,owner3_title,owner3_fname,owner3_lname
    #
    #
    def _get_post_form_data(self, permit_type, eventTarget, scriptManager, viewstate, viewstateGENERATOR, ACA_CS_FIELD):
        #
        return {
            'ctl00$ScriptManager1':scriptManager,
            'ACA_CS_FIELD':ACA_CS_FIELD,
            '__EVENTTARGET':eventTarget,
            '__EVENTARGUMENT':'',
            '__LASTFOCUS':'',
            '__VIEWSTATE':viewstate,
            '__VIEWSTATEGENERATOR':viewstateGENERATOR,
            'txtSearchCondition':'Search...',
            'ctl00$HeaderNavigation$hdnShoppingCartItemNumber':'',
            'ctl00$HeaderNavigation$hdnShowReportLink':'N',
            'ctl00$PlaceHolderMain$addForMyPermits$collection':'rdoNewCollection',
            'ctl00$PlaceHolderMain$addForMyPermits$txtName':'name',
            'ctl00$PlaceHolderMain$addForMyPermits$txtDesc':'',
            'ctl00$PlaceHolderMain$generalSearchForm$txtGSPermitNumber':'',
            'ctl00$PlaceHolderMain$generalSearchForm$ddlGSPermitType':permit_type, #'Licenses/Adult Use Cannabis/Distributor/Temporary License',
            'ctl00$PlaceHolderMain$generalSearchForm$ddlGSCapStatus':'',
            'ctl00$PlaceHolderMain$generalSearchForm$txtGSStartDate':self.twoyearago,   # '03/05/2016',
            'ctl00$PlaceHolderMain$generalSearchForm$txtGSStartDate_ext_ClientState':'',
            'ctl00$PlaceHolderMain$generalSearchForm$txtGSEndDate':self.today_date,     # '03/05/2018',
            'ctl00$PlaceHolderMain$generalSearchForm$txtGSEndDate_ext_ClientState':'',
            'ctl00$PlaceHolderMain$generalSearchForm$txtGSNumber$ChildControl0':'',
            'ctl00$PlaceHolderMain$generalSearchForm$txtGSNumber$ctl00_PlaceHolderMain_generalSearchForm_txtGSNumber_ChildControl0_watermark_exd_ClientState':'',
            'ctl00$PlaceHolderMain$generalSearchForm$txtGSNumber$ChildControl1':'',
            'ctl00$PlaceHolderMain$generalSearchForm$txtGSNumber$ctl00_PlaceHolderMain_generalSearchForm_txtGSNumber_ChildControl1_watermark_exd_ClientState':'',
            'ctl00$PlaceHolderMain$generalSearchForm$ddlGSDirection':'',
            'ctl00$PlaceHolderMain$generalSearchForm$txtGSStreetName':'',
            'ctl00$PlaceHolderMain$generalSearchForm$ddlGSStreetSuffix':'',
            'tl00$PlaceHolderMain$generalSearchForm$txtGSCity':'',
            'ctl00$PlaceHolderMain$generalSearchForm$ddlGSState$State1':'',
            'ctl00$PlaceHolderMain$generalSearchForm$txtGSAppZipSearchPermit':'',
            'ctl00$PlaceHolderMain$generalSearchForm$txtGSAppZipSearchPermit_ZipFromAA':'0',
            'ctl00$PlaceHolderMain$generalSearchForm$txtGSAppZipSearchPermit_zipMask':'',
            'ctl00$PlaceHolderMain$generalSearchForm$txtGSAppZipSearchPermit_ext_ClientState':'',
            'ctl00$PlaceHolderMain$generalSearchForm$txtGSCounty':'',
            'ctl00$PlaceHolderMain$hfASIExpanded':'',
            'ctl00$PlaceHolderMain$txtHiddenDate':'',
            'ctl00$PlaceHolderMain$txtHiddenDate_ext_ClientState':'',
            'ctl00$PlaceHolderMain$dgvPermitList$lblNeedReBind':'',
            'ctl00$PlaceHolderMain$dgvPermitList$gdvPermitList$hfSaveSelectedItems':'',
            'ctl00$PlaceHolderMain$dgvPermitList$inpHideResumeConf':'',
            'ctl00$PlaceHolderMain$hfGridId':'',
            'ctl00$HDExpressionParam':'',
            '__AjaxControlToolkitCalendarCssLoaded':'',
            '__ASYNCPOST':'true'
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