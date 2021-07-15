import json,csv
import os,sys
import scrapy
import logging
from datetime import datetime, timedelta
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
# appending sys.path to import from parent folder to avoid hard-coded the parent folder name.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from usGeo import USGeo

#
class AccreditationSpider(scrapy.Spider):    
    name = "accreditation"
    #
    def __init__(self, *args, **kwargs):
        logger = logging.getLogger('scrapy.spidermiddlewares.httperror')
        logger.setLevel(logging.ERROR)  # set logging level here
        super().__init__(*args, **kwargs)
        #
        dispatcher.connect(self.spider_closed, signals.spider_closed)               
        #
        self.input_state = None
        if kwargs and 'state' in kwargs: 
            inputState = kwargs['state']
            #print("inputState={}".format(inputState))
            if inputState in ['AL','AK','AZ','AR','CA','CO','CT','DE','DC','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA',
                              'ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR',
                              'PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY','AS','FM','GU','MH','MP','PR','PW','VI']:
                self.input_state = inputState
        #
        self.column_headers = ['inst_OPE_id','instName', 'instFormerName','instStatus', 'instUrl', 'CampusType', 'parent_OPE_id', 'parentInstName', 
                           'instAddr1', 'instAddr2','instCity','instState','instZip', 'instCountry','instPhone',
                           'programName','accrType', 'agencyName', 'agencyStatus','accrStatus',
                           'accrDate', 'accrDateInfo', 'currentAction','justification_for_action','nextReviewDate']        
        self.totalCount = 0
        self.state_pagers = None
        #
        self.usgeo = USGeo()
        self.parent_insts = {}
        self.inst_records = []

    #
    def start_requests(self): 
        if not self.input_state : 
            print("\n------------------------------------------------------------------------------")
            print("Please speficy a valid two-character US state (abbreviation), e.g. -a state=AK")
            print("------------------------------------------------------------------------------\n")
            return
        #
        yield scrapy.Request("https://www.ed.gov/accreditation", self.parse)
    #
    def parse(self, response):
        yield scrapy.Request("https://ope.ed.gov/accreditation/Search.aspx", self.parse_search)
    #
    def parse_search(self, response):
        post_action = response.xpath("//form[@method='post']/@action").extract_first()
        post_url = response.urljoin(post_action)
        viewstate = response.xpath("//form[@method='post']//input[@name='__VIEWSTATE']/@value").extract_first()
        viewstateGENERATOR = response.xpath("//form[@method='post']//input[@name='__VIEWSTATEGENERATOR']/@value").extract_first()
        eventValidation = response.xpath("//form[@method='post']//input[@name='__EVENTVALIDATION']/@value").extract_first()
        #
        for state in response.xpath("//form[@method='post']//select/option[@value!='-1']/@value").extract():
            # Only ALLOW one state per spider run!!!!!!!!!!!!
            if state != self.input_state: continue
            #
            form_data = {
                '__EVENTTARGET':'',
                '__EVENTARGUMENT':'',
                '__VIEWSTATE':viewstate,
                '__VIEWSTATEGENERATOR':viewstateGENERATOR,
                '__EVENTVALIDATION':eventValidation,
                'ctl00$cph$txtInstName':'',
                'ctl00$cph$txtAddress':'',
                'ctl00$cph$txtCity':'',
                'ctl00$cph$lstState':state,
                'ctl00$cph$txtHistAgency':'',
                'ctl00$cph$btnSearch':'Search'
            }
            #
            print("{}: {}".format(state, json.dumps(form_data)))
            yield scrapy.FormRequest(post_url, formdata=form_data, callback=self.parse_result, meta={ 'form_data':form_data, 'more_pager_index':0 })
    #
    #
    def parse_result(self, response):
        page_url = response.url
        form_data = response.meta['form_data']
        more_pager_index = response.meta['more_pager_index']
        if not self.state_pagers: self.state_pagers = ['ctl00$cph$pager$ctl00$LinkButton1']
        #
        for tr in response.xpath("//table//table//table//table//table//tr[contains(@class,'gv-row')]"):
            anchor = tr.xpath("./td[1]//a")
            if not anchor: raise ValueError("ERROR...Not finding anchor for {}".format(page_url))
            detail_link = anchor[0].xpath("./@href").extract_first()
            instName = anchor[0].xpath("./text()").extract_first()
            address1 = tr.xpath("./td[2]/text()").extract_first()
            city  = tr.xpath("./td[3]/text()").extract_first()
            state = tr.xpath("./td[4]/text()").extract_first()
            #
            #if instName not in ['Academy College','Anoka-Ramsey Community College']: continue
            #if not instName.startswith('Anthem College'): continue
            #if not instName.startswith('Anoka Technical'): continue
            #if not instName.startswith('Anoka-Ramsey'): continue
            #if not instName.startswith('Anoka'): continue
            #
            yield scrapy.Request(response.urljoin(detail_link), callback=self.parse_detail, meta={'instAddr':[instName, address1, city, state]})
        #
        #
        viewstate = response.xpath("//form[@method='post']//input[@name='__VIEWSTATE']/@value").extract_first()
        viewstateGENERATOR = response.xpath("//form[@method='post']//input[@name='__VIEWSTATEGENERATOR']/@value").extract_first()
        form_data = {
            '__EVENTTARGET':'',
            '__EVENTARGUMENT':'',
            '__VIEWSTATE':viewstate,
            '__VIEWSTATEGENERATOR':viewstateGENERATOR
        }
        post_action = response.xpath("//form[@method='post']/@action").extract_first()
        post_url = response.urljoin(post_action)
        #
        for anchor in response.xpath("//a[starts-with(@id,'ctl00_cph_pager_ctl')]"):
            anc_text = anchor.xpath("./text()").extract_first()
            ctl_href = anchor.xpath("./@href").extract_first() # javascript:__doPostBack('ctl00$cph$pager$ctl00$LinkButton1','')
            # Skip the first page since already clicked by the '...' on the previous pager.  The first page of initial pager is already handled above.
            if anc_text != '...' and int(anc_text) % 10 == 1: continue 
            # 
            idx1 = ctl_href.find("'")
            idx2 = ctl_href.find("'", idx1+1)
            if idx1 == -1:
                idx1 = ctl_href.find('"')
                idx2 = ctl_href.find('"', idx1+1)
            #
            ctl_tgt = ctl_href[idx1+1:idx2]   
            if anc_text == '...' and ctl_tgt == 'ctl00$cph$pager$ctl00$LinkButton1': continue # don't going back to the previous pager
            #
            pager_key = "{}:{}".format(more_pager_index, ctl_tgt) if more_pager_index > 0 else ctl_tgt
            if pager_key in self.state_pagers: continue
            self.state_pagers.append(pager_key)
            #       
            form_data['__EVENTTARGET'] = ctl_tgt
            # ctl_tgt == "ctl00$cph$pager$ctl10$LinkButton1" or ctl_tgt == "ctl00$cph$pager$ctl11$LinkButton1"
            if anc_text == '...': more_pager_index += 1
            #
            yield scrapy.FormRequest(post_url, formdata=form_data, callback=self.parse_result, meta={ 'form_data':form_data, 'more_pager_index':more_pager_index })
    #
    #
    def parse_parent_OPE_id(self, response):
        instAccrDetailUrl = response.meta['instAccrDetailUrl']
        if instAccrDetailUrl in self.parent_insts: return
        #
        instTable = response.xpath("//div[@id='ctl00_cph_divSurveyData']/div[1]/table[1]")[0]
        instName  = instTable.xpath("./tr/td[string-length(text())>0]/text()").extract_first()
        #
        instIdTable = instTable.xpath("./following-sibling::div[1]//table[@class='font-inst-id']")
        instUrlTdNode  = instIdTable.xpath("./following-sibling::table[1]//td[a]")
        instUrl = instUrlTdNode.xpath("./a[text()='Go to Main Campus']/@href").extract_first()
        #
        OPE_ID_Text = instUrlTdNode.xpath("./following-sibling::td[1]/text()").extract_first()
        if OPE_ID_Text and OPE_ID_Text.startswith("OPE ID:"):
            inst_OPE_id = OPE_ID_Text[7:].strip()
        else:
            inst_OPE_id = ''
        #
        self.parent_insts[instAccrDetailUrl] = {'inst_OPE_id':inst_OPE_id, 'instName':instName,'instUrl':instUrl}
    #
    #
    def parse_detail(self, response):
        page_url = response.url
        #
        instTable = response.xpath("//div[@id='ctl00_cph_divSurveyData']/div[1]/table[1]")[0]
        instName  = instTable.xpath("./tr/td[string-length(text())>0]/text()").extract_first()
        instIdTable = instTable.xpath("./following-sibling::div[1]//table[@class='font-inst-id']")
        addr_lines = [ x.strip(' \t\r\n').replace(u'\xa0', u' ') for x in instIdTable.xpath("./tr[1]/td[1]/text()").extract() if x.strip(' \t\r\n') ]
        if addr_lines[-1].startswith("Phone:"): 
            instPhone = addr_lines[-1][6:].strip()
            addr_lines = addr_lines[:-1]  # remove the last line
        else:
            instPhone = ''
        instAddress1 = addr_lines[0]
        instAddress2 = addr_lines[1] if len(addr_lines) >=3 else ''        
        instCity,instState,instZip = self.usgeo.ParseCityStateZip(addr_lines[-1])
        #print("{}: {} [{}, {} {}]".format(instName, addr_lines[-1], instCity,instState,instZip))
        institutionStatus = instIdTable.xpath("../div/text()").extract_first()
        if institutionStatus: institutionStatus = institutionStatus.strip(' *\r\n\t')
        formerNameText = instTable.xpath("./following-sibling::div[1]//tr[@class='font-inst-id']/td/text()").extract_first()
        instFormerName = formerNameText[17:].strip() if formerNameText and formerNameText.startswith('Previous name(s):') else ''
        #
        instUrlTdNode  = instIdTable.xpath("./following-sibling::table[1]//td[a]")
        OPE_ID_Text = instUrlTdNode.xpath("./following-sibling::td[1]/text()").extract_first()
        inst_OPE_id = OPE_ID_Text[7:].strip() if OPE_ID_Text and OPE_ID_Text.startswith("OPE ID:") else ''
        #
        instUrl = instUrlTdNode.xpath("./a[@target='_blank']/@href").extract_first()
        goToMainCampusUrl  = instUrlTdNode.xpath("./a[contains(@href,'InstAccrDetails.aspx')]/@href").extract_first()
        #goToMainCampusText = instUrlTdNode.xpath("./a[contains(@href,'InstAccrDetails.aspx')]/text()").extract_first()
        if goToMainCampusUrl: # and goToMainCampusText == "Go to Main Campus":
            instAccrDetailUrl = response.urljoin( goToMainCampusUrl )
            isMainCampus = False
            if instAccrDetailUrl not in self.parent_insts:
                yield scrapy.Request(instAccrDetailUrl, callback=self.parse_parent_OPE_id, meta={'instAccrDetailUrl':instAccrDetailUrl})
        else:
            instAccrDetailUrl = response.url
            isMainCampus = True
        #
        count = 0
        accrTable = response.xpath("//div[@id='ctl00_cph_divInstAccr']/table[1]")[0]
        for accrNode in accrTable.xpath("./tr"):
            aid = accrNode.xpath("./@id").extract_first()
            #print("id={} for {}".format(aid, page_url))
            if aid.startswith('ctl00_cph_pnlInstAccr'):
                accrType = "Institutional"
                for rowInst in accrNode.xpath("./td/table//tr[@class='gv-row-inst' or @class='gv-row-alt-inst']"):
                    rowInst_tds = rowInst.xpath("./td")
                    if len(rowInst_tds) == 0: continue
                    elif len(rowInst_tds) != 6: raise ValueError("ERROR..len(rowInst_tds)={} for {}".format(len(rowInst_tds), page_url))
                    #
                    agencyAnchor = rowInst_tds[0].xpath("./a")
                    agencyName = agencyAnchor.xpath("./text()").extract_first().strip(' -\r\n')
                    agenyClick = agencyAnchor.xpath("./@onclick").extract_first().strip()
                    idx1 = agenyClick.find("'")
                    idx2 = agenyClick.find("'", idx1+1)
                    ageny_id = agenyClick[idx1+1:idx2]
                    ageny_url = "https://ope.ed.gov/accreditation/ViewAgencyInfo.aspx?{}".format(ageny_id)
                    status_text = agencyAnchor.xpath("./following-sibling::span[1]/text()").extract_first()
                    agencyStatus = status_text.strip(' ()') if status_text else ''
                    #
                    accreditedStatus = rowInst_tds[1].xpath("./text()").extract_first().strip(' -\r\n')
                    accreditedDate = rowInst_tds[2].xpath("./text()").extract_first().strip(' -\r\n')
                    if accreditedDate.startswith('*'): 
                        accreditedDate = accreditedDate[1:]
                        accrDateInfo = 'estimated date'
                    else:
                        accrDateInfo = ''
                    currentAction = ' '.join( [x.strip(' -\r\n') for x in rowInst_tds[3].xpath("./text()").extract() if x.strip(' -\r\n')] )
                    justification_for_action = ' '.join( [x.strip(' -\r\n') for x in rowInst_tds[4].xpath("./text()").extract() if x.strip(' -\r\n')] )
                    nextReviewDate = rowInst_tds[5].xpath("./text()").extract_first().strip(' -\r\n')
                    #
                    programName = ''
                    inst_rec = {'inst_OPE_id':inst_OPE_id, 'instName':instName,'instFormerName':instFormerName, 'institutionStatus':institutionStatus,
                        'instUrl':instUrl, 'isMainCampus':isMainCampus,       
                        'instAddr1':instAddress1, 'instAddr2':instAddress2,'instCity':instCity,'instState':instState,'instZip':instZip, 'instPhone':instPhone,
                        'instAccrDetailUrl':instAccrDetailUrl, 'programName':programName,
                        'accrType':accrType, 'agencyName':agencyName, 'ageny_url':ageny_url, 'agencyStatus':agencyStatus,'accrStatus':accreditedStatus,
                        'accrDate':accreditedDate, 'accrDateInfo':accrDateInfo, 'currentAction':currentAction,'justification_for_action':justification_for_action,'nextReviewDate':nextReviewDate}
                    self.inst_records.append(inst_rec)
                    self.totalCount += 1          
                    count += 1
                    if self.totalCount % 100 == 0: print("{} records found: current instName={}".format(self.totalCount, instName.encode('utf-8')))
            else:
                if aid.startswith('ctl00_cph_pnlSpecialAccr'):
                    accrType = "Specialized"
                elif aid.startswith('ctl00_cph_pnlInternship'):
                    accrType = "Internship/Residency"
                else:
                    raise ValueError("ERROR...unexpected id={} for {}".format(aid, page_url))
                #
                savedAccrAgency = None
                for rowInst in accrNode.xpath("./td/table//tr[@class='gv-row']"):
                    agencyAnchor = rowInst.xpath("./td//a[not(img)]")
                    if agencyAnchor:
                        agencyName = agencyAnchor.xpath("./text()").extract_first().strip(' -\r\n')
                        agenyClick = agencyAnchor.xpath("./@onclick").extract_first().strip()
                        idx1 = agenyClick.find("'")
                        idx2 = agenyClick.find("'", idx1+1)
                        ageny_id = agenyClick[idx1+1:idx2]
                        ageny_url = "https://ope.ed.gov/accreditation/ViewAgencyInfo.aspx?{}".format(ageny_id)
                        status_text = agencyAnchor.xpath("./following-sibling::span[1]/text()").extract_first()
                        agencyStatus = status_text.strip(' ()') if status_text else ''
                        savedAccrAgency = {'agencyName':agencyName,'ageny_id':ageny_id,'ageny_url':ageny_url,'agencyStatus':agencyStatus}
                    elif savedAccrAgency: # same accrAgency as the previous one
                        agencyName = savedAccrAgency['agencyName']
                        ageny_id = savedAccrAgency['ageny_id']
                        ageny_url = savedAccrAgency['ageny_url']
                        agencyStatus = savedAccrAgency['agencyStatus']
                    else:                       
                        #print("accrType={} for {}".format(accrType, instName))
                        agencyName = ageny_id = ageny_url = agencyStatus = ''
                    #
                    accr_info_tds = rowInst.xpath("./td/table/tr/td")
                    if len(accr_info_tds) == 0: continue
                    elif len(accr_info_tds) != 6: raise ValueError("ERROR..len(accr_info_tds)={} for {}".format(len(accr_info_tds), page_url))
                    #
                    programName = accr_info_tds[0].xpath("./text()").extract_first()
                    accreditedStatus = accr_info_tds[1].xpath("./text()").extract_first().strip(' -\r\n')
                    accreditedDate = accr_info_tds[2].xpath("./text()").extract_first().strip(' -\r\n')
                    if accreditedDate.startswith('*'): 
                        accreditedDate = accreditedDate[1:]
                        accrDateInfo = 'estimated date'
                    else:
                        accrDateInfo = ''
                    currentAction = ' '.join( [x.strip(' -\r\n') for x in accr_info_tds[3].xpath("./text()").extract() if x.strip(' -\r\n')] )
                    justification_for_action = ' '.join( [x.strip(' -\r\n') for x in accr_info_tds[4].xpath("./text()").extract() if x.strip(' -\r\n')] )
                    nextReviewDate = accr_info_tds[5].xpath("./text()").extract_first().strip(' -\r\n')
                    #                 
                    inst_rec = {'inst_OPE_id':inst_OPE_id,'instName':instName, 'instFormerName':instFormerName, 'institutionStatus':institutionStatus,
                           'instUrl':instUrl, 'isMainCampus':isMainCampus,
                           'instAddr1':instAddress1, 'instAddr2':instAddress2,'instCity':instCity,'instState':instState,'instZip':instZip,'instPhone':instPhone,
                           'instAccrDetailUrl':instAccrDetailUrl, 'programName':programName,
                           'accrType':accrType, 'agencyName':agencyName, 'ageny_url':ageny_url, 'agencyStatus':agencyStatus,'accrStatus':accreditedStatus,
                           'accrDate':accreditedDate, 'accrDateInfo':accrDateInfo, 'currentAction':currentAction,'justification_for_action':justification_for_action,'nextReviewDate':nextReviewDate}
                    self.inst_records.append(inst_rec)
                    self.totalCount += 1
                    count += 1
                    if self.totalCount % 100 == 0: print("{} records found: current instName={}".format(self.totalCount, instName.encode('utf-8')))
        #
        #print("Record#={} for {}".format(count, page_url))
    #
    #
    def middleware_exception(self, request, middleware_class, exception_url):
        print("{}: {}: {}".format(request.url, middleware_class, exception_url)) 
    #
    #
    def spider_closed(self, spider, reason): 
        #print(json.dumps(self.parent_insts))
        print(self.state_pagers)
        #        
        csv_encoding = 'utf-8' 
        csvFilename  = "edu_accreditation_{}.csv".format(self.input_state)
        #print("csvFilename={}".format(csvFilename))
        csv_file = open(csvFilename, 'w', newline='', encoding=csv_encoding) #, encoding='windows-1250', errors ='ignore')
        csvwriter = csv.writer(csv_file)
        if csv_encoding == 'utf-8': csv_file.write(u'\ufeff') #.write(codecs.BOM_UTF8)
        csvwriter.writerow(self.column_headers)       
        #        
        for rec in self.inst_records:
            inst_OPE_id = rec['inst_OPE_id']
            isMainCampus = rec['isMainCampus']
            instAccrDetailUrl = rec['instAccrDetailUrl']
            #
            campusType = 'Main Campus' if isMainCampus else ''
            if instAccrDetailUrl in self.parent_insts:
                parent_inst = self.parent_insts[instAccrDetailUrl]
                parent_OPE_id = parent_inst['inst_OPE_id']
                parentInstName = parent_inst['instName']
                parentInstUrl = parent_inst['instUrl']                
            else:
                parent_OPE_id = inst_OPE_id
                parentInstName = rec['instName']
                parentInstUrl = rec['instUrl']
            #
            row = [ inst_OPE_id,rec['instName'], rec['instFormerName'], rec['institutionStatus'], rec['instUrl'], campusType, parent_OPE_id, parentInstName, 
                    rec['instAddr1'],rec['instAddr2'],rec['instCity'],rec['instState'],rec['instZip'], 'USA', rec['instPhone'],
                    rec['programName'],rec['accrType'],rec['agencyName'],rec['agencyStatus'],rec['accrStatus'],
                    rec['accrDate'],rec['accrDateInfo'],rec['currentAction'],rec['justification_for_action'],rec['nextReviewDate'] ]            
            csvwriter.writerow(row)
            #
        #
        print(self.totalCount)
        csv_file.close()
