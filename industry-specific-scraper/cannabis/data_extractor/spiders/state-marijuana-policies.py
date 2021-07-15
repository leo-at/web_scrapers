import json,csv
import os,sys,re
import scrapy
import logging
import requests
from datetime import datetime, timedelta
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher

#
class PotPolicySpider(scrapy.Spider):    
    name = "potpolicy"
    #
    def __init__(self, *args, **kwargs):
        logger = logging.getLogger('scrapy.spidermiddlewares.httperror')
        logger.setLevel(logging.ERROR)  # set logging level here
        super().__init__(*args, **kwargs)
        #
        dispatcher.connect(self.spider_closed, signals.spider_closed)               
        #
        p_item_data = r"<p>\s*([^:]+)\s*:\s*([^<]+)</p>"
        self.re_item_data = re.compile(p_item_data)
        #
        csv_encoding = 'utf-8'
        csvFilename  = 'state-marijuana-policies.csv'
        self.csv_file = open(csvFilename, 'w', newline='', encoding=csv_encoding) #, encoding='windows-1250', errors ='ignore')
        self.csvwriter = csv.writer(self.csv_file)
        if csv_encoding == 'utf-8': self.csv_file.write(u'\ufeff') #.write(codecs.BOM_UTF8)
        #
        column_headers = ['state','last_updated','Allows Medical marijuana','Allows Adult-Use marijuana','2016 Medical Sales','2021 Projected Medical Sales','2016 Adult Use Sales','2021 Projected Adult Use Sales',
                        'Noteworthy Information', 'Is there a Regulatory Structure (State Agency)','# of Dispensaries Allowed (# issued)','# of Cultivations Allowed (# issued)',
                        '# of Manufacturers Allowed (# issued)','# of Testing labs Allowed (# issued)','Geographic Distribution of Licenses','Application Fee','Licensing Fees','Residency Requirements',
                        'Vertical Integration Allowed, Required or Prohibited','Medical Marijuana Qualifying Patient Conditions','Testing Required','sourceUrl','dateCollected']
        self.csvwriter.writerow(column_headers)
        self.totalCount = 0        
        #
        self.policy_map_url = "https://thecannabisindustry.org/state-marijuana-policies-map/"
    #
    def start_requests(self): 
        yield scrapy.Request("https://thecannabisindustry.org/", self.parse)
    #
    def parse(self, response):
        yield scrapy.Request(self.policy_map_url, self.parse_policy)
    #
    def parse_policy(self, response):
        #
        json_api = "https://spreadsheets.google.com/feeds/list/1lYGkcaJi7J0omrFUL0lhVFFea8Hu6QW77WCxjTwh1ck/default/public/values?alt=json"
        #
        date_collected = datetime.now().strftime('%Y-%m-%d %H:%M:%S')         
        resource = requests.get(json_api, headers={'x-client-data':'CI62yQEIprbJAQjBtskBCKmdygEIqKPKAQjdo8oB','accept':'application/json, text/javascript, */*; q=0.01'})
        json_data = json.loads(resource.text)
        #
        sourceUrl = self.policy_map_url
        feed_data = json_data['feed']
        for rec in feed_data['entry']:
            last_updated = rec['updated']['$t']
            state_name = rec['gsx$state']['$t']
            state_abbr = rec['gsx$stateid']['$t'][3:] # "US-AL"
            allowsmedicalmarijuana = rec['gsx$allowsmedicalmarijuana']['$t']
            allowsadultusemarijuana = rec['gsx$allowsadult-usemarijuana']['$t']
            noteworthyinformation = rec['gsx$noteworthyinformation']['$t']
            istherearegulatorystructurestateagency = rec['gsx$istherearegulatorystructurestateagency']['$t']
            ofdispensariesallowedissued = rec['gsx$ofdispensariesallowedissued']['$t']
            ofcultivationsallowedissued = rec['gsx$ofcultivationsallowedissued']['$t']
            ofmanufacturersallowedissued = rec['gsx$ofmanufacturersallowedissued']['$t']
            oftestinglabsallowedissued = rec['gsx$oftestinglabsallowedissued']['$t']
            geographicdistributionoflicenses = rec['gsx$geographicdistributionoflicenses']['$t']
            applicationfees = rec['gsx$applicationfees']['$t']
            licensingfees = rec['gsx$licensingfees']['$t']
            residencyrequirements = rec['gsx$residencyrequirements']['$t']
            verticalintegrationallowedrequiredorprohibited = rec['gsx$verticalintegrationallowedrequiredorprohibited']['$t']
            medicalmarijuanaqualifyingpatientconditions = rec['gsx$medicalmarijuanaqualifyingpatientconditions']['$t']
            testingrequired = rec['gsx$testingrequired']['$t']
            medicalsales = rec['gsx$medicalsales']['$t']
            projectedmedicalsales = rec['gsx$projectedmedicalsales']['$t']
            adultusesales = rec['gsx$adultusesales']['$t']
            projectedadultusesales = rec['gsx$projectedadultusesales']['$t']
            #
            state_abbr =  state_abbr.strip()
            row = [state_abbr,last_updated,allowsmedicalmarijuana,allowsadultusemarijuana,medicalsales,projectedmedicalsales,adultusesales,projectedadultusesales,noteworthyinformation,
                   istherearegulatorystructurestateagency,ofdispensariesallowedissued,ofcultivationsallowedissued,ofmanufacturersallowedissued,oftestinglabsallowedissued,
                   geographicdistributionoflicenses,applicationfees,licensingfees,residencyrequirements,verticalintegrationallowedrequiredorprohibited,medicalmarijuanaqualifyingpatientconditions,
                   testingrequired, sourceUrl, date_collected]
            self.csvwriter.writerow(row)
            self.totalCount += 1
    #
    def parse_policy_V1(self, response):
        last_updated = response.xpath("//p[@class='updated']/strong/text()").extract_first()
        #
        state_policies = {}
        for label in response.xpath("//div[@id='mapDiv']/div/div[1]/*[1]//*[@transform]/*[@transform]/*/@aria-label").extract():
            # print("{}".format(label.encode('utf-8')))
            idx = label.find(' 0')
            if idx > 0: state = label[:idx]
            else: raise ValueError("parse_policy: ERROR...not found state in {}".format(label.encode('utf-8')))
            #
            for m in self.re_item_data.findall(label):
                self.log( json.dumps( { 'state':state, 'item_name': m[0].strip(), 'item_value': m[1], 'last_updated':last_updated } ) )

    #
    # a function that will be called if any exception was raised while processing the request. This includes
    # pages that failed with 404 HTTP errors and such. It receives a Twisted Failure instance as first parameter.
    def error_handler(self, failure):
        # log all failures
        print("error_handler: {}".format(repr(failure)))

        if failure.check(HttpError):
            # these exceptions come from HttpError spider middleware: the non-200 response
            request = failure.request # this is the original request
            self.logger.error('HttpError %s on %s from %s', failure.value.response.status, request.url, request.meta['start_url'])

        elif failure.check(DNSLookupError):            
            request = failure.request
            self.logger.error('DNSLookupError on %s', request.url)

        elif failure.check(TimeoutError, TCPTimedOutError):
            request = failure.request
            self.logger.error('TimeoutError on %s', request.url)

        else:
            request = failure.request
            self.logger.error(repr(failure))
        #
    def middleware_exception(self, request, middleware_class, exception_url):
        print("middleware_exception -- {}: {}: {}".format(request.url, middleware_class, exception_url)) 
    #
    #
    def spider_closed(self, spider, reason): 
        #
        print(self.totalCount)
        self.csv_file.close()
        #