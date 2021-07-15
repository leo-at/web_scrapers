import time
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from scrapy.http import HtmlResponse

#
class SeleniumDownloadMiddleware(object):
    def __init__(self):
        self.chrome_options = Options()
        self.chrome_options.add_argument("--headless")
        self.chrome_options.add_argument("--no-sandbox")
        self.chrome_options.add_argument("--disable-notifications")
        self.chrome_options.add_argument("--disable-default-apps")
        # self.chrome_options.add_argument("--log-level=0")  # INFO = 0, WARNING = 1, LOG_ERROR = 2, LOG_FATAL = 3.
        self.chrome_options.add_argument("--log-level=3")  # fatal
        self.chrome_options.add_experimental_option('prefs', {'profile.managed_default_content_settings.images': 2})
        self.driver = None   
        self._plain_html_pages = {}     
    #
    def _initialzation(self):
        if platform.system() == 'Windows':
            chromedriver_path = 'C:/dnb/dev/bin/chromedriver.exe'
        else:
            chromedriver_path = '/var/projects/chromedriver'
        #
        if self.driver == None:
            self.driver = webdriver.Chrome(chromedriver_path, chrome_options=self.chrome_options)
            #self.driver.implicitly_wait(4) # seconds
    #
    #
    def process_response(self, request, response, spider):
        if response.status != 200: return response
        request_url = request.url
        if not request_url.startswith('http') or request_url.endswith('.txt'): return response
        # print(response.text.encode('utf-8'))
        useWebdriver = False
        if request_url.find("/thecannabisindustry.org/state-marijuana-policies-map/") >= 0: useWebdriver = True
        if request_url.find("/egov.sos.state.or.us/") > 0: useWebdriver = True
        #if request_url.find("/aca5.accela.com/bcc/") >= 0: useWebdriver = True        
        #
        if not useWebdriver: 
            return response
        else:
            self._initialzation()
            self.driver.get(response.url)
            #
            if request_url.find("/egov.sos.state.or.us/br/pkg_web_name_srch_inq.show_detl") >= 0:
                try:
                    WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.NAME, 'SHOW_DETL')))
                except Exception as e:
                    pass
                    #print("ERROR.....Not finding elment with name=SHOW_DETL for {}".format(request_url))
            else:
                time.sleep(3.0)
            #
            resp_html = self.driver.find_element_by_tag_name('html').get_attribute('outerHTML')
            return HtmlResponse(url=self.driver.current_url, body=resp_html, encoding=response.encoding, request=request)

    #
    def process_exception(self, request, exception, spider):
        ex_class = "%s.%s" % (exception.__class__.__module__, exception.__class__.__name__)
        #
        spider.middleware_exception(request, self.__class__.__name__, request.url)        