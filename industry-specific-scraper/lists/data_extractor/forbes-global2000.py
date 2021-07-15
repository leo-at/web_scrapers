import platform,os
import csv,json,time
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from scrapy.http import Request,HtmlResponse

#
def main(csvFilename):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-notifications")
    chrome_options.add_argument("--disable-default-apps")
    # chrome_options.add_argument("--log-level=0")  # INFO = 0, WARNING = 1, LOG_ERROR = 2, LOG_FATAL = 3.
    chrome_options.add_argument("--log-level=3")  # fatal
    chrome_options.add_experimental_option('prefs', {'profile.managed_default_content_settings.images': 2})
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36")
    if platform.system() == 'Windows':
        chromedriver_path = 'C:/dnb/dev/bin/chromedriver.exe'
    else:
        chromedriver_path = '/var/projects/chromedriver'
    driver = webdriver.Chrome(chromedriver_path, chrome_options=chrome_options)
    #
    start_url = "https://www.forbes.com/global2000/"
    driver.get(start_url)
    try:
        skipbutton = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//div[@id='navigation']/div/div[@class='continue-button']")))
        skipbutton.click()
    except Exception as e:
        print("ERROR!!! {}: Not finding element with XPATH=//*[@id='navigation']/div/a".format(start_url))
        return
    #
    csv_encoding = 'utf-8'
    csv_file = open(csvFilename, 'w', newline='', encoding=csv_encoding) #, encoding='windows-1250', errors ='ignore')
    csvwriter = csv.writer(csv_file)
    if csv_encoding == 'utf-8': csv_file.write(u'\ufeff') #.write(codecs.BOM_UTF8)
    #          
    column_headers = ['rank','position','shortName','companyName','website','country','state','headquarters','employees','industry','ceo','revenue','marketValue','assets','profits','profile','sourceUrl','dateCollected']
    csvwriter.writerow(column_headers)
    totalCount = 0    
    #
    # somehow this code does not work!!!
    # json_url = "https://www.forbes.com/ajax/list/data?year=2017&uri=global2000&type=organization"
    # driver.get(json_url)
    # time.sleep(3)
    # json_text = driver.page_source
    # json_data = json.loads(json_text)
    json_file_name = csvFilename.replace(".csv", ".json")
    dirname = os.path.dirname(os.path.realpath(__file__))
    json_file_path = os.path.join(dirname, json_file_name)
    json_data = []
    with open(json_file_path, encoding='utf-8') as f:
        json_data = json.load(f)    
    #
    for item in json_data:
        rank = item['rank']
        position = item['position']
        imageUri = item['imageUri']
        name = item['name']
        ceo = item['ceo'] if 'ceo' in item else ''
        industry = item['industry'] if 'industry' in item else ''
        country = item['country'] if 'country' in item else ''
        state = item['state'] if 'state' in item else ''
        headquarters = item['headquarters'] if 'headquarters' in item else ''
        revenue = item['revenue'] if 'revenue' in item else ''
        marketValue = item['marketValue'] if 'marketValue' in item else ''
        assets = item['assets'] if 'assets' in item else ''
        profits = item['profits'] if 'profits' in item else ''
        #if rank not in [1,92,166,842,1992]: continue
        # 
        #detail_url = 'https://www.forbes.com/companies/guangxi-guiguan-electric-power/'
        detail_url = "https://www.forbes.com/companies/{}/".format(item['uri']) # ""    
        date_collected = datetime.now().strftime('%Y-%m-%d %H:%M:%S') 
        print("{}: rank={}: {}".format(date_collected, rank, detail_url))       
        companyName = name
        website = employees = profile = ''
        #    
        try:    
            driver.get(detail_url)
            page_html = (driver.page_source).encode('utf-8')
            html_response = HtmlResponse(url=detail_url, body=page_html, encoding='utf-8', request=Request(detail_url))    
            profile = html_response.xpath("//div[@class='profile']/text()").extract_first()
            if profile: profile = profile.strip(" \t\r\n")
            #print(profile)        
            for data in html_response.xpath("//div[@class='main-info']/div/dl"):
                field_name = data.xpath('./dt/text()').extract_first()
                field_value = data.xpath("./dd//text()").extract_first()
                if field_name == 'Website': website = field_value
                elif field_name=='Employees': employees = field_value.replace(',','') if field_value else ''
                elif field_name=='Headquarters': headquarters = field_value
            if profile: companyName = _get_company_name(profile)
        except Exception as ex:
            companyName = "ERROR"
            print("ERROR: {}: {}".format(detail_url, ex))
        #
        row = [rank, position, name, companyName, website, country, state, headquarters, employees, industry, ceo, revenue, marketValue, assets, profits, profile, detail_url, date_collected]
        csvwriter.writerow(row)
        totalCount += 1
    #
    driver.quit()
    print("totalCount={}".format(totalCount))
    csv_file.close()
#
#
def _get_company_name(profile):
    nameWords = []
    word_list = profile.split(' ')
    for i in range(len(word_list)):
        word = word_list[i]
        if i==0 or word.endswith('.'):
            nameWords.append(word)
            break
        elif word[0].isupper() or word in ['of','for']:
            nameWords.append(word)
        else:
            break
    companyName = ' '.join(nameWords)
    return companyName.strip(' ,')

#
#
if __name__ == '__main__':
    csvFilename = 'forbes-global2000-2017.csv'
    main(csvFilename)