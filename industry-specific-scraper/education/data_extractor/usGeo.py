import os
import json

#
class USGeo():
    # 
    def __init__(self):
        json_file_name = "usCities.json"
        dirname = os.path.dirname(os.path.realpath(__file__))
        json_file_path = os.path.join(dirname, json_file_name)
        with open(json_file_path) as f:
            self.usCities = json.load(f)
        #
        geo_100_file = "us_geo_100.txt"
        dirname = os.path.dirname(os.path.realpath(__file__))
        geo_100_file_path = os.path.join(dirname, geo_100_file)
        # state	abbrev	latitude	longitude
        self.geo_100_list = []
        with open(geo_100_file_path, 'rt') as f:
            for line in f:
                fields = line.split('\t')
                if fields[0] == 'zipcode': continue
                elif len(fields) < 4: continue     
                zipcode = fields[0]
                state = fields[1]
                latitude = fields[2]
                longitude = fields[3].strip(' \r\n')
                #
                self.geo_100_list.append([latitude,longitude,state,zipcode])
        #
        geo_50_file = "us_geo_50.txt"
        dirname = os.path.dirname(os.path.realpath(__file__))
        geo_50_file_path = os.path.join(dirname, geo_50_file)
        # ZipCode	primary_city	state	Latitude	Longitude
        self.geo_50_list = []
        with open(geo_50_file_path, 'rt') as f:
            for line in f:
                fields = line.split('\t')
                if fields[0] == 'ZipCode': continue
                elif len(fields) < 5: continue     
                zipcode = fields[0]
                city = fields[1]
                state = fields[2]
                latitude = fields[3]
                longitude = fields[4].strip(' \r\n')
                #
                self.geo_50_list.append([latitude,longitude,state,zipcode])
        #
        self._StateAbbrevList = ['VI', 'IN', 'TN', 'CA', 'KY', 'IA', 'AP', 'AS', 'DC', 'GU', 'WV', 'NE', 'HI', 'SC', 'AA', 'CO', 'MP', 'ND', 'NV', 'UT', 
                                'MA', 'NY', 'PA', 'MT', 'AE', 'MN', 'LA', 'AZ', 'DE', 'AR', 'TX', 'FL', 'CT', 'KS', 'NH', 'NJ', 'SD', 'IL', 'GA', 'PR', 
                                'WY', 'AL', 'ME', 'VT', 'NM', 'UM', 'ID', 'MO', 'WA', 'RI', 'OK', 'VA', 'OR', 'MS', 'MD', 'AK', 'WI', 'NC', 'OH', 'MI']
        self._StateList = ['ALABAMA','ALASKA','AMERICAN SAMOA','ARIZONA','ARKANSAS','ARMED FORCES AFRICA, CANADA, EUROPE AND THE MIDDLE EAST',
                    'ARMED FORCES AMERICAS (EXCEPT CANADA)','ARMED FORCES PACIFIC','CALIFORNIA','COLORADO','CONNECTICUT','DELAWARE','DISTRICT OF COLUMBIA',
                    'FLORIDA','GEORGIA','GUAM','HAWAII','IDAHO','ILLINOIS','INDIANA','IOWA','KANSAS','KENTUCKY','LOUISIANA','MAINE','MARYLAND','MASSACHUSETTS',
                    'MICHIGAN','MINNESOTA','MISSISSIPPI','MISSOURI','MONTANA','NEBRASKA','NEVADA','NEW HAMPSHIRE','NEW JERSEY','NEW MEXICO',
                    'NEW YORK','NORTH CAROLINA','NORTH DAKOTA','NORTHERN MARIANA ISLANDS','OHIO','OKLAHOMA','OREGON','PENNSYLVANIA','PUERTO RICO',
                    'RHODE ISLAND','SOUTH CAROLINA','SOUTH DAKOTA','TENNESSEE','TEXAS','US MINOR OUTLYING ISLANDS','UTAH','VERMONT','VIRGIN ISLANDS',
                    'VIRGINIA','WASHINGTON','WEST VIRGINIA','WISCONSIN','WYOMING']   
        self._StateList.extend(self._StateAbbrevList)                             
        #
        self.StateName2Abbrev = {
            'ALABAMA':'AL','ALASKA':'AK','AMERICAN SAMOA':'AS','ARIZONA':'AZ','ARKANSAS':'AR','ARMED FORCES AFRICA, CANADA, EUROPE AND THE MIDDLE EAST':'AE','ARMED FORCES AMERICAS (EXCEPT CANADA)':'AA',
            'ARMED FORCES PACIFIC':'AP','CALIFORNIA':'CA','COLORADO':'CO','CONNECTICUT':'CT','DELAWARE':'DE','DISTRICT OF COLUMBIA':'DC','FLORIDA':'FL','GEORGIA':'GA','GUAM':'GU',
            'HAWAII':'HI','IDAHO':'ID','ILLINOIS':'IL','INDIANA':'IN','IOWA':'IA','KANSAS':'KS','KENTUCKY':'KY','LOUISIANA':'LA','MAINE':'ME','MARYLAND':'MD','MASSACHUSETTS':'MA',
            'MICHIGAN':'MI','MINNESOTA':'MN','MISSISSIPPI':'MS','MISSOURI':'MO','MONTANA':'MT','NEBRASKA':'NE','NEVADA':'NV','NEW HAMPSHIRE':'NH','NEW JERSEY':'NJ','NEW MEXICO':'NM',
            'NEW YORK':'NY','NORTH CAROLINA':'NC','NORTH DAKOTA':'ND','NORTHERN MARIANA ISLANDS':'MP','OHIO':'OH','OKLAHOMA':'OK','OREGON':'OR','PENNSYLVANIA':'PA','PUERTO RICO':'PR',
            'RHODE ISLAND':'RI','SOUTH CAROLINA':'SC','SOUTH DAKOTA':'SD','TENNESSEE':'TN','TEXAS':'TX','US MINOR OUTLYING ISLANDS':'UM','UTAH':'UT','VERMONT':'VT','VIRGIN ISLANDS':'VI',
            'VIRGINIA':'VA','WASHINGTON':'WA','WEST VIRGINIA':'WV','WISCONSIN':'WI','WYOMING':'WY' }
        #
        self._ProvinceAbbrevList = ['AB','BC','MB','NB','NL','NS','NT','ON','PE','QC','SK','YT']

    #
    @property
    def Geo50List(self):
        return self.geo_50_list

    #
    @property
    def Geo100List(self):
        return self.geo_100_list
    
    #
    @property
    def GeoCityList(self):
        return self.usCities

    #
    @property
    def StateAbbrevList(self):
        return self._StateAbbrevList

    #
    def StateAbbrev(self, stateName):
        name = stateName.upper()
        if name in self.StateName2Abbrev:
            return self.StateName2Abbrev[name]
        else:
            return ''

    #
    def IsUSState(self, state):
        if state.upper() in self._StateList:
            return True 
        else:
            return False
    #
    def IsCanadianProvince(self, province):
        if province.upper() in self._ProvinceAbbrevList:
            return True 
        else:
            return False

    # Concord, North Carolina 
    # Concord, North Carolina 28804
    def ParseCityStateZip(self, city_state_zip):
        #
        idx = city_state_zip.find(',')
        if idx==-1: idx = city_state_zip.find(' ')
        city = city_state_zip[:idx]
        #
        idx2 = city_state_zip.find(',', idx+1)
        if idx2 > 1: idx = idx2         
        state_zip = city_state_zip[idx+1:].strip()
        idx = state_zip.rfind(' ') # search for space from the end: e.g. New Hampshire 01341
        if idx == -1:
            stateOrProvince = state_zip.strip(' .')
            postalCode = ''
        else:            
            if not state_zip[idx+1:].replace('-','').isdigit():
                idx = state_zip.find(' ') # search for space from the start (for Canadian postalCode)
            #
            if self.IsUSState(state_zip) or self.IsCanadianProvince(state_zip):
                stateOrProvince = state_zip.strip(' .')
                postalCode = ''
            else:
                postalCode = state_zip[idx+1:]  
                stateOrProvince = state_zip[:idx].strip(' .')
        #    
        #testStateOrProvice = stateOrProvince.replace('.','').strip()
        if not self.IsUSState(stateOrProvince) and not self.IsCanadianProvince(stateOrProvince):
            city = stateOrProvince = postalCode = ''
        #for a in stateOrProvince: print("{}: {}: {}".format(city_state_zip, a, ord(a)))
        #
        return city,stateOrProvince,postalCode