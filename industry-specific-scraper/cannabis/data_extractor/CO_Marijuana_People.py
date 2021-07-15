import sys
import csv, json
from datetime import datetime

#
def main(input_file_path):
    output_file_path = input_file_path.replace('.csv', '_pivot.csv')
    read_people_file(input_file_path, output_file_path)

#
def read_people_file(input_file_path, output_file_path):
    csv_encoding = 'utf-8' 
    #
    csv_file = open(output_file_path, 'w', newline='', encoding=csv_encoding) #, encoding='windows-1250', errors ='ignore')
    csvwriter = csv.writer(csv_file)
    if csv_encoding == 'utf-8': csv_file.write(u'\ufeff') #.write(codecs.BOM_UTF8)
    csvwriter.writerow(['business_license_number','license_expiration_date','fullname1','fullname2','fullname3','fullname4','fullname5','fullname6','fullname7','fullname8','fullname9','sourceUrl','collectedDate'])   
    #
    business_license_people = {}
    business_license_Date = {}
    #
    with open(input_file_path, 'rt', encoding=csv_encoding) as f:
        reader = csv.reader(f, delimiter=',')
        for row in reader:
            if row[1] == 'middlename': continue
            business_license_number = row[10]
            business_license_status = row[11]
            #
            #if business_license_number not in ['402-00011','406RNP-00001','405R-00016','405R-00005']: continue
            #
            license_type = row[5]
            license_status = row[6]
            expiration_date = row[7]
            try:
                if expiration_date.strip() != '':
                    dt_expiration_date = datetime.strptime(expiration_date, '%m/%d/%Y')
                else:
                    dt_expiration_date = datetime.strptime('1/1/1000', '%m/%d/%Y')
            except:
                print("{}: {}".format(business_license_number, row))
                continue
            #
            middlename = ' ' + row[1] if row[1] else ''
            suffix = ', ' + row[3] if row[3] else ''
            fullname = "{}{} {}{}".format(row[0], middlename, row[2], suffix)
            #
            if business_license_number not in business_license_people: business_license_people[business_license_number] = {}
            #
            license_people = business_license_people[business_license_number]
            if fullname in license_people:
                status_date = license_people[fullname]
                dt_status_date = status_date['expiration_date']
                if dt_expiration_date > dt_status_date: 
                    license_people[fullname] = {'license_status':license_status, 'expiration_date':dt_expiration_date, 'license_type':license_type,'date_collected':row[13],'sourceUrl':row[12]}
            else:                
                license_people[fullname] = {'license_status':license_status, 'expiration_date':dt_expiration_date, 'license_type':license_type,'date_collected':row[13],'sourceUrl':row[12]}
            #
            if business_license_number not in business_license_Date: 
                business_license_Date[business_license_number] = expiration_date
            else:
                if business_license_Date[business_license_number] and dt_expiration_date:
                    dt_license_date = datetime.strptime(business_license_Date[business_license_number], '%m/%d/%Y')
                    if dt_expiration_date > dt_license_date: business_license_Date[business_license_number] = expiration_date                

    # print(json.dumps(business_license_people))
    #
    for business_license_number, people in business_license_people.items():
        license_expiration_date = business_license_Date[business_license_number]
        fullname_list = []
        for fullname,status_date in sorted(people.items(), key=lambda x:x[1]['expiration_date'], reverse=True):
            fullname_list.append(fullname)  
            date_collected = status_date['date_collected'] 
            sourceUrl = status_date['sourceUrl']
        #
        fullname1 = fullname_list[0] if len(fullname_list) > 0 else ''
        fullname2 = fullname_list[1] if len(fullname_list) > 1 else ''
        fullname3 = fullname_list[2] if len(fullname_list) > 2 else ''
        fullname4 = fullname_list[3] if len(fullname_list) > 3 else ''
        fullname5 = fullname_list[4] if len(fullname_list) > 4 else ''
        fullname6 = fullname_list[5] if len(fullname_list) > 5 else ''
        fullname7 = fullname_list[6] if len(fullname_list) > 6 else ''
        fullname8 = fullname_list[7] if len(fullname_list) > 7 else ''
        fullname9 = fullname_list[8] if len(fullname_list) > 8 else ''
        #
        csv_row = [business_license_number,license_expiration_date,fullname1,fullname2,fullname3,fullname4,fullname5,fullname6,fullname7,fullname8,fullname9,sourceUrl,date_collected]
        csvwriter.writerow(csv_row)            
    #
    csv_file.close()
#
if __name__ == '__main__':
    if len(sys.argv) >= 2:
        input_file = sys.argv[1]    
    else:
        input_file = 'CO_Marijuana_People.csv'
    #
    main(input_file)
