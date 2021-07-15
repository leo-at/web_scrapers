import os,sys,csv
from datetime import datetime, timedelta

#
# full_name|doing_business_as|license_no|license_type_name|license_status_name|addr_line_1|addr_line_4|addr_city|addr_county|addr_state|addr_zipcode|
def main(input_file_path, output_file_path):
    date_collected = datetime.now().strftime('%Y-%m-%d %H:%M:%S')   
    #
    csv_encoding = 'utf-8' 
    csv_file = open(output_file_path, 'w', newline='', encoding=csv_encoding) #, encoding='windows-1250', errors ='ignore')
    csvwriter = csv.writer(csv_file)
    if csv_encoding == 'utf-8': csv_file.write(u'\ufeff') #.write(codecs.BOM_UTF8)
    csvwriter.writerow(['business_name','tradename','license_no','license_type','license_status','address1','address2','city','state','zipcode','country','collectedDate'])   
    #
    license_type_mapping = get_license_type_mapping()
    #
    license_type_list = []
    with open(input_file_path, 'rt', encoding=csv_encoding) as f:
        reader = csv.reader(f, delimiter='|')
        for row in reader:
            if row[0] == 'full_name': continue
            business_name = row[0]
            tradename = row[1]
            license_no = row[2]
            license_type = row[3]
            #
            if license_type not in license_type_list: license_type_list.append(license_type)
            #
            license_status = row[4]
            address1 = row[5]
            address2 = ''
            city = row[7]
            county = row[8]
            state = row[9]
            zipcode = row[10]
            #
            license_type_name = license_type_mapping[license_type]
            #
            csv_row = [business_name,tradename,license_no,license_type_name,license_status,address1,address2,city,state,zipcode,'USA',date_collected]
            csvwriter.writerow(csv_row)
    #
    csv_file.close()
    #
    print(license_type_list)

#
def get_license_type_mapping():
    return {
        'Center - Type 1':'Medical Marijuana Center (Type 1; up to 300 patients)', 
        'Center - Type 2':'Medical Marijuana Center (Type 2; 301 to 500 patients)', 
        'Center - Type 3':'Medical Marijuana Center (Type 3; 501 or more patients)', 
        'Infused Product Manufacturer':'Medical Marijuana Infused Products Manufacturer', 
        
        'MMJ Off- Premises Storage':'Medical Marijuana Off-Premises Storage', 

        'MMJ Operator':'Medical Marijuana Operator',
        'MMJ Testing Facility':'Medical Marijuana Testing Facility', 
        'MMJ Transporter':'Medical Marijuana Transporter', 

        'MMJ Transporter- NP':'Medical Marijuana Transporter - NP', # NP Indicates that the transporter does not have a Licensed Premises where marijuana can be stored. 

        'Optional Premises':'Medical Marijuana Optional Premises Cultivation (OPC)',         
        'Retail Marijuana Cultivation Facility':'Retail Marijuana Cultivation Facility', 
        'Retail Marijuana Products Mfg':'Retail Marijuana Products Manufacturer', 
        'Retail Marijuana Store':'Retail Marijuana Store', 
        'Retail Marijuana Testing Facility':'Retail Marijuana Testing Facility', 
        'Retail Operator':'Retail Marijuana Operator', 
        'Retail Transporter':'Retail Marijuana Transporter', 
        'RMJ Transporter- NP':'Retail Marijuana Transporter - NP', # NP Indicates that the transporter does not have a Licensed Premises where marijuana can be stored. 

        'Vendor':'Responsible Vendor Training Provider'  # https://www.colorado.gov/pacific/enforcement/responsible-vendor-training-program-providers     
    }


#
if __name__ == '__main__':
    if len(sys.argv) >= 2:
        input_file = sys.argv[1]    
    else:
        input_file = 'CO_Marijuana_Businesses_donwload.txt'

    dirname = os.path.dirname(os.path.realpath(__file__))
    input_file_path = os.path.join(dirname, input_file)
    #
    print(input_file_path)
    #
    filedir = os.path.dirname(input_file_path)
    output_file_path = os.path.join(filedir, "CO_Marijuana_Businesses.csv")

    main(input_file_path, output_file_path)