import os,sys
import csv
import hashlib


#
def getMD5Hash(input_text):
    hashob = hashlib.md5()
    hashob.update(input_text)
    return hashob.hexdigest()

#
def process_csv_dir(fullpathdir):
    print(fullpathdir)
    #
    key_list = []
    unique_records = []
    #
    count_input = 0
    count_output = 0
    for input_file in os.listdir(fullpathdir):
        if not input_file.endswith(".csv"): continue
        fullpathfile = os.path.join(fullpathdir,input_file)
        print(fullpathfile)
        #
        with open(fullpathfile, 'r', encoding='utf-8-sig') as csvfile:
            reader = csv.reader(csvfile, delimiter=',')
            for row in reader:
                count_input += 1
                data_key = '|'.join(row)
                if data_key in key_list: continue
                key_list.append(data_key)
                unique_records.append(row)
                count_output += 1
    #
    hash_list = []    
    count_institutions = 0
    output_file = 'accreditation_records.csv'
    instit_file = 'accreditation_institutions.csv'
    with open(output_file, 'w', newline='', encoding='utf-8-sig') as outCsvFile:
        with open(instit_file, 'w', newline='', encoding='utf-8-sig') as instCsvFile:
            wtr = csv.writer(outCsvFile, delimiter=',')
            instWtr = csv.writer(instCsvFile, delimiter=',')
            for row in unique_records:
                #
                name_addr = "{}|{}|{}|{}|{}|{}|{}|{}".format(row[1],row[8],row[9],row[10],row[11],row[12],row[13],row[14])
                hash_val = getMD5Hash(name_addr.encode('utf-8'))
                #
                if row[0] == 'inst_OPE_id':
                    row.insert(0, 'recordId')
                else:
                    row.insert(0, hash_val)
                wtr.writerow(row)         
                #
                if hash_val in hash_list: continue
                hash_list.append(hash_val)    
                #
                instWtr.writerow(row[0:16])
                count_institutions += 1
    #
    print("input: {}; output: {}; institutions: {}; hash #: {}".format(count_input, count_output, count_institutions, len(hash_list)))


#
if __name__ == '__main__':
    if len(sys.argv) >= 2:
        input_dir = sys.argv[1]
    else:
        input_dir = "."
    isDir = os.path.isdir(input_dir)
    if not isDir:
        print("Please specify a directory contains csv file...")
    else:
        fullpath = os.path.abspath(input_dir)        
        process_csv_dir(fullpath)

