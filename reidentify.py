# coding=utf-8
REQUIRED_INFORMATION = ['name', 'birthday', 'address', 'sex']

# input collected data
data_tables = input()
sensitive_medical_table = input()

# cross
total_data = sensitive_medical_table
for table in data_tables:
    total_data = inner_join(total_data, table)
    if all(field in total_data.fileds for field in REQUIRED_INFORMATION):
        break

# search
hyeon_row = total_data.find({'name': '현범수', 'birthday': '1.1', 'address': 'jeju-do jeju-si 1-1', 'sex': '남'})
print hyeon_row['illness']

#
# - or -
#

# print all
unique_persons = unique(total_data, where=('name', 'birthday', 'address', 'sex'))
for row in unique_persons:
    print row['name'], row['birthday'], row['address'], row['sex'], row['illness']
