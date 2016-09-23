# coding=utf-8
# 모든 원본 데이터는 딕셔너리의 리스트 형태로 있다고 가정한다.
REQUIRED_INFORMATION = ['name', 'birthday', 'address', 'sex']

# input collected data
data_tables = input()
sensitive_medical_table = input()


class DeidentifiedContent(object):
    def __init__(self, method, content):
        assert isinstance(method, str)
        assert isinstance(content, str)
        self.method = 'masking'
        self.content = ''


class MaskedContent(DeidentifiedContent):
    def __index__(self, content, valid):
        assert isinstance(content, str)
        assert isinstance(valid, (list, tuple))
        DeidentifiedContent.__init__(self, 'masking', content)
        self.content = content
        self.valid = valid


def mergeable(to_content, from_content):
    return to_content == from_content


def inner_join(total_data, additional_data):
    result_set = []
    # additional_data.person_list X total_data.person_list
    for additional_table_row in additional_data:
        matching_rows = []
        for total_data_row in total_data:
            # addtional_data.person.columns X total_data.person.columns
            for column_name in additional_table_row:
                if column_name in total_data_row:
                    # addtional의 컬럼이 total에 존재
                    if mergeable(total_data_row[column_name], additional_table_row[column_name]):
                        total_data_row[column_name] = merge(total_data_row[column_name], additional_data[column_name])
                    else:
                        break
                else:
                    # addtional의 컬럼이 total에 존재 안 함
                    continue
            else:
                # 모든 컬럼을 병합 가능 -> 두 row를 조인할 수 있음
                matching_rows.append(total_data_row)
        for total_data_row in matching_rows:
            merged_row = total_data_row
            for column_name in total_data_row:
                


def main():
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


if __name__ == '__main__':
    main()
