import sqlite3
import csv
import re
from copy import deepcopy
from collections import defaultdict
from itertools import zip_longest


class DeidentifiedContent(object):
    def __init__(self, method, content):
        assert isinstance(method, str)
        assert isinstance(content, str)
        self.method = 'masking'
        self.content = ''

    def mergeable(self, other):
        if isinstance(other, str):
            return self.content == other
        elif isinstance(other, DeidentifiedContent):
            return other.mergeable(self.content)

    def __str__(self):
        return self.content

    def __repr__(self):
        return self.content


class MaskedContent(DeidentifiedContent):
    def __init__(self, content, valid=None, align='left', masking_char='*'):
        assert isinstance(content, str)
        super().__init__('masking', content)
        self.content = content
        if valid is not None:
            self.valid = valid
        else:
            valid = []
            for char in content:
                if char == masking_char:
                    valid.append(False)
                else:
                    valid.append(True)
            self.valid = tuple(valid)
        self.align = align

    def mergeable(self, other):
        if self.align == 'left':
            if isinstance(other, str):
                for valid, char1, char2 in zip_longest(self.valid, self.content, other, fillvalue=None):
                    if valid:
                        if char1 != char2:
                            break
                    else:
                        continue
                else:
                    return True
            elif isinstance(other, MaskedContent):
                if other.align == 'left':
                    for valid1, valid2, char1, char2 in zip_longest(self.valid, other.valid, self.content,
                                                                    other.content, fillvalue=None):
                        if valid1 and valid2:
                            if char1 != char2:
                                break
                        else:
                            continue
                    else:
                        return True
                else:
                    raise NotImplementedError()
            elif isinstance(other, DeidentifiedContent):
                return self.mergeable(other.content)
            else:
                raise TypeError()
        elif self.align == 'right':
            raise NotImplementedError


# class DatasetRecord(dict):
#     def __init__(self, **kwargs):
#         super().__init__(**kwargs)
#         self.merged_datasets


def mergeable(to_content, from_content, string_equivalence=None):
    if string_equivalence is None:
        def string_equivalence(string1, string2):
            return string1 == string2

    if isinstance(to_content, (str, bytes)) and isinstance(from_content, (str, bytes)):
        return string_equivalence(to_content, from_content)
    elif isinstance(to_content, DeidentifiedContent) and isinstance(from_content, (DeidentifiedContent, str)):
        return to_content.mergeable(from_content)
    elif isinstance(from_content, DeidentifiedContent) and isinstance(to_content, str):
        return from_content.mergeable(to_content)
    elif isinstance(to_content, (list, tuple, set)) and isinstance(from_content, (str, bytes, DeidentifiedContent)):
        return any(mergeable(from_content, to_content_item, string_equivalence) for to_content_item in to_content)
    elif isinstance(to_content, (str, bytes, DeidentifiedContent)) and isinstance(from_content, (list, tuple, set)):
        return any(mergeable(to_content, from_content_item, string_equivalence) for from_content_item in from_content)
    elif isinstance(to_content, (list, tuple, set)) and isinstance(from_content, (list, tuple, set)):
        return any(mergeable(from_content, to_content_item, string_equivalence) for to_content_item in to_content)
    raise TypeError()


def merge(content1, content2, string_equivalence=None):
    if string_equivalence is None:
        def string_equivalence(string1, string2):
            return string1 == string2

    if isinstance(content1, (str, bytes)) and isinstance(content2, (str, bytes)):
        if string_equivalence(content1, content2):
            return content1
        else:
            return {content1, content2}
    elif isinstance(content1, DeidentifiedContent) and isinstance(content2, (str, bytes)):
        if mergeable(content1, content2, string_equivalence):
            return content2
    elif isinstance(content1, (str, bytes)) and isinstance(content2, DeidentifiedContent):
        if mergeable(content1, content2, string_equivalence):
            return content1
    elif isinstance(content1, DeidentifiedContent) and isinstance(content2, DeidentifiedContent):
        raise NotImplementedError()
    elif isinstance(content1, (list, tuple, set)) and isinstance(content2, (str, bytes, DeidentifiedContent)):
        result_set = set()
        content2_not_merged = True
        for content1_item in content1:
            if content2_not_merged and mergeable(content1_item, content2, string_equivalence):
                result_set.add(merge(content1_item, content2, string_equivalence))
                content2_not_merged = False
            else:
                result_set.add(content1_item)
        if content2_not_merged:
            result_set.add(content2)
        return result_set
    elif isinstance(content1, (str, bytes, DeidentifiedContent)) and isinstance(content2, (list, tuple, set)):
        result_set = set()
        content1_not_merged = True
        for content2_item in content2:
            if content1_not_merged and mergeable(content1, content2_item, string_equivalence):
                result_set.add(merge(content1, content2_item, string_equivalence))
                content1_not_merged = False
            else:
                result_set.add(content2_item)
        if content1_not_merged:
            result_set.add(content1)
        return result_set
    elif isinstance(content1, (list, tuple, set)) and isinstance(content2, (list, tuple, set)):
        result_set = set(content1)
        for content2_item in content2:
            # duplicate operation exists. may be enhanced.
            result_set |= merge(result_set, content2_item, string_equivalence)

        return result_set

    raise TypeError()


def join(total_data, additional_data, equality_functions=None):
    """
    make joined table of total_data and additional_data.
    :param equality_functions: equality_functions[attribute_name] = function(string1, string2): is string is equivalence?
    :param total_data: list of dict of {str: (str or DeidentifiedContent)}
    :param additional_data: list of dict of {str: (str or DeidentifiedContent)}
    :return: joined table of total_data and additional_data
    """
    equality_functions = defaultdict(lambda: None, equality_functions)
    result_set = []
    # additional_data.record_list X total_data.record_list
    for additional_table_record in additional_data:
        matching_records = []
        for matching_total_data_record in total_data:
            # addtional_data.record.attributes X total_data.record.attributes
            for attribute_name in additional_table_record:
                if attribute_name in matching_total_data_record:
                    # addtional's attribute exists in total
                    if not mergeable(matching_total_data_record[attribute_name], additional_table_record[attribute_name],
                                     equality_functions[attribute_name]):
                        break
                else:
                    # addtional's attribute does not exists in total
                    continue
            else:
                # all attributes are mergeable -> these records are join-able.
                matching_records.append(matching_total_data_record)

        # join additional's and totals's record
        if matching_records:
            attribute_intersection = set()
            for matching_total_data_record in matching_records:
                joined_record = deepcopy(matching_total_data_record)
                if ('has matched',) in joined_record:
                    del joined_record[('has matched',)]
                for attribute_name, content in additional_table_record.items():
                    if attribute_name == ('has matched',):
                        continue

                    if attribute_name in matching_total_data_record:
                        joined_record[attribute_name] = merge(matching_total_data_record[attribute_name],
                                                           additional_table_record[attribute_name],
                                                           equality_functions[attribute_name])
                        attribute_intersection.add(attribute_name)
                    else:
                        joined_record[attribute_name] = content
                joined_record[('attribute intersection',)] = attribute_intersection
                result_set.append(joined_record)
                # mark as already matched to perform outer join
                matching_total_data_record[('has matched',)] = True

            # mark as already matched to perform outer join
            additional_table_record[('has matched',)] = True

    # 조인되지 않은 레코드
    for additional_table_record in additional_data:
        if ('has matched',) not in additional_table_record:
            result_set.append(additional_table_record)
        else:
            del additional_table_record[('has matched',)]

    for total_data_record in total_data:
        if ('has matched',) not in total_data_record:
            result_set.append(total_data_record)
        else:
            del total_data_record[('has matched',)]

    return result_set


def get_dataset_from_sqlite_narrecord_table(file_name, table_name, id_attribute='id', key_attribute='key',
                                            value_attribute='value'):
    cursor = sqlite3.connect(file_name).cursor()
    cursor.execute('SELECT {id}, {key}, {value} FROM {table}'.format(id=id_attribute, key=key_attribute, value=value_attribute,
                                                                     table=table_name))
    data_set = defaultdict(dict)
    for record_id, key, value in cursor.fetchall():
        try:
            data_set[record_id][key].add(value)
        except KeyError:
            data_set[record_id][key] = {value}

    return list(data_set.values())


def get_dataset_from_csv(file_name):
    with open(file_name, encoding='utf-8') as f:
        data_set = list(csv.DictReader(f))
    return data_set


def find(data_set, query_dict):
    result_set = []
    for data_record in data_set:
        for attribute_name, value in query_dict.items():
            if attribute_name not in data_record:
                break

            if not mergeable(data_record[attribute_name], value):
                break
        else:
            result_set.append(data_record)
    result_set.sort(reverse=True,
                    key=lambda x: len(x[('attribute intersection',)]) if ('attribute intersection',) in x else 0)
    return result_set


def unique():
    return {v['id']: v for v in L}.values()


def print_data(list_of_dict):
    for index, record in enumerate(list_of_dict, start=1):
        print('#{}'.format(index))
        for key, values in record.items():
            print('{}: {}'.format(key, values))
        print()


def print_comparison(data_set1, data_set2, joined_data_set):
    for index, record in enumerate(data_set1, start=1):
        print('[{}]'.format(index))
        for key, values in record.items():
            print('{}: {}'.format(key, values))
        print()


def main():
    # cross
    sensitive_medical_table = get_dataset_from_csv('bob_medical.csv')
    for record in sensitive_medical_table:
        record['이름'] = MaskedContent(record['이름'], align='left')
        record['전화번호'] = MaskedContent(record['전화번호'], align='left')
        record['생년월일'] = MaskedContent(record['생년월일'], align='left')
        if record['학교'] == '검정고시':
            del record['학교']

    facebook_data = get_dataset_from_sqlite_narrecord_table('facebook.db', 'fb', 'url', 'key', 'value')
    for record in facebook_data:
        if '휴대폰' in record:
            try:
                record['전화번호'] |= record.pop('휴대폰')
            except KeyError:
                record['전화번호'] = set(record.pop('휴대폰'))
        if '기타 전화번호' in record:
            try:
                record['전화번호'] |= record.pop('기타 전화번호')
            except KeyError:
                record['전화번호'] = set(record.pop('기타 전화번호'))
        if '학력' in record:
            try:
                record['학교'] |= record.pop('학력')
            except KeyError:
                record['학교'] = set(record.pop('학력'))

    equility_functions = dict()

    def gender_equal(value1, value2):
        if value1 in ('F', '여성') and value2 in ('F', '여성'):
            return True
        if value1 in ('M', '남성') and value2 in ('M', '남성'):
            return True
        return False

    def school_equal(school1, school2):
        school1_stripped = ''.join(re.findall(r'[a-zA-Z0-9ㄱ-ㅎㅏ-ㅣ가-힣]+', school1.lower()))
        school2_stripped = ''.join(re.findall(r'[a-zA-Z0-9ㄱ-ㅎㅏ-ㅣ가-힣]+', school2.lower()))
        return school1_stripped in school2_stripped or school2_stripped in school1_stripped

    equility_functions['성별'] = gender_equal
    equility_functions['학교'] = school_equal

    total_data = join(sensitive_medical_table, facebook_data, equility_functions)
    # print_data(total_data)

    # search
    # found_records = find(total_data, {'이름': '이수림'})
    found_records = find(total_data, {
        '이름': MaskedContent('정**'),
        '성별': 'M',
        '생년월일': MaskedContent('1995****'),
        '전화번호': MaskedContent('***-****-0053'),
        '트랙': '보안컨설팅',
        '이메일': 'naver.com',
        '학교': '대구가톨릭대학교',
        '질병': '고혈압'
    })
    print_data(found_records)

    #
    # - or -
    #

    # print all
    # unique_persons = unique(total_data, where=('name', 'birthday', 'address', 'sex'))
    # print_data(unique_persons)


if __name__ == '__main__':
    main()
