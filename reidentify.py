# I assume all original data is list of dict
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


def mergeable(to_content, from_content, string_equivalence=None):
    if string_equivalence is None:
        string_equivalence = lambda string1, string2: string1 == string2

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
        string_equivalence = lambda string1, string2: string1 == string2

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
    maked joined table of total_data and additional_data.
    :param equality_functions: equality_functions[column_name] = function(string1, string2): is string is equivalence?
    :param total_data: list of dict of {str: (str or DeidentifiedContent)}
    :param additional_data: list of dict of {str: (str or DeidentifiedContent)}
    :return: joined table of total_data and additional_data
    """
    equality_functions = defaultdict(lambda: None, equality_functions)
    result_set = []
    # additional_data.person_list X total_data.person_list
    for additional_table_row in additional_data:
        matching_rows = []
        for matching_total_data_row in total_data:
            # addtional_data.person.columns X total_data.person.columns
            for column_name in additional_table_row:
                if column_name in matching_total_data_row:
                    # addtional's column exists in total
                    if not mergeable(matching_total_data_row[column_name], additional_table_row[column_name],
                                     equality_functions[column_name]):
                        break
                else:
                    # addtional's column does not exists in total
                    continue
            else:
                # all columns are mergeable -> these rows are join-able.
                matching_rows.append(matching_total_data_row)

        # join additional's and totals's row
        if matching_rows:
            for matching_total_data_row in matching_rows:
                joined_row = deepcopy(matching_total_data_row)
                if ('has matched',) in joined_row:
                    del joined_row[('has matched',)]
                for column_name, content in additional_table_row.items():
                    if column_name == ('has matched',):
                        continue

                    if column_name in matching_total_data_row:
                        joined_row[column_name] = merge(matching_total_data_row[column_name],
                                                        additional_table_row[column_name],
                                                        equality_functions[column_name])
                    else:
                        joined_row[column_name] = content
                result_set.append(joined_row)
                # mark as already matched to perform outer join
                matching_total_data_row[('has matched',)] = True

            # mark as already matched to perform outer join
            additional_table_row[('has matched',)] = True

    # append not joined rows
    for additional_table_row in additional_data:
        if ('has matched',) not in additional_table_row:
            result_set.append(additional_table_row)
        else:
            del additional_table_row[('has matched',)]

    for total_data_row in total_data:
        if ('has matched',) not in total_data_row:
            result_set.append(total_data_row)
        else:
            del total_data_row[('has matched',)]

    return result_set


def get_dataset_from_sqlite_narrow_table(file_name, table_name, id_column='id', key_column='key', value_column='value'):
    cursor = sqlite3.connect(file_name).cursor()
    cursor.execute('SELECT {id}, {key}, {value} FROM {table}'.format(id=id_column, key=key_column, value=value_column,
                                                                     table=table_name))
    data_set = defaultdict(dict)
    for row_id, key, value in cursor.fetchall():
        try:
            data_set[row_id][key].add(value)
        except KeyError:
            data_set[row_id][key] = {value}

    return list(data_set.values())


def get_dataset_from_csv(file_name):
    with open(file_name, encoding='utf-8') as f:
        data_set = list(csv.DictReader(f))
    return data_set


def find(data_set, query_dict):
    result_set = []
    for data_row in data_set:
        for column_name, value in query_dict.items():
            if column_name not in data_row:
                break

            if not mergeable(data_row[column_name], value):
                break
        else:
            result_set.append(data_row)

    return result_set


def main():
    # cross
    sensitive_medical_table = get_dataset_from_csv('bob_medical.csv')
    for row in sensitive_medical_table:
        row['이름'] = MaskedContent(row['이름'], align='left')
        row['전화번호'] = MaskedContent(row['전화번호'], align='left')
        row['생년월일'] = MaskedContent(row['생년월일'], align='left')

    facebook_data = get_dataset_from_sqlite_narrow_table('facebook.db', 'fb', 'url', 'key', 'value')
    for row in facebook_data:
        if '휴대폰' in row:
            try:
                row['전화번호'] |= row.pop('휴대폰')
            except KeyError:
                row['전화번호'] = set(row.pop('휴대폰'))
        if '기타 전화번호' in row:
            try:
                row['전화번호'] |= row.pop('기타 전화번호')
            except KeyError:
                row['전화번호'] = set(row.pop('기타 전화번호'))
        if '학력' in row:
            try:
                row['학교'] |= row.pop('학력')
            except KeyError:
                row['학교'] = set(row.pop('학력'))

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
    # print(total_data)
    # search
    found_rows = find(total_data, {'이름': {'현성원'}}, )
    for index, row in enumerate(found_rows, start=1):
        print('#{}'.format(index))
        for key, values in row.items():
            print('{}: {}'.format(key, values))
        print()

        #
        # - or -
        #

        # # print all
        # unique_persons = unique(total_data, where=('name', 'birthday', 'address', 'sex'))
        # for row in unique_persons:
        #     print(row['name'], row['birthday'], row['address'], row['sex'], row['illness'])


if __name__ == '__main__':
    main()
