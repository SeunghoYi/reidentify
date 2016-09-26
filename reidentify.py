# I assume all original data is list of dict
import sqlite3
import csv
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
    def __init__(self, content, valid, align='left'):
        assert isinstance(content, str)
        assert isinstance(valid, (list, tuple))
        super().__init__('masking', content)
        self.content = content
        self.valid = valid
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
    if isinstance(to_content, (str, bytes)) and isinstance(from_content, (str, bytes)):
        if to_content == from_content:
            return True
        if string_equivalence:
            if string_equivalence[to_content] == string_equivalence[from_content]:
                return True
        return False
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
    if isinstance(content1, (str, bytes)):
        return content1
    elif isinstance(content2, (str, bytes)):
        if content2 in string_equivalence:
            return string_equivalence[content2][0]
        return content2
    elif isinstance(content1, DeidentifiedContent) and isinstance(content2, DeidentifiedContent):
        raise NotImplementedError()
    elif isinstance(content1, (list, tuple, set)) and isinstance(content2, (str, bytes, DeidentifiedContent)):
        result_set = set()
        content2_not_merged = True
        for content1_item in content1:
            if content2_not_merged and mergeable(content1_item, content2):
                result_set.add(merge(content1_item, content2))
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
            if content1_not_merged and mergeable(content2_item, content1):
                result_set.add(merge(content2_item, content1))
                content1_not_merged = False
            else:
                result_set.add(content2_item)
        if content1_not_merged:
            result_set.add(content1)
        return result_set
    elif isinstance(content1, (list, tuple, set)) and isinstance(content2, (list, tuple, set)):
        result_set = set(content1)
        for content2_item in content2:
            result_set |= merge(result_set, content2_item)  # duplicate operation exists. may be enhanced.

        return result_set

    raise TypeError()


def join(total_data, additional_data, equivalent_values=None):
    """
    maked joined table of total_data and additional_data.
    :param total_data: list of dict of {str: (str or DeidentifiedContent)}
    :param additional_data: list of dict of {str: (str or DeidentifiedContent)}
    :return: joined table of total_data and additional_data
    """
    equivalent_value = defaultdict(dict)
    if equivalent_values is not None:
        for column, value_pairs in equivalent_values.items():
            for value1, value2 in value_pairs:
                equivalent_value[column][value1] = (value1, value2)
                equivalent_value[column][value2] = (value1, value2)

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
                                     equivalent_value[column_name]):
                        break
                else:
                    # addtional;s column does not exists in total
                    continue
            else:
                # all columns are mergeable -> these rows are join-able.
                matching_rows.append(matching_total_data_row)

        # join additional's and totals's row
        if matching_rows:
            for matching_total_data_row in matching_rows:
                joined_row = deepcopy(matching_total_data_row)
                for column_name, content in additional_table_row.items():
                    if column_name in matching_total_data_row:
                        joined_row[column_name] = merge(matching_total_data_row[column_name],
                                                        additional_table_row[column_name])
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
        row['이름'] = MaskedContent(row['이름'], (True, False, False), align='left')
        row['전화번호'] = MaskedContent(row['전화번호'],
                                    (False, False, False, False, False, False, False, False, False,
                                     True, True, True, True),
                                    align='left')
        row['생년월일'] = MaskedContent(row['생년월일'],
                                    (True, True, True, True, False, False, False, False),
                                    align='left')

    facebook_data = get_dataset_from_sqlite_narrow_table('facebook.db', 'fb', 'url', 'key', 'value')

    equivalent_values = dict()
    equivalent_values['성별'] = {('F', '여성'), ('M', '남성')}

    total_data = join(sensitive_medical_table, facebook_data, equivalent_values)
    # print(total_data)
    # search
    found_rows = find(total_data, {'이름': '이제형', '학력': 'Hanyang University, ERICA - 한양대학교 ERICA'})
    for row in found_rows:
        print(row)

        #
        # - or -
        #

        # # print all
        # unique_persons = unique(total_data, where=('name', 'birthday', 'address', 'sex'))
        # for row in unique_persons:
        #     print(row['name'], row['birthday'], row['address'], row['sex'], row['illness'])


if __name__ == '__main__':
    main()
