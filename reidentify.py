# I assume all original data is list of dict
import sqlite3
import csv
from collections import defaultdict


class DeidentifiedContent(object):
    def __init__(self, method, content):
        super().__init__()
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
    if isinstance(to_content, (str, bytes)) and isinstance(from_content, (str, bytes)):
        return to_content == from_content
    elif isinstance(to_content, (list, tuple, set)) and isinstance(from_content, (str, bytes)):
        return from_content in to_content
    elif isinstance(to_content, (str, bytes)) and isinstance(from_content, (list, tuple, set)):
        return to_content in from_content
    elif isinstance(to_content, (list, tuple, set)) and isinstance(from_content, (list, tuple, set)):
        return not set(to_content).isdisjoint(from_content)


def merge(content1, content2):
    if isinstance(content1, (str, bytes)) and isinstance(content2, (str, bytes)):
        return content1
    elif isinstance(content1, (list, tuple, set)) and isinstance(content2, (str, bytes)):
        return set(content2) | {content1}
    elif isinstance(content1, (str, bytes)) and isinstance(content2, (list, tuple, set)):
        return set(content1) | {content2}
    elif isinstance(content1, (list, tuple, set)) and isinstance(content2, (list, tuple, set)):
        return not set(content1) | set(content2)


def join(total_data, additional_data):
    """
    maked joined table of total_data and additional_data.
    :param total_data: list of dict of {str: (str or DeidentifiedContent)}
    :param additional_data: list of dict of {str: (str or DeidentifiedContent)}
    :return: joined table of total_data and additional_data
    """
    result_set = []
    # additional_data.person_list X total_data.person_list
    for additional_table_row in additional_data:
        matching_rows = []
        for matching_total_data_row in total_data:
            # addtional_data.person.columns X total_data.person.columns
            for column_name in additional_table_row:
                if column_name in matching_total_data_row:
                    # addtional's column exists in total
                    if mergeable(matching_total_data_row[column_name], additional_table_row[column_name]):
                        matching_total_data_row[column_name] = merge(matching_total_data_row[column_name],
                                                                     additional_data[column_name])
                    else:
                        break
                else:
                    # addtional;s column does not exists in total
                    continue
            else:
                # all columns are mergeable -> these rows are join-able.
                matching_rows.append(matching_total_data_row)

        # join additional's and totals's row
        if matching_rows:
            # mark as already matched to perform outer join
            additional_table_row[('has matched',)] = True
            for matching_total_data_row in matching_rows:
                # mark as already matched to perform outer join
                matching_total_data_row[('has matched',)] = True
                joined_row = matching_total_data_row[:]
                for column_name, content in additional_table_row.items():
                    if column_name not in matching_total_data_row:
                        joined_row[column_name] = content
                result_set.append(joined_row)

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
    data_set = defaultdict(lambda: defaultdict(set))
    for row_id, key, value in cursor.fetchall():
        data_set[row_id][key].add(value)

    return data_set.values()


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

    facebook_data = get_dataset_from_sqlite_narrow_table('facebook.db', 'fb', 'url', 'key', 'value')

    total_data = join(sensitive_medical_table, facebook_data)
    print(total_data)
    # search
    hyeon_rows = find(total_data, {'이메일': 'naver.com'})
    print(hyeon_rows)

    #
    # - or -
    #

    # # print all
    # unique_persons = unique(total_data, where=('name', 'birthday', 'address', 'sex'))
    # for row in unique_persons:
    #     print(row['name'], row['birthday'], row['address'], row['sex'], row['illness'])


if __name__ == '__main__':
    main()
