# coding=utf-8
# I assume all original data is list of dict
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


def merge(content1, content2):
    return content1


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
                for column_name, content in additional_table_row.iteritems():
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


def main():
    # cross
    total_data = sensitive_medical_table

    for table in data_tables:
        total_data = join(total_data, table)
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
