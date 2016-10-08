import sqlite3
import csv
import re
from copy import deepcopy
from collections import defaultdict
from itertools import zip_longest


class DeidentifiedContent(object):
    """
    일반 문자열에 대비해 비식별화된 문자열을 나타는 베이스 클래스
    """
    def __init__(self, method='masking', content=''):
        """
        :param method: str. 비식별화 방법. 현재 가능한 값: 'masking'
        :param content: str. 비식별화 후의 문자열 값.
        """
        assert isinstance(method, str)
        assert isinstance(content, str)
        self.method = method
        self.content = content

    def mergeable(self, other):
        """
        다른 문자열 또는 DeidentifiedContent와 동일하다고 볼 수 있어 하나로 합칠 수 있는지의 여부를 확인.
        :param other: str 또는 DeidentifiedContent.
        :return: bool
        """
        if isinstance(other, str):
            return self.content == other
        elif isinstance(other, DeidentifiedContent):
            return other.mergeable(self.content)

    def __str__(self):
        return self.content

    def __repr__(self):
        return self.content


class MaskedContent(DeidentifiedContent):
    """
    마스킹 처리된 내용
    """
    def __init__(self, content, valid=None, align='left', masking_char='*'):
        """
        :param content: str. 마스킹 후의 문자열 값.
        :param valid: list of bool. content와 동일한 인덱스의 값이 마스킹이 된 값인지를 나타냄.
            마스킹이 되어 있으면 False. 되어 있지 않으면 True
        :param align: 'left' or 'right'. 마스킹 처리시 문자열을 정렬한 기준.
        :param masking_char: str. 마스킹 문자
        """
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
        """
        다른 문자열 또는 DeidentifiedContent와 동일하다고 볼 수 있어 하나로 합칠 수 있는지의 여부를 확인.
        :param other: str 또는 DeidentifiedContent.
        :return: bool
        """
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


class DatasetRecord(dict):
    """
    빌트인 클래스 dict에서 상속. 키, 값 쌍을 가진 레코드를 나타낸다.
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.joined_from = ()
        self.joined_common_attributes = set()
        self.has_matched = False


def mergeable(content1, content2, string_equivalence=None):
    """
    재귀함수. 두 내용이 동등하다고 볼 수 있어 하나로 합칠 수 있는지의 여부를 확인.
    :param content1: 비교할 첫 번째 문자열
    :param content2: 비교할 두 번째 문자열
    :param string_equivalence: 문자열 대 문자열 비교시 사용할 일치 여부 함수. 기본값은 문자열 상등(==)이다.
    :return: bool
    """
    if string_equivalence is None:
        def string_equivalence(string1, string2):
            return string1 == string2

    if isinstance(content1, (str, bytes)) and isinstance(content2, (str, bytes)):
        # 문자열 vs 문자열
        return string_equivalence(content1, content2)
    elif isinstance(content1, DeidentifiedContent) and isinstance(content2, (DeidentifiedContent, str)):
        # 비식별화 문자열 vs [비식별화] 문자열
        return content1.mergeable(content2)
    elif isinstance(content1, str) and isinstance(content2, DeidentifiedContent):
        # 문자열 vs 비식별화 문자열
        return content2.mergeable(content1)
    elif isinstance(content1, (list, tuple, set)) and isinstance(content2, (str, bytes, DeidentifiedContent)):
        # 다중 값 vs 단일 값
        return any(mergeable(content2, to_content_item, string_equivalence) for to_content_item in content1)
    elif isinstance(content1, (str, bytes, DeidentifiedContent)) and isinstance(content2, (list, tuple, set)):
        # 단일 값 vs 다중 값
        return any(mergeable(content1, from_content_item, string_equivalence) for from_content_item in content2)
    elif isinstance(content1, (list, tuple, set)) and isinstance(content2, (list, tuple, set)):
        # 다중 값 vs 다중 값
        return any(mergeable(content2, to_content_item, string_equivalence) for to_content_item in content1)
    raise TypeError()


def merge(content1, content2, string_equivalence=None):
    """
    재귀함수. 동등한 두 내용을 합침.
    :param content1: 합칠 첫 번째 문자열
    :param content2: 합칠 두 번째 문자열
    :param string_equivalence: 문자열 대 문자열 비교시 사용할 일치 여부 함수. 기본값은 문자열 상등(==)이다.
    :return: 두 문자열의 정보를 합쳐 만들어진 문자열
    """
    if string_equivalence is None:
        def string_equivalence(string1, string2):
            return string1 == string2

    if isinstance(content1, (str, bytes)) and isinstance(content2, (str, bytes)):
        # 문자열 vs 문자열
        if string_equivalence(content1, content2):
            return content1
        else:
            return {content1, content2}
    elif isinstance(content1, DeidentifiedContent) and isinstance(content2, (str, bytes)):
        # 비식별화 문자열 vs 문자열
        if mergeable(content1, content2, string_equivalence):
            return content2
    elif isinstance(content1, (str, bytes)) and isinstance(content2, DeidentifiedContent):
        # 문자열 vs 비식별화 문자열
        if mergeable(content1, content2, string_equivalence):
            return content1
    elif isinstance(content1, DeidentifiedContent) and isinstance(content2, DeidentifiedContent):
        # 비식별화 문자열 vs 비식별화 문자열
        raise NotImplementedError()
    elif isinstance(content1, (list, tuple, set)) and isinstance(content2, (str, bytes, DeidentifiedContent)):
        # 다중 값 vs 단일 값
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
        # 단일 값 vs 다중 값
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
        # 다중 값 vs 다중 값
        result_set = set(content1)
        for content2_item in content2:
            # duplicate operation exists. may be enhanced.
            result_set |= merge(result_set, content2_item, string_equivalence)

        return result_set

    raise TypeError()


def join(total_dataset, additional_dataset, equality_functions=None):
    """
    total_dataset과 addtional_dataset을 조인한 데이터셋을 만든다.
    :param equality_functions: equality_functions[attribute_name] = function(string1, string2): 두 문자열이 동등한지의 여부
    :param total_dataset: DatasetRecord 객체의 리스트
    :param additional_dataset: list of DatasetRecord obejct 객체의 리스트
    :return: total_dataset과 additional_dataset을 조인해 만든 데이터셋
    """
    equality_functions = defaultdict(lambda: None, equality_functions)
    result_set = []
    # additional_dataset 레코드들 X total_dataset 레코드들
    for additional_data_record in additional_dataset:
        assert isinstance(additional_data_record, DatasetRecord)
        matching_records = []
        for total_data_record in total_dataset:
            assert isinstance(total_data_record, DatasetRecord)

            # addtional_dataset.레코드[i].속성들 X total_dataset.레코드[j].속성들
            for attribute_name in additional_data_record:
                if attribute_name in total_data_record:
                    # addtional 쪽의 속성이 total 쪽에도 존재하는 경우
                    if not mergeable(total_data_record[attribute_name],
                                     additional_data_record[attribute_name], equality_functions[attribute_name]):
                        break
                else:
                    # addtional 쪽의 속성이 total 쪽에도 존재하는 경우
                    continue
            else:
                # 모든 속성이 mergeable -> 이 두 레코드는 조인 가능함
                matching_records.append(total_data_record)

        # additional의 레코드와 totals의 레코드들을 조인
        if matching_records:
            print('*' + record_summary(additional_data_record, exclude_attribute=('url', '사진', '페이스북 커버 사진')))
            for total_data_record in matching_records:
                attribute_intersection = set()
                assert isinstance(total_data_record, DatasetRecord)
                joined_record = deepcopy(total_data_record)
                assert isinstance(joined_record, DatasetRecord)
                if joined_record.has_matched:
                    joined_record.has_matched = False
                for attribute_name, content in additional_data_record.items():
                    if attribute_name in total_data_record:
                        joined_record[attribute_name] = merge(total_data_record[attribute_name],
                                                              additional_data_record[attribute_name],
                                                              equality_functions[attribute_name])
                        attribute_intersection.add(attribute_name)
                        if isinstance(total_data_record[attribute_name], (str, DeidentifiedContent)):
                            total_data_value = total_data_record[attribute_name]
                        else:
                            assert isinstance(total_data_record[attribute_name], set)
                            total_data_value = ','.join(total_data_record[attribute_name])

                        if isinstance(additional_data_record[attribute_name], (str, DeidentifiedContent)):
                            additional_data_value = additional_data_record[attribute_name]
                        else:
                            assert isinstance(additional_data_record[attribute_name], set)
                            additional_data_value = ','.join(additional_data_record[attribute_name])
                    else:
                        joined_record[attribute_name] = content
                joined_record.joined_common_attributes = attribute_intersection
                result_set.append(joined_record)
                # 아우터 조인에 사용하기 위해 이미 매칭된 컬럼으로 표시
                total_data_record.has_matched = True
                joined_record.joined_from = (total_dataset, additional_dataset)
                # 출력
                summary = record_summary(total_data_record, exclude_attribute=('url', '사진', '페이스북 커버 사진'))
                print('  {} -> {}'.format(str(joined_record.joined_common_attributes), summary))
            print()

            # 아우터 조인에 사용하기 위해 이미 매칭된 컬럼으로 표시
            additional_data_record.has_matched = True

    # 조인되지 않은 레코드
    for additional_data_record in additional_dataset:
        assert isinstance(additional_data_record, DatasetRecord)
        if not additional_data_record.has_matched:
            result_set.append(additional_data_record)
        else:
            additional_data_record.has_matched = False

    for total_data_record in total_dataset:
        assert isinstance(total_data_record, DatasetRecord)
        if not total_data_record.has_matched:
            result_set.append(total_data_record)
        else:
            total_data_record.has_matched = False

    return result_set


def get_dataset_from_sqlite_narrecord_table(file_name, table_name, id_attribute='id', key_attribute='key',
                                            value_attribute='value'):
    """
    narrow table 형태로 되어 있는 sqlite 테이블을 불러옴.
    :param file_name: sqlite 데이터베이스 파일 이름
    :param table_name: 불러올 테이블 이름
    :param id_attribute: ID로 사용되는 컬럼명
    :param key_attribute: 속성명으로 사용되는 컬럼명명
    :param value_attribute: 속성 값으로 사용되는 컬럼명
    :return: DatasetRecord의 list형태로 되어 있는 데이터셋.
    """
    cursor = sqlite3.connect(file_name).cursor()
    cursor.execute(
        'SELECT {id}, {key}, {value} FROM {table}'.format(id=id_attribute, key=key_attribute, value=value_attribute,
                                                          table=table_name))
    data_set = defaultdict(DatasetRecord)
    for record_id, key, value in cursor.fetchall():
        try:
            data_set[record_id][key].add(value)
        except KeyError:
            data_set[record_id][key] = {value}

    return list(data_set.values())


def get_dataset_from_csv(file_name):
    """
    csv 파일로부터 데이터를 불러옴.
    :param file_name: csv 파일 이름
    :return: DatasetRecord의 list형태로 되어 있는 데이터셋.
    """
    data_set = []
    with open(file_name, encoding='utf-8') as f:
        for record in csv.DictReader(f):
            data_set.append(DatasetRecord(**record))
    return data_set


def find(data_set, query_dict):
    """
    data_set에서 query_dict와 조인 가능한 레코드를 반환
    :param data_set: DataRecord의 list.
    :param query_dict: dict.
    :return: DataRecord의 list. 조인 가능한 모든 레코드의 리스트
    """
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
                    key=lambda x: len(x.joined_common_attributes))
    return result_set


def collapsed_string(original_string, max_length=15, front_leaving=7, end_leaving=7):
    """
    너무 긴 문자열을 ...으로 줄임.
    :param original_string: 원본 문자열
    :param max_length: 문자열을 줄이지 않는 최대 길이
    :param front_leaving: ...의 앞에 남길 길이
    :param end_leaving: ...의 뒤에 남길 길이
    :return:
    """
    if len(original_string) <= max_length:
        return original_string
    return ''.join((original_string[:front_leaving], '...', original_string[front_leaving:][-end_leaving:]))


def record_summary(record, exclude_attribute=()):
    exclude_attribute = set(exclude_attribute)
    summary = []
    for attribute_name, attribute_value in record.items():
        if attribute_name in exclude_attribute:
            continue

        attribute = attribute_name + ': '
        if isinstance(attribute_value, set):
            if len(attribute_value) == 1:
                attribute += collapsed_string(str(next(iter(attribute_value))))
            else:
                attribute += '{' + ','.join(collapsed_string(item) for item in attribute_value) + '}'
        else:
            attribute += collapsed_string(str(attribute_value))
        summary.append(attribute)

    return ', '.join(summary)


def print_data(list_of_dict):
    for index, record in enumerate(list_of_dict, start=1):
        assert isinstance(record, DatasetRecord)
        print('#{}'.format(index))
        for key, values in record.items():
            print('{}: {}'.format(key, values))
        print('공통 컬럼: {}'.format(record.joined_common_attributes))
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
