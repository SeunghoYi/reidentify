## 프로그램 설명
### 동작 순서  
  1. 데이터 가져오기  
    데이터를 파일로부터 가져와 dict들의 list 형태로 만든다.
        
    1. csv

      ```python
      dataset1 = get_dataset_from_csv('datafile.csv')  # list of dict형태로 리턴됨
      ```  
    1. sqlite  
      현재는 다음과 같은 narrow table 형태를 지원한다.  
                
      Id	|Key	|Value  
      ---	|---	|---
      1 	|이름	|홍길동
      1 	|나이	|20
      2 	|이름	|김영희
      2 	|전화번호	|010-1111-2222
      2 	|전화번호	|02-1234-5678
      
      ```python
      dataset2 = get_dataset_from_sqlite_narrow_table(sqlite 파일명, 가져올 테이블 명 , ID로 사용되는 컬럼명 , 속성명으로 사용되는 컬럼명, 속성값으로 사용되는 컬럼명)  # list of dict 형태로 리턴됨
      ```  
  1. 전처리  
    비식별화된 방식을 지정해 주고 필요하다면 데이터를 적당하게 편집한다.
    * 비식별화 방식 지정  
    
    ```python
    for row in sensitive_medical_table:
        row['이름'] = MaskedContent(row['이름'], align='left')
        row['전화번호'] = MaskedContent(row['전화번호'], align='left')
        row['생년월일'] = MaskedContent(row['생년월일'], align='left')
        if row['학교'] == '검정고시':
            del row['학교']
    ```  
    * 컬럼명 통일시키기
    ```python
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

    ```  
  1. 비교 함수 정의  
    특정 컬럼의 두 값이 같은지 비교하는 함수를 정의해 준다.  
    이 함수는 두 `str` 인자로 받고 같으면 `True`, 다르면 `False`를 리턴해야 한다.  
    `dictionary[컬럼명] = 함수` 형태로 만들어 함수 `join()`의 인자로 전달해 주면 된다.  
    비교 함수가 정의된 컬럼명을 가진 컬럼의 값을 비교할 때는 항상 정의한 함수를 사용하게 된다.  
    **비교 함수가 정의해 주지 않은 컬럼에서는 기본으로 문자열 상동(`str1 == str2`)을 비교함수로 사용한다.**
    
    ```python
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
    ```  
  1. 조인  
    교차시킬 두 데이터 셋과 비교 함수들의 `dict`를 인자로 `join()`을 호출한다.  
    교차하여 만들어진 큰 데이터 셋이 리턴된다.
  
    ```python
    total_data = join(sensitive_medical_dataset, facebook_crawled_dataset, equility_functions)
    ```
  1. 쿼리  
    교차된 데이터 셋으로부터 정보를 알고 싶은 사람의 속성 값 몇 개를 `dict`형 인자로 주어 검색한다.  
    이에 매치되는 모든 row를 리턴해 준다.
  
    ```python
    found_rows = find(total_data, {'url': 'https://www.facebook.com/hong.gildong/about?'})'})
    print_data(found_rows)
    
    found_rows = find(total_data, {'이름': '홍길동', '성별': 'M', '학교': '서울대학교'})
    print_data(found_rows)
    ```
    
## 프로그램에 사용한 정의  
### "비식별화 조치된 데이터"  

* 속성 컬럼과 민감 데이터 컬럼으로 구성된 데이터  

  속성1 | 속성2	|*(무관심 데이터)*	|속성3	|*(무관심 데이터)*	|**민감 데이터**  
  -----|------|---------------|-------|---------------|--------  
* 속성 컬럼은 마스킹, 총계처리 등 정보 손실이 가해져 있음  
* 민감 데이터 컬럼은 별도의 조치가 취해지지 않은 원본 값이 있음
  
### "재식별됐다"  
  
  특정 물리적인 사람을 지목하여 비식별화 조치된 데이터베이스의 민감 정보가 그 사람의 것임을 알아냈다.  

    e.g. 비식별화 조치된 의료 데이터가 재식별된 경우
    > '"현재 나의 상사인 홍길동 과장"은 2010년에 고혈압 진단을 받았다'는 사실을 알아냈다.
    > '"대한민국 대통령 박근혜"는 작년 폐암 진단을 받았다'는 사실을 알아냈다.

*  물리적 지목의 효과를 내는 방법  
        
      특정한 물리적인 인간의 것이 확실한 SNS 계정을 지목하면서 비식별화 조치된 데이터베이터 베이스의 민감 정보 그 사람의 것임을 알아낸다.  
          
        e.g.
        > '페이스북 "https://facebook.com/hong.gildong/about"에 해당하는 인물은 2010년에 고혈압 진단을 받았다'는 사실을 알아냈다.
        > '페이스북 "https://facebook.com/ghpark.korea/about"에 해당하는 인물은 작년 폐암 진단을 받았다'는 사실을 알아냈다.

### 공격 방법
1. 타겟 공격  
  1. 공격 대상 인물을 정한다.  
  
          e.g. "대한민국 대통령 박근혜" 또는 "https://facebook.com/hong.gildong/about"
  1. 공격 대상 인물에 대한 정보를 최대한 수집한다. 프로파일링을 시도하여 인터넷에 있는 정보(OSINT)를 수집하여 DB화한다.
  1.  공격자는 기관에서 배포한, 민감 정보를 포함하고 있는 데이터베이스를 얻는다.
     - 공격 대상이 포함되어 있는 데이터를 구해야 함
     - 그러나 이 데이터베이스는 비식별화 조치가 되어 어느 값이 공격 대상의 것인지 이 
  1. 비식별화된 데이터베이스에서 공격 대상 인물이 될 수 있는 row를 찾아낸다.
  1. 찾아낸 row들에 대해 그 row가 공격 대상을 얼마나 잘 기술하고 있는지의 정도를 평가하여 점수를 매긴다.
  1. 가장 일치하는 row를 찾고 그 민감 정보를 얻는다.
* 무작위 공격
  1. 공격자는 사전에 SNS 크롤링 등을 통해 대규모의 인물 데이터베이스를 구축해 놓는다.
  1. 공격자는 기관에서 배포한, 민감 정보를 포함하고 있는 데이터베이스를 얻는다.
            - 이 데이터베이스는 비식별화 조치가 되어 있음
  1. 비식별화된 데이터베이스의 각 row에 대해서 그 row가 될 수 있는 인물들을 리스팅한다.
  1. 리스팅된 인물들에 대해 그 row가 인물을 얼마나 잘 기술하고 있는지의 정도를 평가하여 점수를 매긴다.
  1. 가장 일치하는 인물을 찾고 그 인물에 해당 row의 민감 정보를 대응시킨다.
            