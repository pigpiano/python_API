import psycopg
from psycopg import sql
import os
from typing import Union


# problem 1
def entire_search(CONNECTION: str, table_name: str) -> list:
    # 연결 및 커서 None으로 초기화, 데이터베이스를 연결을 관리하고 SQL 쿼리를 실행하는데 사용.
    conn = None
    cur = None # SQL 명령을 수행하기 위한 객체
    
    try:
        # 연결 설정
        with psycopg.connect(CONNECTION) as conn:
            with conn.cursor() as cur:
                # 쿼리 작성하기, 특정 테이블에 있는 전체 데이터 가져오기
                query = f"SELECT * FROM myschema.{table_name}"
         
                
                # SQL 쿼리 실행
                cur.execute(query)
                
                # 모든 결과 가져와서 results에 담기
                results = cur.fetchall()
                
            # 가져온 결과 반환
            return results
        
    except Exception as e:

        print(f"An error occurred: {str(e)}")
        
    finally:
        # 연결이 닫히도록 보장
        if conn is not None:
            conn.close()


# problem 2
def registration_history(CONNECTION: str, student_id: str) -> Union[list, None]:
    conn = None
    cursor = None
    try:
        # 데이터베이스에 연결
        conn = psycopg.connect(CONNECTION)
        #커서 객체 생성
        cursor = conn.cursor()
        
        # 제공된 student_id를 가진 학생이 존재하는지 확인
        cursor.execute("""
            SELECT COUNT(*) 
            FROM myschema.students 
            WHERE "STUDENT_ID" = %s;
        """, (student_id,))
        
        if cursor.fetchone()[0] == 0:
            return f"Not Exist student with STUDENT_ID: {student_id}"
        
        # 학생의 수강신청 내역 검색, 오름차순 ASC 디폴트
        cursor.execute("""
            SELECT 
                c."YEAR", 
                c."SEMESTER", 
                c."COURSE_ID_PREFIX",
                c."COURSE_ID_NO", 
                c."DIVISION_NO", 
                c."COURSE_NAME", 
                f."NAME" AS prof_name, 
                g."GRADE"::FLOAT 
            FROM 
                myschema.course_registration cr
                JOIN myschema.course c ON cr."COURSE_ID" = c."COURSE_ID"
                JOIN myschema.faculty f ON c."PROF_ID" = f."ID"
                LEFT JOIN myschema.grade g ON cr."COURSE_ID" = g."COURSE_ID" AND cr."STUDENT_ID" = g."STUDENT_ID"
            WHERE 
                cr."STUDENT_ID" = %s
            ORDER BY
                c."YEAR", c."SEMESTER", c."COURSE_NAME";
        """, (student_id,))
        
        result = cursor.fetchall()
    # 예외 처리하기
    except Exception as e:
        return str(e)
    
    finally:
        # 연결이 닫히도록 보장
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()
    
    # 결과를 딕셔너리 리스트로 변환하고 가로로 출력.
    columns = ["year", "semester", "course_id_prefix", "course_id_no", "division_no", "course_name", "prof_name", "grade"]
    
    for row in result:
        data_dict = dict(zip(columns, row))
        
        
        print_line = ""
        for key, value in data_dict.items():
            print_line += f"{key}: {value} | "
        print(print_line)
        print("-" * 100)



# problem 3
def registration(CONNECTION: str, course_id: int, student_id: str) -> Union[list, None]:
    conn = None
    cursor = None
    
    try:
        conn = psycopg.connect(CONNECTION)
        cursor = conn.cursor()
        
        # 입력된 강의가 존재하는지 check
        cursor.execute("""SELECT COUNT(*) FROM myschema.course WHERE "COURSE_ID" = %s;""", (course_id,))
        if cursor.fetchone()[0] == 0:
            return f"Not Exist course with COURSE ID: {course_id}"
        
        # 입력된 학번을 가지는 학생이 존재하는지 check
        cursor.execute("""SELECT COUNT(*) FROM myschema.students WHERE "STUDENT_ID" = %s;""", (student_id,))
        if cursor.fetchone()[0] == 0:
            return f"Not Exist student with STUDENT ID: {student_id}"
        
        # 이미 해당 강의에 수강 신청을 했는지 check
        cursor.execute("""
            SELECT COUNT(*) 
            FROM myschema.course_registration 
            WHERE "COURSE_ID" = %s AND "STUDENT_ID" = %s;
        """, (course_id, student_id))
        if cursor.fetchone()[0] > 0:
            cursor.execute("""
                SELECT s."NAME", c."COURSE_NAME"
                FROM myschema.students s
                JOIN myschema.course c ON c."COURSE_ID" = %s
                WHERE s."STUDENT_ID" = %s;
            """, (course_id, student_id))
            student_name, course_name = cursor.fetchone()
            return f"{student_name} is already registered in {course_name}"
        
        # 수강 신청하기
        cursor.execute("""
            INSERT INTO myschema.course_registration ("COURSE_ID", "STUDENT_ID") 
            VALUES (%s, %s);
        """, (course_id, student_id))
        conn.commit()
        
        # 해당 목적에 맞게 변경된 테이블 반환
        cursor.execute("SELECT * FROM myschema.course_registration;")
        all_registrations = cursor.fetchall()
        
        
    except Exception as e:
        return str(e)
    
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()
            
    # 업데이트된 'course_registration' 테이블 반환
    return all_registrations
    


# problem 4
def withdrawal_registration(CONNECTION: str, course_id: int, student_id: str) -> Union[list, None]:
    conn = None
    cursor = None
    try:
        conn = psycopg.connect(CONNECTION)
        cursor = conn.cursor()

        # 입력된 강의가 존재하는지 확인하기
        cursor.execute("""
            SELECT COUNT(*) 
            FROM myschema.course 
            WHERE "COURSE_ID" = %s;
        """, (course_id,))
        if cursor.fetchone()[0] == 0:
            return f"Not Exist course with COURSE ID: {course_id}"

        # 입력된 학번을 가지는 학생이 존재하는지 확인
        cursor.execute("""
            SELECT COUNT(*) 
            FROM myschema.students 
            WHERE "STUDENT_ID" = %s;
        """, (student_id,))
        if cursor.fetchone()[0] == 0:
            return f"Not Exist student with STUDENT ID: {student_id}"

        # 해당 강의에 학생이 등록이 되어있는지 확인하기
        cursor.execute("""
            SELECT COUNT(*) 
            FROM myschema.course_registration 
            WHERE "COURSE_ID" = %s AND "STUDENT_ID" = %s;
        """, (course_id, student_id,))
        if cursor.fetchone()[0] == 0:
            cursor.execute("""
                SELECT "NAME" 
                FROM myschema.students 
                WHERE "STUDENT_ID" = %s;
            """, (student_id,))
            student_name = cursor.fetchone()[0]
            cursor.execute("""
                SELECT "COURSE_NAME" 
                FROM myschema.course 
                WHERE "COURSE_ID" = %s;
            """, (course_id,))
            course_name = cursor.fetchone()[0]
            return f"{student_name} is not registrated in {course_name}"

        # 수강 철회
        cursor.execute("""
            DELETE FROM myschema.course_registration 
            WHERE "COURSE_ID" = %s AND "STUDENT_ID" = %s;
        """, (course_id, student_id,))
        conn.commit()

        # 해당 목적에 맞게 변경된 테이블 반환
        cursor.execute("""
            SELECT * 
            FROM myschema.course_registration;
        """)
        return cursor.fetchall()
    
    except Exception as e:
        return str(e)

    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


# problem 5
def modify_lectureroom(CONNECTION: str, course_id: int, buildno: str, roomno: str) -> Union[list, None]:
    conn = None
    cursor = None
    
    try:
        # 데이터 베이스 연결
        conn = psycopg.connect(CONNECTION)
        cursor = conn.cursor()
        
        # 입력된 강의가 존재하는지 확인
        cursor.execute("""
            SELECT COUNT(*) 
            FROM myschema.course 
            WHERE "COURSE_ID" = %s;
        """, (course_id,))
        if cursor.fetchone()[0] == 0:
            return f"Not Exist course with COURSE ID: {course_id}"
        
        # 새로 지정된 건물명과 호수가 존재하는지 확인
        cursor.execute("""
            SELECT COUNT(*) 
            FROM myschema.lectureroom 
            WHERE "BUILDNO" = %s AND "ROOMNO" = %s;
        """, (buildno, roomno))
        if cursor.fetchone()[0] == 0:
            return f"Not Exist lecture room with BUILD NO: {buildno} / ROOM NO: {roomno}"
        
        # Update the lecture room of the course
        cursor.execute("""
            UPDATE myschema.course 
            SET "BUILDNO" = %s, "ROOMNO" = %s
            WHERE "COURSE_ID" = %s;
        """, (buildno, roomno, course_id))
        conn.commit()
        
        # 업데이트된 테이블 반환
        cursor.execute("""
            SELECT * 
            FROM myschema.course;
        """)
        table = cursor.fetchall()
        return table
    
    except Exception as e:
        return str(e)
    
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


# sql file execute ( Not Edit )
def execute_sql(CONNECTION, path):
    folder_path = '/'.join(path.split('/')[:-1])
    file = path.split('/')[-1]
    if file in os.listdir(folder_path):
        with psycopg.connect(CONNECTION) as conn:
            conn.execute(open(path, 'r', encoding='utf-8').read())
            conn.commit()
        print("{} EXECUTRED!".format(file))
    else:
        print("{} File Not Exist in {}".format(file, folder_path))
