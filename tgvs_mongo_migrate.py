import os.path
import re
import shutil
from distutils.dir_util import copy_tree
from tkinter import Tk, filedialog
from minio import Minio
import psycopg2
import threading
from tqdm import tqdm
from datetime import datetime, timedelta
from pymongo import MongoClient

# 데이터베이스 설정
hostname = 'bigsoft.iptime.org'
database = 'keti_pe_3pdx'
username = 'keti_superuser'
password = 'madcoder'
port_id = 55432  # PostgreSQL의 기본 포트는 5432

# 데이터베이스에 연결
conn = psycopg2.connect(
    host=hostname,
    dbname=database,
    user=username,
    password=password,
    port=port_id
)
cur = conn.cursor()

# MinIO 서버 연결 설정
client = Minio('bigsoft.iptime.org:59000',
                access_key='keti_root',
                secret_key='madcoder',
                secure=False)




def find_directories_with_pattern(path, pattern, target_path):
    created_directories = []
    for item in os.listdir(path):
        item_path = os.path.join(path, item)
        if os.path.isdir(item_path) and re.match(pattern, item):
            date = item[:13]
            new_folder = os.path.join(target_path, date)
            if not os.path.exists(new_folder):
                os.makedirs(new_folder)
                created_directories.append(new_folder)
            copy_dir(item_path, new_folder)
    return created_directories


# 선택폴더에서 정규식에 맞는 폴더명 골라서 target_path에 [14:27]으로 폴더이름 만들고 복사
def find_directories_with_pattern_BS(path, pattern, target_path):
    created_directories = []
    for item in os.listdir(path):
        # print("item:",item)
        item_path = os.path.join(path, item)
        # print("path:",path)
        if os.path.isdir(item_path) and re.match(pattern, item):
            date = item[14:27]
            new_folder = os.path.join(target_path, date)
            if not os.path.exists(new_folder):
                os.makedirs(new_folder)
                created_directories.append(new_folder)
            job_file = item_path + '\\' + '7.JOB File, STL'
            new_folder = new_folder + '\\' + '7.JOB File, STL'

            if os.path.isdir(job_file):
                # print(job_file)
                # print("item_path:",item_path)
                print("new_folder:",new_folder)
                copy_tree(job_file, new_folder)
    return created_directories


# buildstrategy 로 옮기기
def into_BS(src_path):
    dir_paths = []
    for item in os.listdir(src_path):
        item_path = os.path.join(src_path, item)
        if os.path.isdir(item_path):
            dir_paths.append(item_path)
    for path in dir_paths:
        for item in os.listdir(path):
            new_path = path + "\\" + item
            print("path:", path)
            print("new_path", new_path)
            BS = path + "\\" + "BeforeSitu"
            if os.path.isdir(new_path) and item.startswith("7."):
                if not os.path.exists(BS):
                    os.makedirs(BS)
                shutil.copytree(new_path, BS)


# src_path 폴더 안의 모든 파일 옮기기
def copy_dir(src_path, dst_path):
    for item in os.listdir(src_path):
        # Construct full paths for the source and destination items
        src_item_path = os.path.join(src_path, item)
        dst_item_path = os.path.join(dst_path, item)

        if os.path.isdir(src_item_path):
            # If the item is a directory, recursively copy its contents
            shutil.move(src_item_path, dst_item_path)
        else:
            # If the item is a file, copy it to the destination
            shutil.copy2(src_item_path, dst_item_path)


# buildprocess 내 파일 분류 정리
def into_situ(src_path):
    dir_paths = []
    for item in os.listdir(src_path):
        item_path = os.path.join(src_path, item)
        if os.path.isdir(item_path):
            dir_paths.append(item_path)
    for path in dir_paths:
        for item in os.listdir(path):
            new_path = path +"\\"+ item
            # print("path:",path)
            print("new_path",new_path)
            AS = path + "\\" + "AfterSitu"
            BS = path + "\\" + "BeforeSitu"
            IS = path + "\\" + "InSitu"
            if os.path.isdir(new_path) and item.startswith("1."):
                if not os.path.exists(BS):
                    os.makedirs(BS)
                shutil.move(new_path, BS)
            elif os.path.isdir(new_path) and item.startswith("2."):
                if not os.path.exists(AS):
                    os.makedirs(AS)
                shutil.move(new_path, AS)
            elif os.path.isdir(new_path) and item.startswith("3."):
                if not os.path.exists(AS):
                    os.makedirs(AS)
                shutil.move(new_path, AS)
            elif os.path.isdir(new_path) and item.startswith("4."):
                if not os.path.exists(AS):
                    os.makedirs(AS)
                shutil.move(new_path, AS)
            elif os.path.isdir(new_path) and item.startswith("5."):
                if not os.path.exists(AS):
                    os.makedirs(AS)
                shutil.move(new_path, AS)
            elif os.path.isdir(new_path) and item.startswith("6."):
                if not os.path.exists(AS):
                    os.makedirs(AS)
                shutil.move(new_path, AS)
            elif os.path.isdir(new_path) and item.startswith("7."):
                shutil.rmtree(new_path)
            elif os.path.isdir(new_path) and item.startswith("8."):
                if not os.path.exists(IS):
                    os.makedirs(IS)
                shutil.move(new_path, IS)
            elif os.path.isdir(new_path) and item.startswith("9."):
                if not os.path.exists(IS):
                    os.makedirs(IS)
                shutil.move(new_path, IS)
            elif not os.path.isdir(new_path):
                if not os.path.exists(BS):
                    os.makedirs(BS)
                shutil.move(new_path, BS)
            elif os.path.isdir(new_path) and item.startswith("10."):
                if not os.path.exists(IS):
                    os.makedirs(IS)
                shutil.move(new_path, IS)


# 날짜 패턴에 맞춰 target_path에 directory 생성
def transform_dir():
    # 파일 선택 대화 상자 열기
    root = Tk()
    root.withdraw()  # Tkinter 창 숨기기
    input_dir_path = filedialog.askdirectory(title="Select a directory")
    root.destroy()  # Tkinter 창 닫기

    # target_path = "C:\\3DP_Data\\Build\\Build Process\\PBF_M160_test\\"
    target_path = "C:\\3DP_Data\Build\Build Strategy\\"

    # pattern = r'\d{8}_\d{4}'
    pattern = r'\d{8}_\d{4}_\d{8}_\d{4}'
    created_directories = find_directories_with_pattern_BS(input_dir_path, pattern, target_path)

    print("Created Directories:")
    for directory in created_directories:
        print(directory)


# 폴더 앞의 숫자 지우기
def remove_num(src_path):
    pattern = re.compile(r'^\d+\.\s?')
    for root, dirs, files in os.walk(src_path):
        for directory in dirs:
            if pattern.match(directory):
                # print(directory)
                # Remove the specified prefix from the directory name
                new_dir_name = re.sub(pattern, '', directory)
                # Rename the directory
                old_path = os.path.join(root, directory)
                new_path = os.path.join(root, new_dir_name)
                os.rename(old_path, new_path)


def get_directories(path):
    # 주어진 경로에 있는 모든 항목을 얻음
    all_contents = os.listdir(path)
    # 디렉토리만 필터링
    directories = [content for content in all_contents if os.path.isdir(os.path.join(path, content))]
    return directories


def delete_folder_in_jobfile(src_path):
    dir_paths = []
    for item in os.listdir(src_path):
        item_path = os.path.join(src_path, item)
        if os.path.isdir(item_path):
            dir_paths.append(item_path)
    for path in dir_paths:
        for item in os.listdir(path):
            if item == "JOB":
                is_item = os.path.join(path, item)
                print("is_item", is_item)
                # print("os.listdir(path):", os.listdir(path))
                print("item :", item)
                print("path:", path)
                job_path = os.path.join(path, item)
                print("job_path:", job_path)
                # sub_folders = [f.name for f in os.scandir(job_path) if f.is_dir()]
                # sub_folders = get_directories(job_path)
                # folder_path = job_path + "\\" + sub_folders[0]
                files = [f for f in os.listdir(job_path) if os.path.isfile(os.path.join(job_path, f))]
                for file in files:
                    print("file:", files)
                    src_path = path + "\\" + file
                    print("src_path:", src_path)
                    if find_files_with_extensions(src_path, ".magics"):
                        dst_path = path + "\\" + "MAGICS" + ".magics"
                        shutil.move(src_path, dst_path)
                        print("dst_path:", dst_path)
                    elif find_files_with_extensions(src_path, ".stl"):
                        dst_path = path + "\\" + "STL" + ".stl"
                        shutil.move(src_path, dst_path)
                        print("dst_path:", dst_path)
                # print("subfolder:", sub_folders)
                # # JOB 밑에 하위폴더 있으면 내용물 꺼내고 폴더 삭제
                # if sub_folders:
                #     folder_path = job_path + "\\" + sub_folders[0]
                #     files = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
                #     # 파일을 해당 폴더와 같은 레벨로 옮깁니다.
                #     for file_name in files:
                #         print("file_name", file_name)
                #         src_path = os.path.join(folder_path, file_name)
                #         dest_path = os.path.join(os.path.dirname(folder_path), file_name)
                #         # print("src",src_path)
                #         # print("dst",dest_path)
                #         shutil.move(src_path, dest_path)
                #     os.rmdir(folder_path)
                # old_path = path + "\\" + 'JOB'
                # os.rename(job_path, old_path)

            # else:
            #     job_path = os.path.join(path, item)
            #     dst_path = path + "\\" + 'JOB' + "\\" + item
            #     print("job_path:",job_path)
            #     print("dst_path:", dst_path)
            #     shutil.move(job_path, dst_path)


def find_files_with_extensions(directory, extensions):
    matching_files = []

    # 주어진 디렉토리에서 모든 파일 목록 가져오기
    all_files = os.listdir(directory)

    # 주어진 확장자들과 일치하는 파일 찾기
    for file_name in all_files:
        if any(file_name.endswith(ext) for ext in extensions):
            matching_files.append(os.path.join(directory, file_name))

    return matching_files


def upload_directory_bp(minio_client, bucket_name, local_dir, base_prefix):
    """
    지정된 로컬 디렉토리를 MinIO 버킷에 업로드합니다.
    특정 조건에 따라 폴더 이름에서 '숫자.' 패턴을 제거하고, 특정 폴더는 무시합니다.
    :param minio_client: Minio 클라이언트 객체
    :param bucket_name: MinIO 버킷 이름
    :param local_dir: 로컬 디렉토리 경로
    :param base_prefix: 버킷 내 타겟 디렉토리 경로
    """
    pattern = re.compile(r'\d+\.\s*')  # '숫자.'와 선택적 공백을 찾는 정규 표현식
    situpattern = re.compile(r'^(\d+)\.')  # 시작하는 숫자 패턴 찾기

    for root, dirs, files in os.walk(local_dir):
        for filename in files:
            # 로컬 파일의 전체 경로
            local_path = os.path.join(root, filename)
            # MinIO 내 저장될 경로
            relative_path = os.path.relpath(local_path, local_dir)
            
            if root == local_dir:
                relative_path = os.path.join('BeforeSitu', relative_path)
            else:
                # 숫자 패턴에 따라 폴더 분류
                match = situpattern.search(relative_path)
                if match:
                    number = int(match.group(1))
                    if number == 1:
                        subdir = 'BeforeSitu/'
                    elif 2 <= number <= 6:
                        subdir = 'AfterSitu/'
                    elif 8 <= number <= 10:
                        subdir = 'InSitu/'
                    elif number == 7:
                        continue  # 7로 시작하는 폴더는 무시
                    # 분류 후 패턴 제거
                    relative_path = pattern.sub('', relative_path, 1)
                    relative_path = os.path.join(subdir, relative_path)
                else:
                    relative_path = pattern.sub('', relative_path)  # 패턴 없는 경우 그대로 제거
            # 'Vision Data' 폴더 내 파일에 대한 특수 처리
            if 'Vision Data' in relative_path:
                if '_FirstShot' in filename:
                    relative_path = relative_path.replace('Vision Data', 'Vision/Scanning')
                elif '_SecondShot' in filename:
                    relative_path = relative_path.replace('Vision Data', 'Vision/Desposition')

            minio_path = os.path.join(base_prefix, relative_path).replace("\\", "/")
            # 파일을 MinIO에 업로드
            minio_client.fput_object(bucket_name, minio_path, local_path)
            print(f"Uploaded {local_path} to {minio_path}")

    
def upload_directory_bs(minio_client, bucket_name, local_dir, base_prefix):
    """
    지정된 로컬 디렉토리 아래에 있는 파일들을 MinIO 버킷의 'BS/<folder_name>/'에 업로드합니다.
    특정 폴더 이름을 새로운 이름으로 매핑합니다.
    
    :param minio_client: Minio 클라이언트 객체
    :param bucket_name: MinIO 버킷 이름
    :param local_dir: 로컬 디렉토리 경로
    :param base_prefix: 버킷 내 기본 경로 ('BS/')
    """
    name_mapping = {
        'JOB': 'MachineCode',
        'STL': 'BS_Model',
        'Magics': 'Project'
    }
    for root, dirs, files in os.walk(local_dir):
        folder_name = os.path.basename(root).split('_')[-1]
        # 이름 매핑에 따라 폴더 이름 변경
        folder_name = name_mapping.get(folder_name, folder_name)

        for filename in files:
            local_path = os.path.join(root, filename)
            # 파일이 저장될 MinIO 경로
            minio_path = os.path.join(base_prefix, folder_name, filename).replace("\\", "/")
            # 파일을 MinIO에 업로드
            minio_client.fput_object(bucket_name, minio_path, local_path)
            print(f"Uploaded {local_path} to {minio_path}")


# bp 데이터 minio에 넣기
def migrate_minio_bp():
    # 디렉토리 경로 설정
    # directory = 'C:\\Users\\wonch\\Desktop\\456'
    directory = 'D:\\NC_Backup\\3DP_Data_M160\\공정'
    for folder in os.listdir(directory):
        # 폴더 이름이 정규 표현식 패턴과 일치하는지 확인
        match = re.match(r'(\d+_\d+)_(\d+_\d+)_([-\w]+?)_([-\w]+?)_([-\w]+)', folder)
        if match:
            # 변수에 패턴 매칭 결과 저장
            bp_datetime, bs_create_date, bs_name = match.groups()[:3]

            # bs이름, 날짜 조회해서 있으면 bs안넣고 id값 반환, 없으면 넣고 id반환
            query = f""" 
                    SELECT "BS_ID" FROM keti."BS"
                    WHERE "BS_NAME" = '{bs_name}'
                    AND "BS_CREATE_DATETIME" = '{bs_create_date}'
                    LIMIT 1
                    """ 
            cur.execute(query)
            exist = cur.fetchone()
            if exist:
                bs_id = exist[0]
            else:
                query_insert_bs = """
                        INSERT INTO keti."BS" (\"BS_NAME\", \"BS_CREATE_DATETIME\", \"DESIGN_ID_LIST\")
                        VALUES (%s, %s, %s)
                        RETURNING "BS_ID"
                        """ 
                cur.execute(query_insert_bs, (bs_name, bs_create_date, [1]))
                bs_id = cur.fetchone()[0]

            query_select_bp = f""" 
                    SELECT "BP_ID" FROM keti."BP_2"
                    WHERE "BP_WORKER" = %s
                    AND "BP_DATETIME" = %s
                    AND "BS_ID" = %s
                    AND "MATERIAL_ID" = %s
                    LIMIT 1
                    """ 
            cur.execute(query_select_bp, ('박종선', bp_datetime, bs_id, 1))
            bp_id = cur.fetchone()
            if bp_id:
                bp_id = bp_id[0]
            else:
                query_insert_bp = """
                        INSERT INTO keti."BP_2" (\"BP_WORKER\", \"BP_DATETIME\", \"BS_ID\", \"MATERIAL_ID\")
                        VALUES (%s, %s, %s, %s)
                        RETURNING "BP_ID"
                        """ 
                cur.execute(query_insert_bp, ('박종선', bp_datetime, bs_id, 1))
                bp_id = cur.fetchone()[0]
        
            # 버킷 이름과 프리픽스 설정
            bucket_name = "keti"
            # local_directory = f'C:\\Users\\wonch\\Desktop\\456\\{folder}' 
            local_directory = f'D:\\NC_Backup\\3DP_Data_M160\\공정\\{folder}'
            base_prefix = f"Build\\Build Process\\2\\{bp_id}"

            upload_directory_bp(client, bucket_name, local_directory, base_prefix)
    conn.commit()
    print("finish")
    cur.close()
    conn.close()


# bs 데이터 minio에 넣기
def migrate_minio_bs():
    # 디렉토리 경로 설정
    # directory = 'C:\\Users\\wonch\\Desktop\\789' 
    directory = 'C:\\Users\\wonch\\PycharmProjects\\MinIoFileDownload\\BS'

    # 디렉토리 내 모든 항목에 대해 반복
    for folder in os.listdir(directory):
        # 폴더 이름이 정규 표현식 패턴과 일치하는지 확인
        pattern = re.compile(r'^(\d{8}_\d{4})_(.+)$')
        match = pattern.match(folder)
        if match:
            # 변수에 패턴 매칭 결과 저장
            bs_create_date = match.group(1)
            bs_name = match.group(2)

            # bs이름, 날짜 조회해서 있으면 bs안넣고 id값 반환, 없으면 넣고 id반환
            query = f""" 
                    SELECT "BS_ID" FROM hbnu."BS"
                    WHERE "BS_NAME" = '{bs_name}'
                    AND "BS_CREATE_DATETIME" = '{bs_create_date}'
                    LIMIT 1
                    """ 
            cur.execute(query)
            bs_id = cur.fetchone()

            if bs_id:
                bs_id = bs_id[0]
            else:
                query_insert_bs = """
                        INSERT INTO hbnu."BS" (\"BS_NAME\", \"BS_CREATE_DATETIME\", \"DESIGN_ID_LIST\")
                        VALUES (%s, %s, %s)
                        RETURNING "BS_ID"
                        """ 
                cur.execute(query_insert_bs, (bs_name, bs_create_date, [1]))
                bs_id = cur.fetchone()[0]
            
            # # 버킷 이름과 프리픽스 설정
            bucket_name = "hbnu"
            # local_directory = f'C:\\Users\\wonch\\Desktop\\789\\{folder}' 
            local_directory = f'C:\\Users\\wonch\\PycharmProjects\\MinIoFileDownload\\BS\\{folder}'
            base_prefix = f"Build\\Build Strategy\\{bs_id}"

            upload_directory_bs(client, bucket_name, local_directory, base_prefix)

    conn.commit()
    print("finish")
    cur.close()
    conn.close()

# mongodb 와 postgresql 의 bp_datetime 에 오차가 있어 오차범위 내의 mongodb값으로 바꿔주고 해당 작업자와 TOTAL_LAYER값 가져오기
def migrate_mongo_to_minio():
    mongo_client = MongoClient(host='keties.iptime.org', port=50002,
                                          username='KETI_root',
                                          password='madcoder',
                                          authSource='admin',
                                          authMechanism='SCRAM-SHA-1')
    query = f""" 
            SELECT "BP_ID", "BP_DATETIME" FROM hbnu."BP_2"
            """ 
    cur.execute(query)
    bp_datetimes = cur.fetchall()

    # 정규 표현식 패턴
    regex = re.compile(r"^\d{8}_\d{4}_[DM]160$")
    # 모든 데이터베이스 이름 가져오기
    databases = mongo_client.list_database_names()
    filtered_dbs= []
    for db in databases:
        match = regex.match(db)
        if match:
            # '_D160'을 제거하고 숫자만 저장
            clean_name = db[:-5]  # 작은따옴표와 '_D160'을 제외하고 슬라이싱
            # datetime_obj = datetime.strptime(clean_name, '%Y%m%d_%H%M')
            filtered_dbs.append(clean_name)

    matching_values = [] # Mongodb 의 매칭값
    # 마지막 4자리 숫자만 추출하여 저장
    # time_values = []
    for (id, bp_datetime) in bp_datetimes:  # Postgresql 각 튜플에서 bp_datetime 값을 추출
        datetime_obj = datetime.strptime(bp_datetime, "%Y%m%d_%H%M")
        time_range = [datetime_obj + timedelta(minutes=i) for i in range(-5, 6)]
        formatted_time_range = [time.strftime("%Y%m%d_%H%M") for time in time_range]

        matching_value = next((value for value in filtered_dbs if value in formatted_time_range), None)
        # # 결과 출력
        if matching_value: # Mongodb 매칭값
            matching_values.append(matching_value)
            update_query = """UPDATE hbnu."BP_2" SET "BP_DATETIME" = %s WHERE "BP_ID" = %s"""
            cur.execute(update_query, (matching_value, id))
            conn.commit()


    # 데이터베이스 이름이 matching_values의 요소로 시작하는지 확인
    matched_databases = [db for db in mongo_client.list_database_names() if any(db.startswith(prefix) 
                         for prefix in matching_values)]

    results = {}
    # 각 매칭된 데이터베이스 이름에 대하여
    for dbname in matched_databases:
        # 해당 데이터베이스 선택
        db = mongo_client[dbname]
        
        # Jobinfo 컬렉션이 존재하는지 확인
        if "JobInfo" in db.list_collection_names():
            # Jobinfo 컬렉션의 모든 문서 조회
            jobinfo_collection = db["JobInfo"]
            user_found = False
            total_layer_found = False

            for document in jobinfo_collection.find({}, {"User": 1, "TotalLayer": 1}):
                # User와 TotalLayer를 각각 최초 한 번만 저장
                if "User" in document and not user_found:
                    user = document["User"]
                    user_found = True
                if "TotalLayer" in document and not total_layer_found:
                    total_layer = document["TotalLayer"]
                    total_layer_found = True
                
                # 두 필드 모두 찾았으면 반복 중단
                if user_found and total_layer_found:
                    break
            
            if user_found or total_layer_found:
                results[dbname] = {}
                if user_found:
                    results[dbname]["User"] = user
                if total_layer_found:
                    results[dbname]["TotalLayer"] = total_layer

    for dbname, info in results.items():
        if "User" in info:
            print(f"  User: {info['User']}")
        if "TotalLayer" in info:
            print(f"  TotalLayer: {info['TotalLayer']}")
        pattern = re.compile(r"(\d{8})_(\d{4})_[DM]160")
        match = pattern.match(dbname)

        if match:
            date_part = match.group(1)  # 8자리 날짜 부분
            time_part = match.group(2)  # 4자리 시간 부분
            match_datetime = f"{date_part}_{time_part}"
        
        if match_datetime:
            user = info.get('User', 'DefaultUser')
            total_layer = info.get('TotalLayer', 0)
        
        query = f""" 
                UPDATE hbnu."BP_2" SET "BP_WORKER" = '{user}', 
                "TOTAL_LAYER" = {total_layer}
                WHERE "BP_DATETIME" = '{match_datetime}'           
                """ 
        cur.execute(query)
        # bp_datetimes = cur.fetchall()

    conn.commit()
    print("finish")
    cur.close()
    conn.close()


    
    















    
def rename_folder_in_minio(bucket_name, old_prefix, new_prefix):
    # old_prefix로 시작하는 모든 객체를 찾음
    objects = client.list_objects(bucket_name, prefix=old_prefix, recursive=True)
    
    for obj in objects:
        old_path = obj.object_name
        new_path = obj.object_name.replace(old_prefix, new_prefix, 1)  # old_prefix를 new_prefix로 변경
        
        # 객체를 새 경로로 복사
        client.copy_object(
            bucket_name,
            new_path,
            f"{bucket_name}/{old_path}"
        )
        
        # 원본 객체 삭제
        client.remove_object(bucket_name, old_path)
        
        print(f"Moved '{old_path}' to '{new_path}'")

# 사용 예시: 'mybucket' 버킷에서 'a/' 폴더를 'b/'로 변경
# rename_folder_in_minio("hbnu", "Desposition/", "Deposition/")