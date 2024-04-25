import os.path
import re
import csv
import pandas as pd
from tkinter import Tk, filedialog
# import MariaDB_to_Postgres

# log파일들 선택해서 데이터 정제해서 csv파일로 저장
def extract_arun():
    pattern = re.compile(r'(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}),(.*?) Z\[(\d+\.\d+)\]')

    root = Tk()
    root.withdraw()
    input_file_paths = filedialog.askopenfilenames(title="Select a file",
                                                 filetypes=[("Text files", "*.txt"), ("All files", "*.*")])
    root.destroy()

    for input_file_path in input_file_paths:
        if not input_file_path:
            print("No file selected. Exiting.")
        else:
            filename = os.path.basename(input_file_path)
            output_csv_path = f'{filename}_output_data.csv'
            with open(output_csv_path, 'w', newline='') as csvfile:
                csvwriter = csv.writer(csvfile)
                # CSV 파일 헤더 작성
                csvwriter.writerow(['time', 'description', 'z value'])
                with open(input_file_path, 'r') as file:
                    for line in file:
                        # print("line : ",line)
                        # 정규식 매칭 수행
                        match = pattern.match(line)
                        if match:
                            timestamp, event, z_value = match.groups()
                            csvwriter.writerow([timestamp, event.strip(), z_value])

            print(f"Data extracted and saved to {output_csv_path}.")


def extract_autopw():
    pattern = re.compile(r'(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}).*?LP\[(\d+)\].*?LP\[(\d+)\]')

    root = Tk()
    root.withdraw()
    input_file_paths = filedialog.askopenfilenames(title="Select a file",
                                                 filetypes=[("All files", "*.*"), ("Text files", "*.txt"),])
    root.destroy()

    for input_file_path in input_file_paths:
        if not input_file_path:
            print("No file selected. Exiting.")
        else:
            filename = os.path.basename(input_file_path)
            output_csv_path = f'{filename}_output_data.csv'

            with open(output_csv_path, 'w', newline='') as csvfile:
                csvwriter = csv.writer(csvfile)
                csvwriter.writerow(['time', 'settingpower', 'realpower'])
                with open(input_file_path, 'r', encoding='SHIFT_JIS') as file:
                    for line in file:
                        match = pattern.match(line)
                        if match:
                            time, settingpower, realpower = match.groups()
                            csvwriter.writerow([time, settingpower, realpower])

            print(f"Data extracted and saved to {output_csv_path}.")


def extract_level():
    pattern = re.compile(r'(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}) Z\[(\d+\.\d+)\] Count\[(\d+)\] Time\[(\d+\.\d+)\].*?\[RC\] lvl\[(\d+)\] RC\[(\d+\.\d+)\] LV\[(-?\d+\.\d+)\]')

    root = Tk()
    root.withdraw()  # Tkinter 창 숨기기
    input_file_paths = filedialog.askopenfilenames(title="Select a file",
                                                 filetypes=[ ("All files", "*.*"), ("Text files", "*.txt")])
    root.destroy()

    for input_file_path in input_file_paths:
        if not input_file_path:
            print("No file selected. Exiting.")
        else:
            filename = os.path.basename(input_file_path)
            output_csv_path = f'{filename}_output_data.csv'
            with open(output_csv_path, 'w', newline='') as csvfile:
                csvwriter = csv.writer(csvfile)
                csvwriter.writerow(['time', 'z', 'count', 'during_time', 'lvl', 'rc', 'lv'])
                with open(input_file_path, 'r') as file:
                    for line in file:
                        match = pattern.match(line)
                        if match:
                            time, z, count, during_time, lvl, rc, lv = match.groups()
                            csvwriter.writerow([time, z, count, during_time, lvl, rc, lv])

            print(f"Data extracted and saved to {output_csv_path}.")


# 선택한 csv 파일들 읽어서 postgresql 에 저장
def read_csv_to_pg():
    pg_conn, pg_cur, pg_engine, schema = MariaDB_to_Postgres.postgres_con('keti_superuser',
                                              'madcoder', 'keties.iptime.org',
                                              '55432', 'data_lincsolution', 'cmet_atomm4000')
    root = Tk()
    root.withdraw()  # Tkinter 창 숨기기
    file_names = filedialog.askopenfilenames(title="Select a file",
                                            initialdir="C:\\Users\wonch\PycharmProjects\djtp_printer_monitoring_db",
                                            filetypes=[("All files", "*.*"), ("Text files", "*.txt"), ])
    root.destroy()

    for file_name in file_names:
        data = pd.read_csv(file_name)
        name = os.path.basename(file_name).split('.')[0]
        if name.startswith('ARun'):
            name = 'ARun'
        elif name.startswith('AutoPw'):
            name = 'AutoPw'
        else:
            name = 'LvlLog'
        data.to_sql(name, pg_engine, index=False, if_exists='append', schema='cmet_atomm4000')
        pg_cur.execute(f"""alter table "cmet_atomm4000"."{name}" 
                        alter column "time" type timestamp using "time"::timestamp;""")
        print(file_name)
        pg_conn.commit()
    MariaDB_to_Postgres.postgres_close(pg_conn, pg_cur)
