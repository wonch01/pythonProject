import os.path
import re
import csv
from tkinter import Tk, filedialog


def extract():
    pattern = re.compile(r'(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}) Z\[(\d+\.\d+)\] Count\[(\d+)\] Time\[(\d+\.\d+)\].*?\[RC\] lvl\[(\d+)\] RC\[(\d+\.\d+)\] LV\[(-?\d+\.\d+)\]')

    # 파일 선택 대화 상자 열기
    root = Tk()
    root.withdraw()  # Tkinter 창 숨기기
    input_file_paths = filedialog.askopenfilenames(title="Select a file",
                                                 filetypes=[ ("All files", "*.*"), ("Text files", "*.txt")])
    root.destroy()  # Tkinter 창 닫기

    for input_file_path in input_file_paths:
        if not input_file_path:
            print("No file selected. Exiting.")
        else:
            filename = os.path.basename(input_file_path)
            # 출력 CSV 파일 경로
            output_csv_path = f'{filename}_output_data.csv'

            # CSV 파일을 쓰기 모드로 열기
            with open(output_csv_path, 'w', newline='') as csvfile:
                # CSV 라이터 생성
                csvwriter = csv.writer(csvfile)
                # CSV 파일 헤더 작성
                csvwriter.writerow(['Time', 'Z', 'Count', 'During_Time', 'LvL', 'RC', 'LV'])
                # 선택한 파일을 읽기 모드로 열기
                with open(input_file_path, 'r') as file:
                    # 파일의 각 줄에 대해 반복
                    for line in file:
                        # 정규식 매칭 수행
                        match = pattern.match(line)
                        if match:
                            # 매칭된 데이터 추출
                            Time, Z, Count, During_Time, LvL, RC, LV = match.groups()
                            # CSV 파일에 데이터 쓰기
                            csvwriter.writerow([Time, Z, Count, During_Time, LvL, RC, LV])

            print(f"Data extracted and saved to {output_csv_path}.")




