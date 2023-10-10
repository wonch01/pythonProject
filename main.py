from tkinter import *
from tkinter import filedialog
from PIL import ImageTk, Image, ImageDraw
import csv

window = Tk()
window.geometry("800x800")
window.option_add("*Font", "맑은고딕 25")
window.title("파일부르기")

my_image = None
draw = None
canvas = None

csv_data = None
text_widget = None

def open_file():
    global my_image, draw, canvas, csv_data, text_widget
    filename = filedialog.askopenfilename(initialdir='D:\1\스샷',
                                          title='파일선택',
                                          filetypes=(('Image files', '*.png *.jpg'),
                                                     ('CSV files', '*.csv'),
                                                     ('All files', '*.*')))
    if filename:
        file_extension = filename.split('.')[-1]
        if file_extension in ['png', 'jpg']:
            Label(window, text=filename).pack()
            image = Image.open(filename)
            image = image.resize((700, 700), Image.BILINEAR)
            my_image = ImageTk.PhotoImage(image)
            canvas.create_image(0, 0, anchor=NW, image=my_image)
            draw = ImageDraw.Draw(image)

        elif file_extension == 'csv':
            text_widget = Text(window, wrap=NONE)
            text_widget.pack()
            # CSV 파일 읽어오기
            with open(filename, 'r',encoding='utf-8') as file:
                csv_data = file.read()
            # CSV 텍스트를 Text 위젯에 표시
            if text_widget:
                text_widget.delete(1.0, END)
                text_widget.insert(END, csv_data)


def start_draw(event):
    global last_x, last_y
    last_x, last_y = event.x, event.y

def draw_line(event):
    global last_x, last_y
    x, y = event.x, event.y
    canvas.create_line((last_x, last_y, x, y), fill="red", width=5)
    draw.line([(last_x, last_y), (x, y)], fill="red", width=5)
    last_x, last_y = x, y


def save_file():
        filename = filedialog.asksaveasfilename(defaultextension=".png",
                                                filetypes=[("PNG 파일", "*.png")])
        # if filename:
        #     my_image_to_save = Image.open(filename)
        #     my_image_to_save.save(filename)

def close_window():
    window.quit()
    window.destroy()

# 메뉴 생성
menubar = Menu(window)

# 파일 메뉴 생성
menu_file = Menu(menubar, tearoff=0)
menu_file.add_command(label="New")
menu_file.add_command(label="Open", command=open_file)
menu_file.add_command(label="Save", command=save_file)
menu_file.add_separator()
menu_file.add_command(label="Exit", command=close_window)

# 파일 메뉴를 메뉴바에 추가
menubar.add_cascade(label="File", menu=menu_file)

# 윈도우에 메뉴 설정
window.config(menu=menubar)

# Canvas 생성
canvas = Canvas(window, width=700, height=700)
canvas.pack()

# Canvas 마우스 이벤트 바인딩
canvas.bind("<Button-1>", start_draw)
canvas.bind("<B1-Motion>", draw_line)

# btn_save = Button(window, text='저장', command=save_file)
# btn_save.pack()

# 텍스트 위젯 생성
# text_widget = Text(window, wrap=NONE)
# text_widget.pack()

window.mainloop()