import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext
import subprocess
import os
import sys

class IntersectGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("GUI File Processor")
        self.root.geometry("1200x800")
        
        # Переменные для хранения путей к файлам
        self.file1_path = tk.StringVar()
        self.file2_path = tk.StringVar()
        self.output_path = tk.StringVar()
        
        # Переменные для опций
        self.separator_var = tk.StringVar(value="93")
        self.unicode_escape_var = tk.BooleanVar()
        self.json_serialize_var = tk.BooleanVar()
        self.verbose_var = tk.BooleanVar()
        
        self.create_widgets()
        
    def create_widgets(self):
        # Main frame
        main_frame = tk.Frame(self.root, padx=10, pady=10)
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Top frame for file selection
        top_frame = tk.Frame(main_frame)
        top_frame.pack(fill=tk.X, pady=(0, 10))
        
        # File 1 selection
        file1_frame = tk.Frame(top_frame)
        file1_frame.pack(fill=tk.X, pady=5)
        
        tk.Label(file1_frame, text="Файл с паттернами:").pack(side=tk.LEFT)
        tk.Entry(file1_frame, textvariable=self.file1_path, width=50).pack(side=tk.LEFT, padx=5)
        tk.Button(file1_frame, text="Обзор", command=self.browse_file1).pack(side=tk.LEFT)
        tk.Button(file1_frame, text="Просмотр", command=lambda: self.preview_file(self.file1_path)).pack(side=tk.LEFT, padx=5)
        
        # File 2 selection
        file2_frame = tk.Frame(top_frame)
        file2_frame.pack(fill=tk.X, pady=5)
        
        tk.Label(file2_frame, text="Файл для обработки:").pack(side=tk.LEFT)
        tk.Entry(file2_frame, textvariable=self.file2_path, width=50).pack(side=tk.LEFT, padx=5)
        tk.Button(file2_frame, text="Обзор", command=self.browse_file2).pack(side=tk.LEFT)
        tk.Button(file2_frame, text="Просмотр", command=lambda: self.preview_file(self.file2_path)).pack(side=tk.LEFT, padx=5)
        
        # Output file selection
        output_frame = tk.Frame(top_frame)
        output_frame.pack(fill=tk.X, pady=5)
        
        tk.Label(output_frame, text="Выходной файл:").pack(side=tk.LEFT)
        tk.Entry(output_frame, textvariable=self.output_path, width=50).pack(side=tk.LEFT, padx=5)
        tk.Button(output_frame, text="Обзор", command=self.browse_output).pack(side=tk.LEFT)
        
        # Options frame
        options_frame = tk.LabelFrame(main_frame, text="Опции", padx=10, pady=10)
        options_frame.pack(fill=tk.X, pady=(0, 10))
        
        tk.Label(options_frame, text="Разделитель:").grid(row=0, column=0, sticky=tk.W, padx=5)
        tk.Entry(options_frame, textvariable=self.separator_var, width=10).grid(row=0, column=1, padx=5)
        
        tk.Checkbutton(options_frame, text="Unicode-escape", variable=self.unicode_escape_var).grid(row=0, column=2, padx=10)
        tk.Checkbutton(options_frame, text="JSON-сериализация", variable=self.json_serialize_var).grid(row=0, column=3, padx=10)
        tk.Checkbutton(options_frame, text="Подробный вывод", variable=self.verbose_var).grid(row=0, column=4, padx=10)
        
        # Preview frames
        preview_frame = tk.Frame(main_frame)
        preview_frame.pack(fill=tk.BOTH, expand=True)
        
        # File 1 preview
        file1_preview_frame = tk.LabelFrame(preview_frame, text="Просмотр файла с паттернами")
        file1_preview_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5)
        
        self.file1_text = scrolledtext.ScrolledText(file1_preview_frame, height=15, wrap=tk.WORD)
        self.file1_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # File 2 preview
        file2_preview_frame = tk.LabelFrame(preview_frame, text="Просмотр файла для обработки")
        file2_preview_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5)
        
        self.file2_text = scrolledtext.ScrolledText(file2_preview_frame, height=15, wrap=tk.WORD)
        self.file2_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Output preview
        output_preview_frame = tk.LabelFrame(preview_frame, text="Просмотр выходного файла")
        output_preview_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5)
        
        self.output_text = scrolledtext.ScrolledText(output_preview_frame, height=15, wrap=tk.WORD)
        self.output_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Button frame
        button_frame = tk.Frame(main_frame)
        button_frame.pack(fill=tk.X, pady=10)
        
        tk.Button(button_frame, text="Запуск", command=self.run_intersect, 
                 bg="green", fg="white", font=("Arial", 12, "bold")).pack(side=tk.LEFT, padx=10)
        tk.Button(button_frame, text="Очистить", command=self.clear_all, 
                 bg="red", fg="white").pack(side=tk.LEFT, padx=10)
        tk.Button(button_frame, text="Обновить просмотр", command=self.refresh_previews).pack(side=tk.LEFT, padx=10)
        tk.Button(button_frame, text="Справка", command=self.show_help).pack(side=tk.RIGHT, padx=10)
        
        # Status bar
        self.status_var = tk.StringVar(value="Готов к работе")
        status_bar = tk.Label(self.root, textvariable=self.status_var, relief=tk.SUNKEN, anchor=tk.W)
        status_bar.pack(side=tk.BOTTOM, fill=tk.X)
    
    def browse_file1(self):
        filename = filedialog.askopenfilename(
            title="Выберите файл с паттернами",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")]
        )
        if filename:
            self.file1_path.set(filename)
            self.load_file_preview(filename, self.file1_text)
    
    def browse_file2(self):
        filename = filedialog.askopenfilename(
            title="Выберите файл для обработки",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")]
        )
        if filename:
            self.file2_path.set(filename)
            self.load_file_preview(filename, self.file2_text)
    
    def browse_output(self):
        filename = filedialog.asksaveasfilename(
            title="Выберите выходной файл",
            defaultextension=".txt",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")]
        )
        if filename:
            self.output_path.set(filename)
    
    def preview_file(self, file_path_var):
        if file_path_var.get():
            self.load_file_preview(file_path_var.get(), self.file1_text if file_path_var == self.file1_path else self.file2_text)
        else:
            messagebox.showwarning("Предупреждение", "Сначала выберите файл")
    
    def load_file_preview(self, filename, text_widget):
        try:
            with open(filename, 'r', encoding='utf-8') as file:
                content = file.read()
            text_widget.delete(1.0, tk.END)
            text_widget.insert(1.0, content)
            self.status_var.set(f"Загружен файл: {os.path.basename(filename)}")
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось загрузить файл: {e}")
    
    def refresh_previews(self):
        if self.file1_path.get():
            self.load_file_preview(self.file1_path.get(), self.file1_text)
        if self.file2_path.get():
            self.load_file_preview(self.file2_path.get(), self.file2_text)
        if self.output_path.get() and os.path.exists(self.output_path.get()):
            self.load_file_preview(self.output_path.get(), self.output_text)
    
    def clear_all(self):
        self.file1_path.set("")
        self.file2_path.set("")
        self.output_path.set("")
        self.file1_text.delete(1.0, tk.END)
        self.file2_text.delete(1.0, tk.END)
        self.output_text.delete(1.0, tk.END)
        self.separator_var.set("93")
        self.unicode_escape_var.set(False)
        self.json_serialize_var.set(False)
        self.verbose_var.set(False)
        self.status_var.set("Очищено")
    
    def show_help(self):
        help_text = """GUI File Processor - графический интерфейс для intersect.py

Использование:
1. Выберите файл с паттернами (первый файл)
2. Выберите файл для обработки (второй файл)
3. Укажите путь для выходного файла
4. Настройте опции при необходимости
5. Нажмите "Запуск"

Опции:
- Разделитель: символ для разделения строк в первом файле
- Unicode-escape: преобразование Unicode символов в escape-последовательности
- JSON-сериализация: сериализация паттернов по правилам JSON
- Подробный вывод: детальная информация о процессе

Файлы можно просматривать и обновлять с помощью кнопок "Просмотр" и "Обновить просмотр"."""
        
        help_window = tk.Toplevel(self.root)
        help_window.title("Справка")
        help_window.geometry("600x400")
        
        text_widget = scrolledtext.ScrolledText(help_window, wrap=tk.WORD)
        text_widget.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        text_widget.insert(1.0, help_text)
        text_widget.config(state=tk.DISABLED)
    
    def run_intersect(self):
        # Проверка обязательных полей
        if not self.file1_path.get():
            messagebox.showerror("Ошибка", "Выберите файл с паттернами")
            return
        
        if not self.file2_path.get():
            messagebox.showerror("Ошибка", "Выберите файл для обработки")
            return
        
        if not self.output_path.get():
            messagebox.showerror("Ошибка", "Укажите выходной файл")
            return
        
        # Проверка существования файлов
        if not os.path.exists(self.file1_path.get()):
            messagebox.showerror("Ошибка", "Файл с паттернами не существует")
            return
        
        if not os.path.exists(self.file2_path.get()):
            messagebox.showerror("Ошибка", "Файл для обработки не существует")
            return
        
        # Построение команды
        cmd = [
            sys.executable, "intersect.py",
            self.file1_path.get(),
            self.file2_path.get(),
            self.output_path.get()
        ]
        
        # Добавление опций
        if self.separator_var.get() != "93":
            cmd.extend(["--separator", self.separator_var.get()])
        
        if self.unicode_escape_var.get():
            cmd.append("--unicode-escape")
        
        if self.json_serialize_var.get():
            cmd.append("--json-serialize")
        
        if self.verbose_var.get():
            cmd.append("--verbose")
        
        self.status_var.set("Запуск обработки...")
        self.root.update()
        
        try:
            # Запуск процесса
            result = subprocess.run(cmd, capture_output=True, text=True, cwd=os.path.dirname(os.path.abspath(__file__)))
            
            if result.returncode == 0:
                self.status_var.set("Обработка завершена успешно")
                
                # Обновление просмотра выходного файла
                if os.path.exists(self.output_path.get()):
                    self.load_file_preview(self.output_path.get(), self.output_text)
                
                # Показать вывод программы
                self.show_output(result.stdout, "Результат выполнения")
            else:
                self.status_var.set("Ошибка выполнения")
                self.show_output(result.stderr, "Ошибка выполнения")
                
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось запустить программу: {e}")
            self.status_var.set("Ошибка запуска")
    
    def show_output(self, text, title):
        output_window = tk.Toplevel(self.root)
        output_window.title(title)
        output_window.geometry("800x400")
        
        text_widget = scrolledtext.ScrolledText(output_window, wrap=tk.WORD)
        text_widget.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        text_widget.insert(1.0, text)
        text_widget.config(state=tk.DISABLED)

def main():
    root = tk.Tk()
    app = IntersectGUI(root)
    root.mainloop()

if __name__ == "__main__":
    main()