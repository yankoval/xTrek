import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext
import subprocess
import os
import sys
import tempfile


class IntersectGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("xTrek File Processor")
        self.root.geometry("1400x900")

        # Временные файлы
        self.temp_files = []

        # Переменные для хранения путей к файлам
        self.file1_path = tk.StringVar()
        self.file2_path = tk.StringVar()
        self.output_path = tk.StringVar()

        # Переменные для опций
        self.separator_var = tk.StringVar(value="93")
        self.unicode_escape_var = tk.BooleanVar()
        self.json_serialize_var = tk.BooleanVar()
        self.verbose_var = tk.BooleanVar()

        # Окно для вывода результата
        self.output_window = None

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
        tk.Button(file1_frame, text="Просмотр", command=lambda: self.preview_file(self.file1_path)).pack(side=tk.LEFT,
                                                                                                         padx=5)
        tk.Button(file1_frame, text="Вставить из буфера",
                  command=lambda: self.paste_from_clipboard(self.file1_text)).pack(side=tk.LEFT, padx=5)

        # File 2 selection
        file2_frame = tk.Frame(top_frame)
        file2_frame.pack(fill=tk.X, pady=5)

        tk.Label(file2_frame, text="Файл для обработки:").pack(side=tk.LEFT)
        tk.Entry(file2_frame, textvariable=self.file2_path, width=50).pack(side=tk.LEFT, padx=5)
        tk.Button(file2_frame, text="Обзор", command=self.browse_file2).pack(side=tk.LEFT)
        tk.Button(file2_frame, text="Просмотр", command=lambda: self.preview_file(self.file2_path)).pack(side=tk.LEFT,
                                                                                                         padx=5)
        tk.Button(file2_frame, text="Вставить из буфера",
                  command=lambda: self.paste_from_clipboard(self.file2_text)).pack(side=tk.LEFT, padx=5)

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

        tk.Checkbutton(options_frame, text="Unicode-escape", variable=self.unicode_escape_var).grid(row=0, column=2,
                                                                                                    padx=10)
        tk.Checkbutton(options_frame, text="JSON-сериализация", variable=self.json_serialize_var).grid(row=0, column=3,
                                                                                                       padx=10)
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
        tk.Button(button_frame, text="Показать результат", command=self.show_result_output).pack(side=tk.LEFT, padx=10)
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

    def paste_from_clipboard(self, text_widget):
        try:
            clipboard_content = self.root.clipboard_get()
            text_widget.delete(1.0, tk.END)
            text_widget.insert(1.0, clipboard_content)
            self.status_var.set("Текст вставлен из буфера обмена")
        except tk.TclError:
            messagebox.showinfo("Информация", "Буфер обмена пуст или содержит не текстовые данные")
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось вставить из буфера: {e}")

    def preview_file(self, file_path_var):
        if file_path_var.get():
            self.load_file_preview(file_path_var.get(),
                                   self.file1_text if file_path_var == self.file1_path else self.file2_text)
        else:
            messagebox.showwarning("Предупреждение", "Сначала выберите файл")

    def load_file_preview(self, filename, text_widget):
        try:
            # Пробуем разные кодировки
            encodings = ['utf-8', 'cp1251', 'latin-1', 'iso-8859-1']
            content = None

            for encoding in encodings:
                try:
                    with open(filename, 'r', encoding=encoding) as file:
                        content = file.read()
                    break
                except UnicodeDecodeError:
                    continue

            if content is None:
                # Если ни одна кодировка не подошла, читаем как бинарный файл
                with open(filename, 'rb') as file:
                    content = file.read().decode('utf-8', errors='replace')

            text_widget.delete(1.0, tk.END)
            text_widget.insert(1.0, content)
            self.status_var.set(f"Загружен файл: {os.path.basename(filename)}")
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось загрузить файл: {e}")

    def refresh_previews(self):
        if self.file1_path.get():
            self.load_file_preview(self.file1_path.get(), self.file1_text)
        if self.file2_path.get():
            self.load_file_preview(self.file2_path.get(), self.output_text)
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

        # Очистка временных файлов
        self.cleanup_temp_files()

        self.status_var.set("Очищено")

    def cleanup_temp_files(self):
        """Удаление временных файлов"""
        for temp_file in self.temp_files:
            try:
                if os.path.exists(temp_file):
                    os.unlink(temp_file)
            except:
                pass
        self.temp_files = []

    def create_temp_file(self, content, prefix="intersect_temp"):
        """Создание временного файла с содержимым"""
        try:
            with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8',
                                             prefix=prefix, suffix='.txt',
                                             delete=False) as temp_file:
                temp_file.write(content)
                self.temp_files.append(temp_file.name)
                return temp_file.name
        except Exception as e:
            raise Exception(f"Не удалось создать временный файл: {e}")

    def show_help(self):
        help_text = """GUI File Processor - графический интерфейс для intersect.py

Использование:
1. Выберите файл с паттернами (первый файл) ИЛИ вставьте текст в окно
2. Выберите файл для обработки (второй файл) ИЛИ вставьте текст в окно
3. Укажите путь для выходного файла
4. Настройте опции при необходимости
5. Нажмите "Запуск"

Особенности:
- Поддержка вставки текста из буфера обмена
- Автоматическое определение кодировки файлов
- Немодальное окно результатов
- Автоматическая очистка временных файлов

Опции:
- Разделитель: символ для разделения строк в первом файле
- Unicode-escape: преобразование Unicode символов в escape-последовательности
- JSON-сериализация: сериализация паттернов по правилам JSON
- Подробный вывод: детальная информация о процессе"""

        help_window = tk.Toplevel(self.root)
        help_window.title("Справка")
        help_window.geometry("600x400")

        text_widget = scrolledtext.ScrolledText(help_window, wrap=tk.WORD)
        text_widget.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        text_widget.insert(1.0, help_text)
        text_widget.config(state=tk.DISABLED)

    def show_result_output(self):
        """Показать окно с результатом выполнения"""
        if self.output_window is not None and self.output_window.winfo_exists():
            self.output_window.lift()
            self.output_window.focus()
        else:
            self.create_output_window()

    def create_output_window(self):
        """Создать окно для вывода результатов"""
        if self.output_window is not None and self.output_window.winfo_exists():
            self.output_window.destroy()

        self.output_window = tk.Toplevel(self.root)
        self.output_window.title("Результат выполнения")
        self.output_window.geometry("800x400")
        self.output_window.protocol("WM_DELETE_WINDOW", self.on_output_window_close)

        text_widget = scrolledtext.ScrolledText(self.output_window, wrap=tk.WORD)
        text_widget.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        text_widget.config(state=tk.DISABLED)

        # Сохраняем ссылку на виджет
        self.result_text_widget = text_widget

    def on_output_window_close(self):
        """Обработчик закрытия окна результатов"""
        self.output_window.destroy()
        self.output_window = None
        self.result_text_widget = None

    def update_result_output(self, text):
        """Обновить содержимое окна результатов"""
        if self.output_window is None or not self.output_window.winfo_exists():
            self.create_output_window()

        self.result_text_widget.config(state=tk.NORMAL)
        self.result_text_widget.delete(1.0, tk.END)
        self.result_text_widget.insert(1.0, text)
        self.result_text_widget.config(state=tk.DISABLED)

    def run_intersect(self):
        # Проверка обязательных полей
        file1_content = self.file1_text.get(1.0, tk.END).strip()
        file2_content = self.file2_text.get(1.0, tk.END).strip()

        if not file1_content and not self.file1_path.get():
            messagebox.showerror("Ошибка", "Введите паттерны или выберите файл с паттернами")
            return

        if not file2_content and not self.file2_path.get():
            messagebox.showerror("Ошибка", "Введите текст для обработки или выберите файл")
            return

        if not self.output_path.get():
            messagebox.showerror("Ошибка", "Укажите выходной файл")
            return

        # Очистка предыдущих временных файлов
        self.cleanup_temp_files()

        # Создание временных файлов при необходимости
        try:
            if file1_content and not self.file1_path.get():
                temp_file1 = self.create_temp_file(file1_content, "patterns_")
                file1_to_use = temp_file1
            else:
                file1_to_use = self.file1_path.get()

            if file2_content and not self.file2_path.get():
                temp_file2 = self.create_temp_file(file2_content, "data_")
                file2_to_use = temp_file2
            else:
                file2_to_use = self.file2_path.get()

        except Exception as e:
            messagebox.showerror("Ошибка", str(e))
            return

        # Построение команды
        cmd = [
            sys.executable, "intersect.py",
            file1_to_use,
            file2_to_use,
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
            result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8',
                                    cwd=os.path.dirname(os.path.abspath(__file__)))

            output_text = ""
            if result.stdout:
                output_text += "СТАНДАРТНЫЙ ВЫВОД:\n" + result.stdout + "\n" + "=" * 50 + "\n"

            if result.stderr:
                output_text += "ОШИБКИ:\n" + result.stderr

            if result.returncode == 0:
                self.status_var.set("Обработка завершена успешно")

                # Обновление просмотра выходного файла
                if os.path.exists(self.output_path.get()):
                    self.load_file_preview(self.output_path.get(), self.output_text)

                # Показать вывод программы в немодальном окне
                self.update_result_output(output_text)
                self.show_result_output()

            else:
                self.status_var.set("Ошибка выполнения")
                self.update_result_output(output_text)
                self.show_result_output()

        except Exception as e:
            error_msg = f"Не удалось запустить программу: {e}"
            messagebox.showerror("Ошибка", error_msg)
            self.status_var.set("Ошибка запуска")
            self.update_result_output(error_msg)
            self.show_result_output()

        finally:
            # Очистка временных файлов
            self.cleanup_temp_files()


def main():
    root = tk.Tk()
    app = IntersectGUI(root)

    # Очистка временных файлов при закрытии
    def on_closing():
        app.cleanup_temp_files()
        root.destroy()

    root.protocol("WM_DELETE_WINDOW", on_closing)
    root.mainloop()


if __name__ == "__main__":
    main()