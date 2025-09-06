# install.py - Проверка зависимостей
import sys
import tkinter as tk

def check_dependencies():
    print("Проверка зависимостей...")
    
    # Проверка Python версии
    if sys.version_info < (3, 6):
        print("Ошибка: Требуется Python 3.6 или выше")
        return False
    
    # Проверка tkinter
    try:
        tk.Tk()
        print("✓ tkinter доступен")
    except:
        print("Ошибка: tkinter не доступен. Установите tkinter для вашей системы")
        return False
    
    print("Все зависимости удовлетворены!")
    return True

if __name__ == "__main__":
    if check_dependencies():
        print("Запуск GUI...")
        from intersect_gui import main
        main()
    else:
        input("Нажмите Enter для выхода...")