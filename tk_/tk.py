import tkinter as tk
from tkinter import filedialog, messagebox
import os

def select_file_with_prompt(prompt_text="Выберите файл"):
    """
    Открывает диалоговое окно для выбора файла с заданным текстом
    
    Args:
        prompt_text (str): Текст, который будет отображен в диалоговом окне
    
    Returns:
        str or None: Путь к выбранному файлу или None, если файл не выбран
    """
    # Создаем скрытое окно tkinter
    root = tk.Tk()
    root.withdraw()  # Скрываем основное окно
    root.attributes('-topmost', True)  
    
    try:

        file_path = filedialog.askopenfilename(
            title=prompt_text,
            filetypes=[
                ("All files", "*.*"),
                ("Python files", "*.py"),
                ("Text files", "*.txt"),
                ("Excel files", "*.xlsx *.xls")
            ]
        )
        

        root.destroy()
        
        if file_path:
            print(f"Выбран файл: {os.path.basename(file_path)}")
            return file_path
        else:
            print("Файл не выбран")
            return None
            
    except Exception as e:
        messagebox.showerror("Ошибка", f"Произошла ошибка: {e}")
        root.destroy()
        return None





