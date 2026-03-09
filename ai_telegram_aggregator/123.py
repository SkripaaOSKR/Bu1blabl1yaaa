import os

# =================================================================================
# 🛠 НАСТРОЙКИ (CONFIG)
# =================================================================================

OUTPUT_FILE = "FULL_PROJECT_CONTEXT.txt"

# ✅ [БЕЛЫЙ СПИСОК - РАСШИРЕНИЯ]
# Скрипт берет ТЕКСТ ТОЛЬКО из этих файлов.
WHITELIST_EXTENSIONS = {
    '.py', '.ui', '.qss', '.html', '.css', '.js' '.yml' '.env'
}

# 📂 [БЕЛЫЙ СПИСОК - ПАПКИ]
# Если пусто {} -> сканируем весь проект.
# Если заполнено {'handlers'} -> сканируем только эти папки.
WHITELIST_DIRS = {
    # 'handlers',
    # 'database',
}

# ⛔ [ЧЕРНЫЙ СПИСОК - ПАПКИ]
# Игнорируем эти папки (чтобы дерево не зависло на миллионах файлов из venv или .git).
BLACKLIST_DIRS = {
    '.git', '.idea', '__pycache__', 'venv', 'env', 
    'dist', 'build', 'assets', 'media', 'node_modules'
}

# ⛔ [ЧЕРНЫЙ СПИСОК - ФАЙЛЫ]
BLACKLIST_FILES = {
    'collect_project.py', OUTPUT_FILE, '.DS_Store'
}

# =================================================================================
# 🌳 ГЕНЕРАТОР ДЕРЕВА (PROJECT MAP)
# =================================================================================

def get_tree_structure(start_path):
    """Строит визуальное дерево проекта (ПОЛНОЕ)"""
    tree_str = "PROJECT STRUCTURE:\n"
    tree_str += ".\n"  # Корневая точка
    
    # Вспомогательная функция для рекурсии
    def walk(directory, prefix=""):
        nonlocal tree_str
        
        # Получаем список всех элементов в папке
        try:
            entries = sorted(os.listdir(directory))
        except PermissionError:
            return # Пропускаем папки без доступа

        # Фильтруем список
        filtered_entries = []
        for e in entries:
            full_path = os.path.join(directory, e)
            
            # Убираем только мусорные папки (чтобы не парсить venv и .git)
            if os.path.isdir(full_path) and e in BLACKLIST_DIRS:
                continue
            
            # Убираем только конкретные мусорные файлы
            if os.path.isfile(full_path) and e in BLACKLIST_FILES:
                continue
            
            # ВАЖНО: Мы больше не фильтруем дерево по расширениям (WHITELIST_EXTENSIONS),
            # поэтому в карте будут видны все файлы (json, png, md и т.д.)
            
            filtered_entries.append(e)

        # Отрисовываем элементы
        entries_count = len(filtered_entries)
        for index, entry in enumerate(filtered_entries):
            full_path = os.path.join(directory, entry)
            is_last = (index == entries_count - 1)
            
            connector = "└── " if is_last else "├── "
            tree_str += f"{prefix}{connector}{entry}\n"
            
            if os.path.isdir(full_path):
                extension = "    " if is_last else "│   "
                walk(full_path, prefix + extension)

    walk(start_path)
    return tree_str + "\n" + "="*50 + "\n\n"

# =================================================================================
# ⚙️ ОСНОВНАЯ ЛОГИКА
# =================================================================================

def collect_code():
    project_root = os.getcwd()
    
    # Определяем зоны сканирования
    if WHITELIST_DIRS:
        print(f"🎯 Сканируем только папки: {WHITELIST_DIRS}")
        dirs_to_scan = [os.path.join(project_root, d) for d in WHITELIST_DIRS if os.path.exists(d)]
    else:
        print("🌍 Сканируем весь проект")
        dirs_to_scan = [project_root]

    files_count = 0
    
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as outfile:
        # 1. Сначала пишем Дерево Проекта (Карту) 🗺️
        print("🌳 Генерируем карту проекта...")
        # Если сканируем частями, строим дерево для каждой части
        if len(dirs_to_scan) > 1:
             outfile.write("PARTIAL PROJECT MAPS:\n")
             for d in dirs_to_scan:
                 outfile.write(get_tree_structure(d))
        else:
             outfile.write(get_tree_structure(project_root))

        # 2. Теперь собираем контент файлов
        print("📝 Собираем код...")
        
        for start_dir in dirs_to_scan:
            for root, dirs, files in os.walk(start_dir):
                # Фильтр папок (чтобы os.walk не лез в мусор)
                dirs[:] = [d for d in dirs if d not in BLACKLIST_DIRS]
                
                for file in files:
                    if file in BLACKLIST_FILES: continue
                    
                    # А вот здесь мы по-прежнему проверяем расширение,
                    # чтобы не пытаться считывать текст из картинок или скомпилированных файлов.
                    _, ext = os.path.splitext(file)
                    if ext not in WHITELIST_EXTENSIONS: continue
                    
                    file_path = os.path.join(root, file)
                    rel_path = os.path.relpath(file_path, project_root)
                    
                    try:
                        with open(file_path, 'r', encoding='utf-8', errors='ignore') as infile:
                            content = infile.read()
                            
                        # Красивый заголовок для каждого файла
                        outfile.write(f"FILE: {rel_path}\n")
                        outfile.write(f"{'-'*50}\n")
                        outfile.write(content)
                        outfile.write("\n\n" + "="*50 + "\n\n")
                        
                        print(f"✅ Добавлен: {rel_path}")
                        files_count += 1
                    except Exception as e:
                        print(f"❌ Ошибка: {rel_path} - {e}")

    print(f"\n🎉 Готово! Файл '{OUTPUT_FILE}' создан.")
    print(f"📊 В него вошло файлов с кодом: {files_count}")

if __name__ == "__main__":
    collect_code()