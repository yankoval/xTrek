import sys
import os

# Получаем путь к папке, где лежит текущий файл
current_dir = os.path.dirname(os.path.abspath(__file__))
# Добавляем этот путь в sys.path, если его там нет
if current_dir not in sys.path:
    sys.path.append(current_dir)
from gs1_processor import get_inn_by_gtin

my_gtin = "4670017921436"
owner_inn = get_inn_by_gtin(my_gtin)

if owner_inn == "7733154124":
    print("Это товар ООО 'ФИТОКОСМЕТИК'")
elif owner_inn == "9718180660":
    print("Это товар ООО 'ГЛОБАЛ ЛАЙН'")
print(owner_inn)
# и так далее...