import os
import re
BOT_TOKEN = os.getenv("SERVICEBOT_TOKEN")

# ========== НАСТРОЙКИ НОМЕРОВ ==========

# Полное соответствие английских букв русским
ENG_TO_RUS = {
    # Полное соответствие по стандарту РФ
    'A': 'А',  # Латинская A → Русская А
    'B': 'В',  # Латинская B → Русская В
    'C': 'С',  # Латинская C → Русская С
    'E': 'Е',  # Латинская E → Русская Е
    'H': 'Н',  # Латинская H → Русская Н (важно!)
    'K': 'К',  # Латинская K → Русская К
    'M': 'М',  # Латинская M → Русская М
    'O': 'О',  # Латинская O → Русская О
    'P': 'Р',  # Латинская P → Русская Р
    'T': 'Т',  # Латинская T → Русская Т
    'X': 'Х',  # Латинская X → Русская Х (важно!)
    'Y': 'У',  # Латинская Y → Русская У (важно!)
}

# Русские буквы, которые используются в номерах РФ
RUS_LETTERS = "АВЕКМНОРСТУХ"

# Обратное соответствие (русские → английские, для отладки)
@@ -101,50 +102,56 @@ def normalize_car_number(text: str) -> str:
        return ""
    
    # 1. Приводим к верхнему регистру
    text = text.strip().upper()
    
    # 2. Удаляем все пробелы, дефисы и другие разделители
    text = text.replace(' ', '').replace('-', '').replace('_', '')
    
    # 3. Заменяем английские буквы на русские
    result = []
    for char in text:
        # Если это английская буква из нашего словаря - заменяем
        if char in ENG_TO_RUS:
            result.append(ENG_TO_RUS[char])
        else:
            # Иначе оставляем как есть (русские буквы, цифры)
            result.append(char)
    
    normalized = ''.join(result)
    
    # 4. Удаляем ВСЕ символы, кроме разрешённых русских букв и цифр
    # Разрешены только буквы из RUS_LETTERS и цифры
    allowed_chars = RUS_LETTERS + '0123456789'
    normalized = ''.join([c for c in normalized if c in allowed_chars])
    
    # Спец-формат: 3 буквы + 3 цифры (например, ВКК044 -> В044КК797)
    compact_three_letters = f'^[{RUS_LETTERS}]{{3}}\d{{3}}$'
    if re.match(compact_three_letters, normalized):
        normalized = f"{normalized[0]}{normalized[3:6]}{normalized[1:3]}{DEFAULT_REGION}"
        return normalized

    # 5. Автодобавление региона если нужно
    # Формат номера РФ: буква-цифра-цифра-цифра-буква-буква
    # Пример: А123ВС777
    
    # Считаем количество букв и цифр
    letters = sum(1 for c in normalized if c in RUS_LETTERS)
    digits = sum(1 for c in normalized if c.isdigit())
    
    # Если есть хотя бы 3 цифры и 3 буквы - считаем, что номер полный
    if digits >= 3 and letters >= 3:
        # Убедимся, что цифр ровно 6 (3 в номере + 3 в регионе)
        if digits < 6:
            # Добавляем недостающие цифры из региона
            missing_digits = 6 - digits
            normalized += DEFAULT_REGION[:missing_digits]
        return normalized
    
    # Если номер короткий (только основная часть)
    # Пример: 'Х340РУ' → добавляем регион
    if len(normalized) <= 6:
        normalized += DEFAULT_REGION
    
    return normalized

def validate_car_number(text: str) -> tuple[bool, str, str]:
