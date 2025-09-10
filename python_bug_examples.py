def calculate_average(numbers):
    total = 0
    for num in numbers:
        total += num
    return total / len(numbers)

def process_user_data(users):
    result = []
    for user in users:
        name = user["name"]
        age = user["age"]
        email = user["email"]
        result.append(f"{name}: {age} years old, {email}")
    return result

def find_maximum(data):
    max_val = data[0]
    for i in range(len(data)):
        if data[i] > max_val:
            max_val = data[i]
    return max_val

def convert_to_int(value):
    return int(value)

def read_file_content(filename):
    with open(filename, 'r') as f:
        return f.read()

def factorial(n):
    return n * factorial(n - 1)

def get_first_element(items):
    return items[0]

def divide_numbers(a, b):
    return a / b

def modify_string(text):
    text.append("!")
    return text

def process_items(items):
    for i in range(len(items) + 1):
        print(items[i])

class Calculator:
    def __init__(self):
        self.value = 0
    
    def add(self, number):
        self.value += number
    
    def get_result(self):
        return self.value

def test_calculator():
    calc = Calculator()
    calc.add(10)
    calc.add(5)
    result = calc.get_result()
    print(f"计算结果: {result}")

def process_numbers():
    numbers = [1, 2, 3, 4, 5]
    average = calculate_average(numbers)
    print(f"平均值: {average}")

def test_user_processing():
    users = [
        {"name": "张三", "age": 25},
        {"name": "李四", "age": 30, "email": "lisi@example.com"}
    ]
    result = process_user_data(users)
    print(result)

def test_file_operations():
    content = read_file_content("data.txt")
    print(content)

def main():
    print("开始测试...")
    
    test_calculator()
    process_numbers()
    test_user_processing()
    test_file_operations()
    
    empty_list = []
    first = get_first_element(empty_list)
    print(f"第一个元素: {first}")
    
    result = divide_numbers(10, 0)
    print(f"除法结果: {result}")
    
    text = "Hello World"
    modified = modify_string(text)
    print(f"修改后: {modified}")
    
    numbers = [1, 2, 3]
    process_items(numbers)
    
    user_input = "abc123"
    number = convert_to_int(user_input)
    print(f"转换结果: {number}")
    
    fact = factorial(5)
    print(f"阶乘: {fact}")

if __name__ == "__main__":
    main()
