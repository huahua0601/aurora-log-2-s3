#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
10个常见的Python Bug示例
这些示例展示了Python开发中经常遇到的错误类型
"""

# Bug 1: 缩进错误 (IndentationError)
def bug_1_indentation_error():
    """缩进错误示例"""
    print("这是正确的缩进")
   print("这里缩进不对")  # 这行缩进错误
    return "完成"

# Bug 2: 变量名拼写错误 (NameError)
def bug_2_name_error():
    """变量名拼写错误示例"""
    message = "Hello World"
    print(mesage)  # 变量名拼写错误，应该是 message
    return "完成"

# Bug 3: 列表索引越界 (IndexError)
def bug_3_index_error():
    """列表索引越界示例"""
    my_list = [1, 2, 3]
    print(my_list[5])  # 索引越界，列表只有3个元素
    return "完成"

# Bug 4: 字典键不存在 (KeyError)
def bug_4_key_error():
    """字典键不存在示例"""
    my_dict = {"name": "张三", "age": 25}
    print(my_dict["address"])  # 键不存在
    return "完成"

# Bug 5: 类型错误 (TypeError)
def bug_5_type_error():
    """类型错误示例"""
    number = 10
    text = "Hello"
    result = number + text  # 不能将数字和字符串相加
    return result

# Bug 6: 零除错误 (ZeroDivisionError)
def bug_6_zero_division_error():
    """零除错误示例"""
    a = 10
    b = 0
    result = a / b  # 除以零
    return result

# Bug 7: 属性错误 (AttributeError)
def bug_7_attribute_error():
    """属性错误示例"""
    my_string = "Hello World"
    my_string.append("!")  # 字符串没有append方法
    return my_string

# Bug 8: 值错误 (ValueError)
def bug_8_value_error():
    """值错误示例"""
    user_input = "abc"
    number = int(user_input)  # 无法将非数字字符串转换为整数
    return number

# Bug 9: 无限递归 (RecursionError)
def bug_9_recursion_error(n=1000):
    """无限递归示例"""
    return bug_9_recursion_error(n + 1)  # 没有递归终止条件

# Bug 10: 文件不存在 (FileNotFoundError)
def bug_10_file_not_found_error():
    """文件不存在示例"""
    with open("不存在的文件.txt", "r") as file:
        content = file.read()
    return content

# 修复版本的示例
class FixedExamples:
    """修复后的正确版本"""
    
    @staticmethod
    def fix_1_indentation():
        """修复缩进错误"""
        print("这是正确的缩进")
        print("这里缩进也对了")  # 正确的缩进
        return "完成"
    
    @staticmethod
    def fix_2_name_error():
        """修复变量名错误"""
        message = "Hello World"
        print(message)  # 正确的变量名
        return "完成"
    
    @staticmethod
    def fix_3_index_error():
        """修复索引越界"""
        my_list = [1, 2, 3]
        if len(my_list) > 5:
            print(my_list[5])
        else:
            print("索引超出范围")
        return "完成"
    
    @staticmethod
    def fix_4_key_error():
        """修复字典键错误"""
        my_dict = {"name": "张三", "age": 25}
        address = my_dict.get("address", "地址未设置")  # 使用get方法
        print(address)
        return "完成"
    
    @staticmethod
    def fix_5_type_error():
        """修复类型错误"""
        number = 10
        text = "Hello"
        result = str(number) + text  # 将数字转换为字符串
        return result
    
    @staticmethod
    def fix_6_zero_division_error():
        """修复零除错误"""
        a = 10
        b = 0
        if b != 0:
            result = a / b
        else:
            result = "不能除以零"
        return result
    
    @staticmethod
    def fix_7_attribute_error():
        """修复属性错误"""
        my_string = "Hello World"
        my_list = list(my_string)
        my_list.append("!")  # 先转换为列表
        return "".join(my_list)
    
    @staticmethod
    def fix_8_value_error():
        """修复值错误"""
        user_input = "abc"
        try:
            number = int(user_input)
        except ValueError:
            number = "输入不是有效的数字"
        return number
    
    @staticmethod
    def fix_9_recursion_error(n=0):
        """修复无限递归"""
        if n > 1000:  # 添加终止条件
            return "达到最大值"
        return FixedExamples.fix_9_recursion_error(n + 1)
    
    @staticmethod
    def fix_10_file_not_found_error():
        """修复文件不存在错误"""
        try:
            with open("不存在的文件.txt", "r") as file:
                content = file.read()
        except FileNotFoundError:
            content = "文件不存在"
        return content

def run_bug_examples():
    """运行所有bug示例（会产生错误）"""
    bugs = [
        bug_1_indentation_error,
        bug_2_name_error,
        bug_3_index_error,
        bug_4_key_error,
        bug_5_type_error,
        bug_6_zero_division_error,
        bug_7_attribute_error,
        bug_8_value_error,
        bug_9_recursion_error,
        bug_10_file_not_found_error
    ]
    
    for i, bug_func in enumerate(bugs, 1):
        print(f"\n=== Bug {i}: {bug_func.__name__} ===")
        try:
            result = bug_func()
            print(f"结果: {result}")
        except Exception as e:
            print(f"错误类型: {type(e).__name__}")
            print(f"错误信息: {e}")

def run_fixed_examples():
    """运行修复后的示例"""
    fixes = [
        FixedExamples.fix_1_indentation,
        FixedExamples.fix_2_name_error,
        FixedExamples.fix_3_index_error,
        FixedExamples.fix_4_key_error,
        FixedExamples.fix_5_type_error,
        FixedExamples.fix_6_zero_division_error,
        FixedExamples.fix_7_attribute_error,
        FixedExamples.fix_8_value_error,
        FixedExamples.fix_9_recursion_error,
        FixedExamples.fix_10_file_not_found_error
    ]
    
    for i, fix_func in enumerate(fixes, 1):
        print(f"\n=== 修复 {i}: {fix_func.__name__} ===")
        try:
            result = fix_func()
            print(f"结果: {result}")
        except Exception as e:
            print(f"仍有错误: {type(e).__name__} - {e}")

if __name__ == "__main__":
    print("Python Bug示例集合")
    print("=" * 50)
    
    choice = input("选择运行模式:\n1. 运行bug示例 (会产生错误)\n2. 运行修复版本\n3. 两个都运行\n请输入选择 (1/2/3): ")
    
    if choice == "1":
        run_bug_examples()
    elif choice == "2":
        run_fixed_examples()
    elif choice == "3":
        print("\n首先运行bug示例:")
        run_bug_examples()
        print("\n" + "="*50)
        print("现在运行修复版本:")
        run_fixed_examples()
    else:
        print("无效选择")
