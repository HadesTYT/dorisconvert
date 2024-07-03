import pyodbc
import argparse
import re

def convert_to_doris1(oracle_ddl, table_name):
    doris_ddl = oracle_ddl.replace('VARCHAR2', 'VARCHAR')

    def replace_number_type(match):
        precision_scale = match.group(1)
        if precision_scale is None:
            return 'DOUBLE'
        precision, scale = map(int, precision_scale.split(','))
        if scale == 0:
            if precision <= 10:
                return 'INT'
            else:
                return 'BIGINT'
        else:
            return f'DECIMAL({precision},{scale})'

    doris_ddl = re.sub(r'NUMBER\((\d+,\d+)\)', replace_number_type, doris_ddl)
    doris_ddl = re.sub(r'NUMBER\(\d+\)', replace_number_type, doris_ddl)
    doris_ddl = re.sub(r'NUMBER', 'DOUBLE', doris_ddl)

    # doris_ddl = doris_ddl.replace('DATE', 'DATETIME')

    def move_default_to_end(line):
        line = line.replace(' ENABLE,', '').replace(' ENABLE', '')
        match = re.search(r'(DEFAULT\s+[\S\s]+?)(\s+NOT NULL)', line)
        if match:
            default_part = match.group(1)
            not_null_part = match.group(2)
            line = line.replace(default_part, '').replace(not_null_part, '')
            line = line.rstrip().rstrip(',') + f' {not_null_part} {default_part},'
        elif 'DEFAULT' in line:
            line = line.rstrip().rstrip(',') + ','
        return line


    def remove_index_and_constraints(lines):
        new_lines = []
        skip_line = False
        for line in lines:
            if 'CONSTRAINT' in line and 'PRIMARY KEY' in line:
                skip_line = True
            if skip_line:
                if '),' in line or 'TABLESPACE' in line:
                    skip_line = False
                continue
            if not any(keyword in line for keyword in
                       ['SEGMENT CREATION', 'PCTFREE', 'PCTUSED', 'INITRANS', 'MAXTRANS', 'NOCOMPRESS', 'LOGGING',
                        'STORAGE', 'TABLESPACE', 'CONSTRAINT', 'PCTINCREASE', 'FREELISTS', 'FREELIST GROUPS', 'BUFFER_POOL', 'FLASH_CACHE', 'CELL_FLASH_CACHE']):
                new_lines.append(line)
        return new_lines

    lines = doris_ddl.split('\n')
    doris_lines = remove_index_and_constraints(lines)

    doris_lines = [move_default_to_end(line) for line in doris_lines]

    doris_lines = [line if not line.strip().endswith('NOT NULL') else line.rstrip() + ',' for line in doris_lines]

    doris_ddl = '\n'.join(doris_lines)

    doris_ddl = doris_ddl.replace(f'"{table_name}"', f'ODS_S_{table_name}')
    doris_ddl = doris_ddl.replace('"', '')
    doris_ddl = doris_ddl.replace(' NOT NULL ENABLE', ' NOT NULL')


    primary_key_match = re.search(r'CONSTRAINT .* PRIMARY KEY \((.*?)\)', oracle_ddl)
    if primary_key_match:
        primary_keys = primary_key_match.group(1).replace('"', '')
    else:
        primary_keys = 'id'  # 默认的主键字段


    doris_ddl = doris_ddl.rstrip(')') + f'''
        validity INT,
        modtime DATETIME
)
UNIQUE KEY({primary_keys})
DISTRIBUTED BY HASH({primary_keys.split(",")[0].strip()}) BUCKETS 10
PROPERTIES (
    "replication_num" = "2"
);
'''
    return doris_ddl

def convert_to_doris2(oracle_ddl, table_name):
    doris_ddl = oracle_ddl.replace('VARCHAR2', 'VARCHAR')

    # 通用处理NUMBER类型的逻辑
    def replace_number_type(match):
        precision_scale = match.group(1)
        if precision_scale is None:
            return 'DOUBLE'
        precision, scale = map(int, precision_scale.split(','))
        if scale == 0:
            if precision <= 10:
                return 'INT'
            else:
                return 'BIGINT'
        else:
            return f'DECIMAL({precision},{scale})'

    doris_ddl = re.sub(r'NUMBER\((\d+,\d+)\)', replace_number_type, doris_ddl)
    doris_ddl = re.sub(r'NUMBER\(\d+\)', replace_number_type, doris_ddl)
    doris_ddl = re.sub(r'NUMBER', 'DOUBLE', doris_ddl)


    def move_default_to_end(line):
        line = line.replace(' ENABLE,', '').replace(' ENABLE', '')
        match = re.search(r'(DEFAULT\s+[\S\s]+?)(\s+NOT NULL)', line)
        if match:
            default_part = match.group(1)
            not_null_part = match.group(2)
            line = line.replace(default_part, '').replace(not_null_part, '')
            line = line.rstrip().rstrip(',') + f' {not_null_part} {default_part},'
        elif 'DEFAULT' in line:
            line = line.rstrip().rstrip(',') + ','
        return line


    def remove_index_and_constraints(lines):
        new_lines = []
        skip_line = False
        for line in lines:
            if 'CONSTRAINT' in line and 'PRIMARY KEY' in line:
                skip_line = True
            if skip_line:
                if '),' in line or 'TABLESPACE' in line:
                    skip_line = False
                continue
            if not any(keyword in line for keyword in
                       ['SEGMENT CREATION', 'PCTFREE', 'PCTUSED', 'INITRANS', 'MAXTRANS', 'NOCOMPRESS', 'LOGGING',
                        'STORAGE', 'TABLESPACE', 'CONSTRAINT', 'PCTINCREASE', 'FREELISTS', 'FREELIST GROUPS', 'BUFFER_POOL', 'FLASH_CACHE', 'CELL_FLASH_CACHE']):
                new_lines.append(line)
        return new_lines

    lines = doris_ddl.split('\n')
    doris_lines = remove_index_and_constraints(lines)

    doris_lines = [move_default_to_end(line) for line in doris_lines]

    doris_lines = [line if not line.strip().endswith('NOT NULL') else line.rstrip() + ',' for line in doris_lines]

    doris_ddl = '\n'.join(doris_lines)

    doris_ddl = doris_ddl.replace(f'"{table_name}"', f'ODS_T_{table_name}')
    doris_ddl = doris_ddl.replace('"', '')
    doris_ddl = doris_ddl.replace(' NOT NULL ENABLE', ' NOT NULL')

    primary_key_match = re.search(r'CONSTRAINT .* PRIMARY KEY \((.*?)\)', oracle_ddl)
    if primary_key_match:
        primary_keys = primary_key_match.group(1).replace('"', '').split(', ')
    else:
        primary_keys = ['id']  # 默认的主键字段

    legal_fields = []
    for line in doris_lines:
        if 'LEGAL' in line.upper():  # 查找包含LEGAL的字段
            parts = line.strip().split()
            if parts:
                field_name = parts[0].replace('"', '')  # 去掉双引号
                legal_fields.append(field_name)

    for field in legal_fields:
        if field not in primary_keys:
            primary_keys.append(field)

    doris_ddl = doris_ddl.rstrip(')') + f'''
        validity INT,
        modtime DATETIME
)
UNIQUE KEY({', '.join(primary_keys)})
DISTRIBUTED BY HASH({primary_keys[0]}) BUCKETS 10
PROPERTIES (
    "replication_num" = "2"
);
'''
    return doris_ddl

def main():
    parser = argparse.ArgumentParser(description='Convert Oracle DDL to Doris DDL.')
    parser.add_argument('table_file', type=str, help='Path to the file containing table names, one per line')
    args = parser.parse_args()

    table_file = args.table_file

    with open(table_file, 'r') as f:
        table_names = [line.strip() for line in f.readlines()]

    username = 'ds'
    password = 'kckckc258'
    dsn = 'OracleDSN'

    connection = pyodbc.connect(f'DSN={dsn};UID={username};PWD={password}')

    cursor = connection.cursor()

    with open('ddl.txt', 'w') as output_file:
        table_number = 0  # 计数器变量
        for table_name in table_names:
            table_number += 1  # 每次迭代时递增
            synonym_query = f"SELECT COUNT(*) FROM all_synonyms WHERE SYNONYM_NAME = '{table_name}'"
            cursor.execute(synonym_query)
            count = cursor.fetchone()[0]

            if count == 0:
                convert_function = convert_to_doris2
            else:
                convert_function = convert_to_doris1

            query = f"SELECT DBMS_METADATA.GET_DDL('TABLE', '{table_name}') FROM DUAL"
            cursor.execute(query)

            ddl = cursor.fetchone()[0]

            doris_ddl = convert_function(ddl, table_name)

            # Remove the 'DS.' prefix from any occurrence in the doris_ddl
            doris_ddl = doris_ddl.replace('DS.', '')
            # 修改成只替换第三行的 (，保持其他行不变
            doris_ddl_lines = doris_ddl.split('\n')
            if doris_ddl_lines[2].find('(') != -1:
                doris_ddl_lines[2] = doris_ddl_lines[2].replace('(', '(\n', 1)
            doris_ddl = '\n'.join(doris_ddl_lines)

            # 找到主键字段
            primary_key_match = re.search(r'UNIQUE KEY\((.*?)\)', doris_ddl, re.IGNORECASE)
            if primary_key_match:
                primary_keys = primary_key_match.group(1).replace('"', '').split(', ')
                doris_ddl_lines = doris_ddl.split('\n')
                for key in primary_keys:
                    exact_match_found = False
                    for i, line in enumerate(doris_ddl_lines):
                        # Split the line by spaces to ensure exact matching of the key
                        if any(word == key.strip() for word in line.split()):
                            if 'UNIQUE KEY' not in line and 'DISTRIBUTED BY HASH' not in line:
                                # 将主键字段行的内容追加到第三行后面
                                doris_ddl_lines[2] += '\n' + line
                                doris_ddl_lines[i] = ''  # 清空找到的行
                                exact_match_found = True
                                break  # Exit the inner loop once exact match is found
                    if not exact_match_found:
                        print(f"Exact match for '{key.strip()}' not found in the DDL.")
            doris_ddl = '\n'.join(doris_ddl_lines)

            output_file.write(f"{doris_ddl}\n-- Converted from Oracle table: {table_name}\n\n")
            print(f"-- Converted from Oracle table: {table_name} number: {table_number}\n\n")

    cursor.close()
    connection.close()

if __name__ == "__main__":
    main()
