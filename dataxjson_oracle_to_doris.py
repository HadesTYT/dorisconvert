import os
import json
import pyodbc
from datetime import datetime

def determine_table_prefix(table_name, cursor):
    # 查询是否存在同义词
    synonym_query = f"SELECT COUNT(*) FROM all_synonyms WHERE SYNONYM_NAME = '{table_name}'"
    cursor.execute(synonym_query)
    count = cursor.fetchone()[0]

    # 根据查询结果确定表名前缀
    if count == 0:
        return "ODS_T"
    else:
        return "ODS_S"

def find_primary_keys(table_name, cursor, table_prefix):
    # 查询主键列
    primary_key_query = f"""
        SELECT column_name
        FROM user_cons_columns
        WHERE table_name = '{table_name}'
        AND position IS NOT NULL
    """
    cursor.execute(primary_key_query)
    primary_keys = cursor.fetchall()

    # 提取主键列名称
    primary_key_columns = [key[0] for key in primary_keys]

    # 对于T表，添加以LEGAL结尾的字段作为额外主键
    if table_prefix == "ODS_T":
        legal_key_query = f"""
            SELECT column_name
            FROM user_tab_columns
            WHERE table_name = '{table_name}'
            AND column_name LIKE '%LEGAL'
        """
        cursor.execute(legal_key_query)
        legal_key = cursor.fetchone()
        if legal_key:
            primary_key_columns.append(legal_key[0])

    # 拼接主键列名称为逗号分隔的字符串
    split_pk = ",".join(primary_key_columns)

    return split_pk

def find_table_columns(table_name, cursor):
    # 查询表的所有字段
    column_query = f"SELECT column_name FROM user_tab_columns WHERE table_name = '{table_name}' ORDER BY column_id"
    cursor.execute(column_query)
    columns = cursor.fetchall()
    table_columns = [column[0] for column in columns]

    return table_columns

def generate_datax_json(table_name, cursor):
    # 确定表名前缀
    table_prefix = determine_table_prefix(table_name, cursor)

    # 查找主键列
    split_pk = find_primary_keys(table_name, cursor, table_prefix)

    # 查找表的所有字段
    table_columns = find_table_columns(table_name, cursor)

    # 构建 column 列表，包括所有字段和额外的固定字段
    column_list = [f"`{column}`" for column in table_columns]
    column_list.extend([
        "`validity`",
        "`modtime`"
    ])

    # DataX JSON模板
    datax_template = {
        "job": {
            "setting": {
                "speed": {
                    "channel": 3,
                    "byte": 1048576
                },
                "errorLimit": {
                    "record": 0,
                    "percentage": 0.02
                }
            },
            "content": [
                {
                    "reader": {
                        "name": "oraclereader",
                        "parameter": {
                            "username": "maint",
                            "password": "kckckc258",
                            "splitPk": split_pk,
                            "connection": [
                                {
                                    "querySql": [
                                        f"select a.*,1 validity,SYSDATE modtime from ${{SCHEMA}}.{table_name} a"
                                    ],
                                    "jdbcUrl": [
                                        "jdbc:oracle:thin:@//${IP}:1521/topprod"
                                    ]
                                }
                            ]
                        }
                    },
                    "writer": {
                        "name": "doriswriter",
                        "parameter": {
                            "loadUrl": [
                                "192.168.2.155:8030"
                            ],
                            "loadProps": {
                                "format": "json",
                                "strip_outer_array": "true",
                                "line_delimiter": "\\x02"
                            },
                            "username": "user_erp",
                            "password": "Th099a1099",
                            "column": column_list,
                            "connection": [
                                {
                                    "table": [
                                        f"{table_prefix}_{table_name}"
                                    ],
                                    "selectedDatabase": "ERP_ODS",
                                    "jdbcUrl": "jdbc:mysql://192.168.2.155:9030/ERP_ODS"
                                }
                            ]
                        }
                    }
                }
            ]
        }
    }

    # 替换模板中的变量
    datax_json = json.dumps(datax_template, indent=2)  # 设置indent参数来保留格式化
    # datax_json = datax_json.replace("${{SCHEMA}}", "your_schema_here")  # 替换schema变量
    # datax_json = datax_json.replace("${{IP}}", "your_ip_here")  # 替换IP变量

    return datax_json

def main():
    # 连接Oracle数据库
    username = 'ds'
    password = 'kckckc258'
    dsn = 'OracleDSN'
    connection = pyodbc.connect(f'DSN={dsn};UID={username};PWD={password}')
    cursor = connection.cursor()

    # 读取表名列表文件
    tables_file = "datax.txt"
    with open(tables_file, "r") as f:
        tables = f.read().splitlines()

    # 创建json文件夹
    if not os.path.exists("json"):
        os.makedirs("json")

    # 生成每个表的DataX JSON配置文件
    for table in tables:
        json_data = generate_datax_json(table, cursor)
        json_filename = f"json/{table}.json"
        with open(json_filename, "w") as f:
            f.write(json_data)
            # 打印当前时间
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"Generated {json_filename} time: {current_time}")

    # 关闭数据库连接
    cursor.close()
    connection.close()

if __name__ == "__main__":
    main()
