create_table_cmd = """

    CREATE TABLE {schema_name}.{table_name}({table_conf})

"""
delete_cmd = """

    DELETE FROM {schema_name}.{table_name} {where}

"""

select_cmd = """

    SELECT * FROM {schema_name}.{table_name} {where}

"""