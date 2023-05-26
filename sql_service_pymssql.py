import pymssql

def get_connection(server, database, user, password ):
    return  pymssql.connect(server, user , password, database)  

def sql_execute_query(database, query):
    try:

        conexion = get_connection(server='localhost', user='sa', password='123456', database=f'{database}')          
        cursor = conexion.cursor(as_dict=True)
        cursor.execute(query)

        # Obtén los resultados de la consulta
        resultados = cursor.fetchall()

        return resultados
    except pymssql.Error as e:
        print(f"Error al ejecutar la consulta SQL: {e}")
        return None
    finally:
        if conexion:
            conexion.close()
