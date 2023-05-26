import pyodbc

def establecer_conexion():
    server = 'localhost'
    database = 'warehouse'
    user = 'sa'
    password = '123456'

    # Cadena de conexión
    conn_str = f"DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={user};PWD={password}"

    try:
        # Establecer la conexión
        conn = pyodbc.connect(conn_str)
        print("Conexión exitosa.")

        # Retornar la conexión para su uso posterior
        return conn

    except pyodbc.Error as e:
        # Imprimir el error en caso de fallo
        print("Error al establecer la conexión:", str(e))

        # Retornar None en caso de fallo
        return None

