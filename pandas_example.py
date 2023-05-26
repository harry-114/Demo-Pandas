import os
import sql_service_pymssql
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

def menu_pandas():
    while True:
       _print_decorated("¡Bienvenido a PANDAS!")
       print("1. Mas vendidos from DBF")
       print("2. promedio ventas DBF")
       print("3. SQL")
       print("0. volver al menu principal\n")

       opcion = int(input("Selecciona una opción: "))

       if opcion == 1:
           os.system("cls")
           _demo_mas_vendidos()
           input("Presione cualquier tecla para volver al menu PANDAS")
           break
       if opcion == 2:
           os.system("cls")
           _demo_promedio_ventas()
           input("Presione cualquier tecla para volver al menu de PANDAS")
           break
       if opcion == 3:
           os.system("cls")
           _demo_sql()
           input()
       elif opcion == 0:
        #    import main
        #    main.menu_principal()
           break
       
def _demo_mas_vendidos():
    
    df_Venta01, df_Producto = _get_dataframes()

    # Se hace un left join de los datafremes
    # de df_producto solo se toman COD_PRODUC', 'DESCRIPCIO', 'ESGRATUITO'
    df_merged = pd.merge(df_Venta01, df_Producto[['COD_PRODUC', 'DESCRIPCIO', 'ESGRATUITO']], on='COD_PRODUC', how='left')
    
    # Se agrupan los productos y se suman el total de cantidades
    productos_vendidos = df_merged[df_merged['ESGRATUITO'] == False].groupby(['COD_PRODUC', 'DESCRIPCIO'])['CANTIDAD'].sum().reset_index()

    # Se ordenar los productos por cantidad 
    productos_vendidos = productos_vendidos.sort_values('CANTIDAD', ascending=False)
    
    productos_top = productos_vendidos.head(5)

    etiquetas = productos_top.apply(lambda row: f'{row["COD_PRODUC"]} - {row["DESCRIPCIO"]}', axis=1)
    
    print("Creando gráfico...")
    plt.figure(figsize=(15, 9))
    plt.bar(productos_top['COD_PRODUC'], productos_top['CANTIDAD'], label=etiquetas)
    plt.tight_layout(rect=[0, 0, 0.8, 1])
    plt.xlabel('PLU')
    plt.ylabel('Cantidad Vendida')
    plt.title('Productos mas Vendidos')
    plt.legend(bbox_to_anchor=(1.01, 1), loc='upper left')


    # Agregar etiquetas de cantidad a cada barra
    for i, valor in enumerate(productos_top['CANTIDAD']):
        plt.text(i, valor, str(valor), ha='center', va='bottom')

    print("Gráfico creado correctamente")

    print("Guardando gráfico")
    
    plt.savefig('Mas vendidos.png')
    print("finalizado")


    import seaborn as sns

    table_Venta01, table_Producto = _get_dataframes()
    
    # Obtener los datos de interés de la tabla
    precio = []
    cantidad = []

    for record in table_Producto:
        precio.append(record['VENTA1'])

    for record in table_Venta01:
        cantidad.append(record['CANTIDAD'])

    # Crear el gráfico de dispersión utilizando Seaborn
    sns.scatterplot(x=precio, y=cantidad)

    # Añadir título y etiquetas de los ejes
    plt.title('Relación entre Precio y Cantidad')
    plt.xlabel('Precio')
    plt.ylabel('Cantidad')

    # Mostrar el gráfico
    plt.show()

def _demo_promedio_ventas():
    # Se carga la información de la dbf
    df_Venta01, df_Producto = _get_dataframes()

    # Se hace un left join de los dataframes
    df_merged = pd.merge(df_Venta01, df_Producto[['COD_PRODUC', 'DESCRIPCIO', 'ESGRATUITO',]], on='COD_PRODUC', how='left')

    # Se agrupan los productos y se calcula el promedio de las ventas
    valor_base_promedio = df_merged[df_merged['ESGRATUITO'] == False].groupby(['COD_PRODUC', 'DESCRIPCIO'])['VALORBASE'].mean().reset_index()

    # Se ordenan los productos por promedio de ventas
    valor_base_promedio = valor_base_promedio.sort_values('VALORBASE', ascending=False)

    # Seleccionar los productos más vendidos
    productos_top = valor_base_promedio.head(5)

    # Crear el gráfico utilizando Seaborn
    plt.figure(figsize=(12, 8))
    sns.catplot(x='COD_PRODUC', y='VALORBASE', data=productos_top, palette='Blues_d')
    plt.xlabel('PLU')
    plt.ylabel('Promedio de Ventas')
    plt.title('Promedio de Ventas de los Productos más Vendidos')

    # Mostrar el gráfico
    plt.savefig('promedio ventas.png')
    plt.show()

def _demo_sql():
    query_fechas =  "SELECT * FROM DimFecha"
    query_atencion_telefonica = "SELECT AtencionTelefonicaFechaClave, AtencionTelefonicaEstado  FROM FactAtencionTelefonica "    
    
    dim_fechas = sql_service_pymssql.sql_execute_query(query_fechas)
    fac_atencion_telefonica = sql_service_pymssql.sql_execute_query(query_atencion_telefonica)    

    df_Fechas = pd.DataFrame(dim_fechas)
    df_atencion_telefonica =  pd.DataFrame(fac_atencion_telefonica)

    df_merge =  pd.merge(df_atencion_telefonica, df_Fechas, left_on="AtencionTelefonicaFechaClave", right_on="FechaClave", how="left")

    df_agrupada = df_merge.groupby(["AtencionTelefonicaEstado", "Año"])["AtencionTelefonicaEstado"].count().reset_index(name="Conteo")
    
    condicion_contestadas = df_agrupada["AtencionTelefonicaEstado"] == "Contestada"
    condicion_no_contestadas =  df_agrupada["AtencionTelefonicaEstado"] == "No contestada"
    
    df_contestadas = df_agrupada[condicion_contestadas]
    df_no_contestadas =  df_agrupada[condicion_no_contestadas]

    # print(df_agrupada)
    # print(df_contestadas)
    # print(df_no_contestadas)

    # Agrupar por año y sumar las llamadas
    df_contestada_por_año = df_contestadas.groupby('Año')['Conteo'].sum()
    df_no_contestada_por_año = df_no_contestadas.groupby('Año')['Conteo'].sum()

    # print( df_contestada_por_año.values)
    # print(df_contestada_por_año.index)

    values_contestada = range(len(df_contestada_por_año.index))    
    
    plt.plot(values_contestada, df_contestada_por_año.values, label='Contestada')
    plt.plot(values_contestada, df_no_contestada_por_año.values, label='No contestada')

    plt.xticks(values_contestada, df_contestada_por_año.index.astype(int))

    
    plt.xlabel('Año')
    plt.ylabel('Llamadas')
    plt.title('Llamadas contestadas vs no contestadas por año')

    plt.legend()

    plt.show()
    
def _get_dataframes():
    from dbfread import DBF

    # Ruta y nombre del archivo DBF
    dbf_Venta01 = 'D:\\DlpSystems\\Delfin\\DbfRed\\venta01.dbf'
    dbf_Producto = 'D:\\DlpSystems\\Delfin\DbfRed\producto.dbf'

    # Se leen los archivos DBF
    table_Venta01 = DBF(dbf_Venta01)
    table_Producto = DBF(dbf_Producto)

    df_Venta01 = pd.DataFrame(table_Venta01).drop("_NullFlags", axis=1)
    df_Venta01 = pd.DataFrame(table_Producto)

    return df_Venta01, df_Venta01

def _print_decorated(message):
    os.system("cls")
    border = '*' * (len(message) + 6)
    print(border)
    print(f'* {message} *')
    print(border)
    print("\n")

menu_pandas()