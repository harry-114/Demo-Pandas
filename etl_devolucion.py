import sql_service_pymssql
import sql_service_pyodbc
import pandas as pd

def obtener_Devoluciones():

    query_devoluciones =  ''' 
    SELECT IIF(vd.CodAlmacen = 'FD962324-CA2C-E511-8102-00155D002802', 1, 2) AS EmpresaClave
		, c.NumeroIdentificacion AS IdentificacionCuenta
		, co.NumeroIdentificacion AS IdentificacionEmpleado
		, vd.PrefijoNumero AS NumeroDocumento
		, vd.FechaDocumento
		, IIF(vd.Estado = 1, 'Borrador',IIF(vd.Estado = 2 , 'En Espera',IIF(vd.Estado = 3, 'Finalizado', IIF(vd.Estado = 4 , 'Cancelado', IIF(vd.Estado = 5, 'Anulado', 'No procesado'))))) AS Estado
		, IIF(vd.FormaPago = 1, 'Contado', IIF(vd.FormaPago = 2, 'Credito', 'Sin forma pago')) AS Tipo
		, vdd.[CodDocumentoDetalle]
		, vdd.[CodDocumento]
		, vdd.[CodProducto]
		, vdd.[Cantidad]
		, vdd.Orden AS NumeroLinea
		, vdd.Costo AS ValorCosto
		, vdd.[PrecioListaUnidad] AS ValorListaUnidad
		, vdd.[DescuentoUnidad] AS ValorDescuentoUnidad
		, vdd.[PrecioBrutoUnidad] AS ValorBrutoUnidad
		, vdd.[IvaUnidad]  AS ValorIvaUnidad
		, vdd.[ImpuestoUnidad] AS ValorImpuestoUnidad 
		, vdd.[RetencionUnidad] AS ValorRetencionUnidad
		, vdd.[PrecioBrutoUnidad] + vdd.[IvaUnidad] AS ValorNetoUnidad
		, vdd.[Cantidad]* vdd.[Costo] AS ValorCostoTotal
		, vdd.[Cantidad]* vdd.[PrecioBrutoUnidad] AS ValorBrutoTotal	

		, (vdd.PrecioBrutoUnidad *vdd.Cantidad )+(vdd.IvaUnidad*vdd.Cantidad)  ValorNetoTotal

		, vdd.[Cantidad]* vdd.[PrecioListaUnidad] AS ValorListaTotal
		, vdd.[Cantidad]* vdd.DescuentoUnidad AS ValorDescuentoTotal
		, vdd.[Cantidad]* vdd.IvaUnidad  AS ValorIvaTotal

		, IIF(ddi.Tarifa = 0, 'Excento', IIF(ddi.Tarifa > 0, 'Gravado', 'Excluido')) AS TipoIva

	  FROM [DSEmpresa].[dbo].[VistaDocumentoDetalle] vdd
	  LEFT JOIN VistaDocumento vd ON vd.CodDocumento = vdd.CodDocumento
	  LEFT JOIN DocumentoDetalleImpuesto ddi ON ddi.CodDocumentoDetalle =  vdd.CodDocumentoDetalle
	  LEFT JOIN Cuenta c ON vd.CodCuenta  =  c.CodCuenta
	  LEFT JOIN Contacto co ON vd.CodResponsable = co.CodContacto

	  WHERE vd.CodDocumentoTipo = '988E1B6F-9652-EA11-BC22-1C39472C285F' AND vd.FechaDocumento BETWEEN DATEADD(m, -6, DATETIMEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), DAY(GETDATE()), 0, 0, 0, 0)) AND DATETIMEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), DAY(GETDATE()), 23, 59, 59, 0)
    '''

    #Obtencion de datos
    devoluciones =  sql_service_pymssql.sql_execute_query("DSEmpresa", query_devoluciones)
    
    df_devoluciones = pd.DataFrame(devoluciones)


    #Transformacion de datos.
    df_devoluciones["FechaClave"] = df_devoluciones['FechaDocumento'].dt.strftime('%Y%m%d').astype(int)    
    df_devoluciones['HoraClave'] = df_devoluciones['FechaDocumento'].apply(lambda x: (x.hour * 10000) + (x.minute * 100) + x.second)

    df_devoluciones = df_devoluciones.drop(['FechaDocumento'], axis=1)

    df_devoluciones['ClienteClaveAlterna'] = df_devoluciones['IdentificacionCuenta'].apply(lambda x: x.split('-')[0].strip())

    #df_devoluciones.sort_values(['IdentificacionEmpleado'], ascending=True)

    query_empleados =  '''SELECT [EmpleadoClave], [EmpleadoClaveAlterna] FROM [DimEmpleado]''' 
    empleados = sql_service_pymssql.sql_execute_query("warehouse", query_empleados)

    print(df_devoluciones.shape[0])

    df_empleados = pd.DataFrame(empleados).sort_values('EmpleadoClaveAlterna', ascending=True)

    #Se mezclan las devoluciones con los empleados
    df_devoluciones =  pd.merge(df_devoluciones, df_empleados, left_on='IdentificacionEmpleado', right_on='EmpleadoClaveAlterna', how='left')

    query_clientes =  '''SELECT [ClienteClave], [ClienteClaveAlterna] FROM [DimCliente]'''
    
    clientes = sql_service_pymssql.sql_execute_query("warehouse", query_clientes)

    df_Clientes = pd.DataFrame(clientes).sort_values("ClienteClaveAlterna", ascending=True)

    print(df_devoluciones.shape[0])
    #Se mezclan las devoluciones con los clientes
    df_devoluciones = pd.merge(df_devoluciones, df_Clientes, on='ClienteClaveAlterna', how="left")

    print(df_devoluciones.shape[0])
    query_productos = '''SELECT [ProductoClave], [ProductoClaveAlterna] FROM [DimProducto]'''
    
    productos = sql_service_pymssql.sql_execute_query("warehouse", query_productos)
    
    df_productos =  pd.DataFrame(productos).sort_values("ProductoClaveAlterna", ascending=True)

    df_devoluciones =  pd.merge(df_devoluciones, df_productos, left_on='CodProducto', right_on='ProductoClaveAlterna', how='inner')
    print(df_devoluciones.shape[0])

    nuevas_etiquetas = {columna: 'Devolucion' + columna for columna in df_devoluciones.columns}

    # Renombrar las columnas del DataFrame
    df_devoluciones = df_devoluciones.rename(columns=nuevas_etiquetas)


    # DevolucionCodDocumentoDetalle, DevolucionCodDocumento, DevolucionCodProducto,
    df_devoluciones = df_devoluciones.drop(['DevolucionIdentificacionEmpleado', 'DevolucionIdentificacionCuenta', 'DevolucionCodDocumentoDetalle'
                                            , 'DevolucionCodProducto', 'DevolucionValorImpuestoUnidad', 'DevolucionValorRetencionUnidad'
                                            ,'DevolucionClienteClaveAlterna', 'DevolucionEmpleadoClaveAlterna', 'DevolucionProductoClaveAlterna'], axis=1)
    
    df_devoluciones = df_devoluciones.rename(columns={'DevolucionCodDocumento': 'DevolucionClaveAlterna'})
    df_devoluciones = df_devoluciones.rename(columns={'DevolucionValorCosto': 'DevolucionValorCostoUnidad'})

    _insertar_datos(df_devoluciones)
   

def _insertar_datos(df_devoluciones):
     
    # Eliminar las filas con valores NaN
    #df_devoluciones = df_devoluciones.dropna()

    # Reemplazar los valores NaN por una cadena vacía
    df_devoluciones.fillna(' ')

    # Ajustar los nombres de las columnas si exceden la longitud máxima    
    

    # Establecer la conexión con SQL Server
    cnn = sql_service_pymssql.get_connection(server='localhost', user='sa', password='123456', database=f'warehouse')
    
    # Crear un cursor
    cursor = cnn.cursor()

    # Convertir el DataFrame a una lista de tuplas
    valores = [tuple(row) for row in df_devoluciones.values]

    # Especificar las columnas en la consulta INSERT
    columnas = ', '.join(df_devoluciones.columns)

    # Construir la consulta INSERT excluyendo la columna de identidad
    consulta = f"INSERT INTO FactDevolucion ({columnas}) VALUES ({', '.join(['%s']*len(df_devoluciones.columns))})"
    
    
    try:

        cursor.executemany(consulta, valores) 

        # Ejecutar la consulta INSERT con los valores del DataFrame
        
        # Confirmar los cambios en la base de datos
        cnn.commit()
        print("Datos insertados correctamente en la tabla FactDevolucion.")
        
    except Exception as e:
        # Imprimir el error en caso de fallo
        print("Error al insertar los datos en la tabla FactDevolucion:", str(e))
        
        # Revertir los cambios en caso de fallo
        #cnn.rollback()
        
    finally:
        # Cerrar la conexión y el cursor
        cursor.close()
        cnn.close()


obtener_Devoluciones()
