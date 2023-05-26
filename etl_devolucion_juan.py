import sql_service_pymssql
import sql_service_pyodbc
import pandas as pd
import math

def obtener_Devoluciones():

    # region Obtencion de devoluciones
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

    devoluciones =  sql_service_pymssql.sql_execute_query("DSEmpresa", query_devoluciones)
    
    df_devoluciones = pd.DataFrame(devoluciones)

# endregion
   
    # region Transformacion de datos
    df_devoluciones["FechaClave"] = df_devoluciones['FechaDocumento'].dt.strftime('%Y%m%d').astype(int)    
    df_devoluciones['HoraClave'] = df_devoluciones['FechaDocumento'].apply(
        lambda x: (x.hour * 10000) + (x.minute * 100) + x.second
    )

    df_devoluciones['ClienteClaveAlterna'] = df_devoluciones['IdentificacionCuenta'].apply(
        lambda x: x.split('-')[0].strip()
    )    
    # endregion
   
    # region  se hace el merge de devoluciones con empleados
    query_empleados =  '''SELECT [EmpleadoClave], [EmpleadoClaveAlterna] FROM [DimEmpleado]''' 

    empleados = sql_service_pymssql.sql_execute_query("warehouse", query_empleados)

    df_empleados = pd.DataFrame(empleados)
    
    df_devoluciones =  pd.merge(df_devoluciones, df_empleados, left_on='IdentificacionEmpleado', right_on='EmpleadoClaveAlterna', how='left')

   # endregion
    
    # region Se mezclan las devoluciones con los clientes
    query_clientes =  '''SELECT [ClienteClave], [ClienteClaveAlterna] FROM [DimCliente]'''
    
    clientes = sql_service_pymssql.sql_execute_query("warehouse", query_clientes)

    df_Clientes = pd.DataFrame(clientes)
    df_devoluciones = pd.merge(df_devoluciones, df_Clientes, on='ClienteClaveAlterna', how="left")   
   
   # endregion
    
    # region Se mezclan las devoluciones con los productos    
    query_productos = '''SELECT [ProductoClave], [ProductoClaveAlterna] FROM [DimProducto]'''
    
    productos = sql_service_pymssql.sql_execute_query("warehouse", query_productos)
    
    df_productos =  pd.DataFrame(productos)

    df_devoluciones =  pd.merge(df_devoluciones
                                    , df_productos
                                    , left_on='CodProducto'
                                    , right_on='ProductoClaveAlterna'
                                    , how='inner'
                                )
    
    # endregion

    df_devoluciones = df_devoluciones.drop(['IdentificacionEmpleado', 'IdentificacionCuenta'
        , 'CodDocumentoDetalle', 'CodProducto', 'ValorImpuestoUnidad', 'ValorRetencionUnidad'
        ,'ClienteClaveAlterna', 'EmpleadoClaveAlterna', 'ProductoClaveAlterna', 'FechaDocumento']
        , axis=1
    )

    nuevas_etiquetas = {columna: 'Devolucion' + columna for columna in df_devoluciones.columns}

    # Renombrar las columnas del DataFrame
    df_devoluciones = df_devoluciones.rename(columns=nuevas_etiquetas)
    df_devoluciones = df_devoluciones.rename(columns={'DevolucionCodDocumento': 'DevolucionClaveAlterna'
                                                      , 'DevolucionValorCosto': 'DevolucionValorCostoUnidad'}) 


    _insertar_datos(df_devoluciones)
   

def _insertar_datos(df_devoluciones):

    # Establecer la conexión con SQL Server
    cnn = sql_service_pymssql.get_connection(server='localhost:1433', user='sa', password='123456', database=f'warehouse')
    
    # Crear un cursor
    cursor = cnn.cursor()

    # Convertir el DataFrame a una lista de tuplas
    valores = [(None if isinstance(val, float) and math.isnan(val) else val) for row in df_devoluciones.values for val in row]

    # Especificar las columnas en la consulta INSERT
    columnas = ', '.join(df_devoluciones.columns)

    # Construir la consulta INSERT excluyendo la columna de identidad
    consulta = f"INSERT INTO FactDevolucion ({columnas}) VALUES ({', '.join(['%s']*len(df_devoluciones.columns))})"    
    
    try:
         # Ejecutar la consulta INSERT con los valores del DataFrame
        cursor.executemany(consulta, valores) 
        
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


def replace_nan_with_none(tup):
    return tuple(map(lambda x: None if isinstance(x, float)  else x, tup))

obtener_Devoluciones()
