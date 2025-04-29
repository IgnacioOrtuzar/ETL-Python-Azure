import logging
import azure.functions as func
import requests
import pandas as pd
import time
import pyodbc
import json
import datetime
import os

import concurrent.futures
import aiohttp
import asyncio
from more_itertools import chunked

from datetime import datetime,timedelta
from pandas import json_normalize
from sqlalchemy import create_engine, text



app = func.FunctionApp()

@app.schedule(schedule="15 * * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def Trigger_Latinad_API(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')

    main()
    logging.info('Python timer trigger function executed.')

def main():
    
    api_key = os.environ['pass_api_key']
    hdr = {'Authorization': api_key}
    connection_string =  os.environ['Conection_string']

    def extraction_data_display():
        
        url_tareas = "http://api.publinet.io/companies/283/displays"
        try:
            response = requests.get(url_tareas, headers=hdr)
            response.raise_for_status()  # Verificar si la solicitud fue exitosa
            df_task = json_normalize(response.json())

            df_task.drop(columns=['audience_provider.id',], inplace=True)
            df_task.columns = df_task.columns.str.replace('.', '_')
            df_task = df_task[df_task['id'] != 40660]

            df_task = df_task[['id','company_id','name','resolution_width','resolution_height','latitude','longitude','slots','slot_length','constraints',
                              'last_connection','paired','created_at','updated_at','player_config','player_config_updated_at','reboot_on_next_sync','player_version',
                              'shows_per_hour','aux_shows_multiplier','price_per_day','smart_campaign_enabled','smart_campaign_exhibition_enabled','smart_campaign_cpm',
                              'commission','commission_cpm','location_type','open_ooh_venue_type_id','size_type','size_width','size_height','published','offline_notification_sent',
                              'time_to_send_offline_notification','enable_offline_notification','lead_time','default_resolve','default_resolve_time','min_campaign_time',
                              'estimated_viewers','description','orientation','quality','brand','brightness','visualization','reference_places','ground_elevation','target',
                              'brands_advertised','camlytics_tokens','certification_mode_enabled','certification_mode_save_impressions','certification_mode_enabled_at',
                              'certification_mode_duration','certification_mode_on_secondary_displays','country','country_iso','administrative_area_level_1',
                              'administrative_area_level_2','locality','formatted_address','zip_code','time_zone','brightness_schedule','auto_smart_sort','auto_smart_sort_type',
                              'use_cms','main_display_id','is_main_display','archived','dats_why_poi_id','pois_request_date','has_external_audience_data','is_test',
                              'priority_content_url','priority_content_type','priority_content_length','multiplier','external_programmatic_cpm','third_party_average_monthly_audience',
                              'default_content_id','external_id','enabled_automatic_evidence','mirror_screens_count','bundle_only','audience_provider_id',
                              'is_online','time_zone_offset','current_loop_time','display_currency','display_type','count_secondary_displays',
                              'last_connection_display_local_time','automatic_evidence','tags','working_hours','rank_discounts','pictures',
                              'secondary_displays','secondary_displays_with_pictures','audience_provider_name','audience_provider_stylized_name','audience_provider_url',
                              'audience_provider_image','audience_provider_description','audience_provider_created_at','audience_provider_updated_at']]
            # Retornar los IDs y el DataFrame final
            
            ids = df_task['id'].tolist()
            return ids, df_task
        
        except requests.exceptions.HTTPError as e:
            print(f"Error HTTP: {e.response.status_code} - {e.response.reason}")
        except Exception as e:
            print(f"Otro error: {str(e)}")

    def extraction_data_contenido():
        url_tareas = "https://api.publinet.io/companies/283/contents?datatable=true&length=11000&start=0&columns[0][data]=updated_at&columns[0][orderable]=true&order[0][column]=0&order[0][dir]=desc"
        df_task = []  # Inicializa df_task como lista vacía
        df_content = pd.DataFrame()  # Inicializa df_content como DataFrame vacío
        
        try:
            # Realiza la solicitud GET a la API
            response = requests.get(url_tareas, headers=hdr)
            response.raise_for_status()  # Lanza un error si el código de estado no es 200

            # Extrae el JSON de la respuesta
            data = response.json()

            # Verifica si 'data' está en la respuesta y tiene contenido
            if 'data' not in data or not data['data']:
                raise ValueError("No se encontraron tareas en la respuesta.")

            # Normaliza solo la parte 'data'
            df_task = json_normalize(data['data'])

            # Filtra registros donde 'count_displays' > 0
            #df_task = df_task[df_task['count_displays'] > 0]

            # Extrae la lista de IDs
            id_contenido = df_task['id'].tolist()

            # Renombra la columna 'file' a 'arch'
            df_task.rename(columns={'file': 'arch'}, inplace=True)

            # Selecciona las columnas relevantes
            columnas = ['id', 'name', 'type', 'width', 'height', 'length', 'arch', 'file_thumb', 'ready',
                        'company_id', 'created_by', 'updated_by', 'created_at', 'updated_at', 'archived', 'is_filling',
                        'origin', 'belong_to_smart', 'belong_to_id', 'smartv2_continuous', 'canva_id', 'external_id',
                        'content_category_id', 'category', 'count_displays', 'tags']

            # Validar que las columnas existan antes de seleccionarlas
            df_content = df_task.loc[:, [col for col in columnas if col in df_task.columns]]

            # Limita la longitud de la columna 'arch' a 50 caracteres
            df_content['arch'] = df_content['arch'].apply(lambda x: "" if pd.isna(x) else (x if len(x) <= 50 else ""))

        except requests.exceptions.RequestException as e:
            print(f"Error en la solicitud: {e}")
        except ValueError as e:
            print(f"Error de validación: {e}")
        except Exception as e:
            print(f"Error inesperado: {e}")

        # Retorna los valores, incluso si están vacíos
        return id_contenido, df_content

    # Función asíncrona para hacer la solicitud por lotes
    async def fetch_report(session, url, cont):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=600)) as response:
                response.raise_for_status()
                data = await response.json()
                print(f"Campaña con ID {cont} procesada.")
                result_df = json_normalize(data['report'])
                return result_df
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            print(f"Error al hacer la solicitud HTTP para la campaña con ID {cont}: {e}")
            return None

    # Función asíncrona principal que ahora maneja lotes
    async def buscar_elemento_async(contenido, lista_ids):
        resultados = []
        failed_ids = []
        Fecha_actual = datetime.now().date()
        fecha_inicio = Fecha_actual - timedelta(days=25)
        fecha_fin = Fecha_actual + timedelta(days=2)
        
        ids_displays = ','.join(map(str, lista_ids))

        async with aiohttp.ClientSession(headers=hdr) as session:
            tasks = []
            for cont in contenido:
                url = f"https://api.publinet.io/reports?contents={cont}&displays={ids_displays}&from_date={fecha_inicio}&per_date=1&to_date={fecha_fin}"
                tasks.append(fetch_report(session, url, cont))

            for future in asyncio.as_completed(tasks):
                result = await future
                if result is not None:
                    resultados.append(result)
                else:
                    failed_ids.append(cont)

        return resultados, failed_ids

    def buscar_elemento(contenido, lista_ids):
        resultados, failed_ids = asyncio.run(buscar_elemento_async(contenido, lista_ids))
        return resultados, failed_ids

    def split_and_remove_columns(df):
        # Obtener las columnas específicas que quieres dividir
        columns_to_split = ['display', 'content', 'child_content_id', 'shows', 'total_time', 'date', 'impacts']

        # Crear dos DataFrames con las columnas especificadas
        df1 = df[columns_to_split].copy()
        df['impacts'] = df['impacts'].fillna(0).astype(int)
        df1.rename(columns = {'date':'Fecha'}, inplace=True)
        df2 = df.drop(columns=columns_to_split)
        return df1, df2

    def remove_empty_rows(df):
        # Eliminar filas con valores NaN en las columnas 'ID' y 'CONTENT'
        df.columns = df.columns.str.replace('.', '_')
        df_cleaned = df.dropna(subset=['content_display_display_id', 'content_display_content_id'], how='any')
        df_cleaned.rename(columns={'content_display_display_id': 'display', 'content_display_content_id': 'content'}, inplace=True)
        return df_cleaned

    def insert_data(df_contenido, df_display,df_content_data_filtrado):
        # Crear motor SQLAlchemy utilizando parámetros de conexión
        engine = create_engine(f'mssql+pyodbc:///?odbc_connect={connection_string}')
        
        eliminar_y_crear_tabla() 
        df_content_data_filtrado.to_sql('Global_LatinAD_Contenido_display', con=engine, if_exists='append',index=False,chunksize=5000)
       
             
        df_contenido.to_sql('Global_LatinAD_Contenido_data', con=engine, if_exists='append', index=False, chunksize=5000)
        df_display.to_sql('Global_LatinAD_Display_info', con=engine, if_exists='append', index=False)
        print('listo insert')
    
    def mezclar_columnas(row):
        return f"{row['content']}{row['display']}{row['Fecha']}"

    def eliminar_y_crear_tabla():
        Fecha_actual = datetime.now().date()
        fecha_inicio =  Fecha_actual - timedelta(days=25)
        fecha_fin =  Fecha_actual + timedelta(days=2)
            
        conn = pyodbc.connect(connection_string)
        
        cursor = conn.cursor()
        
        # Script SQL para eliminar la tabla si existe
        script_CONTENT = f"DELETE FROM Global_LatinAD_Contenido_data "

        script_display="""DELETE FROM Global_LatinAD_Display_info"""
        
        script_display_content = f"""DELETE FROM Global_LatinAD_Contenido_display
                                        WHERE Fecha BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
                                        """
        
        cursor.execute(script_CONTENT)
        cursor.execute(script_display)
        cursor.execute(script_display_content)

        print('se elimino y creo exitosamente')


        conn.commit()
        conn.close()   
            
    def run():
        start = time.time()
        ids,df_display = extraction_data_display()
        id_contenido,df_content = extraction_data_contenido()
        
        resultados, failed_ids = buscar_elemento(id_contenido, ids)
        print("IDs de contenido que fallaron:")
        print(failed_ids)
        
        # Concatenar todos los resultados en un único DataFrame
        df_final = pd.concat(resultados, ignore_index=True)

        #Dividir el DataFrame final en dos según las columnas especificadas y eliminarlas del original
        df_dividido, df_sin_columnas = split_and_remove_columns(df_final)
        
        #Eliminar filas vacías en las columnas 'display' y 'content' del DataFrame sin columnas
        df_sin_columnas_limpiado = remove_empty_rows(df_sin_columnas)
    
        df_merged = pd.merge(df_dividido, df_sin_columnas_limpiado, on=['display', 'content'], how='left')
        
        df_merged['id'] = df_merged.apply(mezclar_columnas, axis=1)
        df_merged = df_merged[df_merged['Fecha'] != '']
        df_merged = df_merged.dropna(axis=1, how='all')
        if 'content_display_rules' in df_merged.columns:
            df_merged.drop(columns=['content_display_rules'], inplace=True)
        df_merged['total_time'] = df_merged['total_time'] / 100

        insert_data(df_content,df_display,df_merged)
        
        print("Tiempo de ejecución:", (time.time()-start))
    
    run()
        