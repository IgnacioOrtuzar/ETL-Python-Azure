import logging
import azure.functions as func
import json
import pyodbc
import pandas as pd
import requests
import concurrent.futures
import dateutil
import urllib.parse
import os

from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import create_engine, text
from pandas import json_normalize
from datetime import datetime,timedelta


connection_string =  os.environ['Conection_string']
hdr = {'api-key': str(os.environ['api_key'])}


app = func.FunctionApp()

@app.schedule(schedule="0 */3 * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def New_Sercom_Api(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')
    parallel_fetch()
    logging.info('Python timer trigger function executed.')




def parallel_fetch():
        with ThreadPoolExecutor() as executor:
            # Ejecutar las funciones en paralelo
            futures = {
                #executor.submit(extract_data_turn): 'turns',
                #executor.submit(extract_data_project): 'projects',
                executor.submit(extraction_data_task): 'tasks',
                executor.submit(extract_data_element): 'element'
            }

            results = {}
            
            # Manejo de resultados a medida que se completan
            for future in as_completed(futures):
                try:
                    result = future.result()
                    results[futures[future]] = result
                except Exception as e:
                    print(f"Error al extraer {futures[future]}: {e}")
                    
        df_tasks, df_element =   results['tasks']  , results['element']
        
        df_projects = extract_data_project() 
        df_turns = extract_data_turn()
        
        df_tasks['team_id'] = df_tasks['team_id'].astype(pd.Int64Dtype())
        df_tasks['turn_id'] = df_tasks['turn_id'].astype(pd.Int64Dtype())
        df_tasks.fillna(0, inplace=True)
        
    
        insert_data(df_tasks,df_turns,df_projects,df_element)    

def extraction_data_task():
        # Base de la URL
        base_url = "https://sercom.dsy.cl/api/1/companies/1/tasks"

        # Parámetros de la consulta
        params = {
            "expand[]": [
                "task_detail",
                "task_list",
                "r_task_team",
                "team_list",
                "team_detail",
                "r_task_project",
                "project_list",
                "project_detail",
                "r_task_turn",
                "turn_list",
                "turn_detail",
                "r_turn_driver",
                "r_turn_worker_turn",
                "r_worker_turn_worker",
                "worker_list",
                "user_detail",
                "r_task_deleted_by_user"
            ],
            "deleted":"false",
            #"update_date": datetime.now().date() - timedelta(days=2)
            "assigned_from": "2025-04-04",
            "assigned_to": "2025-04-04"
        }

        # Crear la cadena de parámetros de consulta
        query_string = urllib.parse.urlencode(params, doseq=True)

        # Construir la URL final
        url_tareas = f"{base_url}?{query_string}"
        
        try:
            df_task = json_normalize(json.loads(requests.get(url_tareas, params=hdr).text))
            #hacer if
            
            df_task = trans_data(df_task)
            return df_task
        except Exception as e:
            print(f"Error para el proyecto : {str(e)}")

def extract_data_turn():
        url_tareas = f"""https://sercom.dsy.cl/api/1/companies/1/turns?expand[]=turn_detail&expand[]=r_turn_driver&expand[]=r_turn_worker_turn&expand[]=r_worker_turn_worker&expand[]=r_turn_team&expand[]=worker_list&expand[]=r_worker_turn_worker&expand[]=team_detail"""

        try:
            df_task = json_normalize(json.loads(requests.get(url_tareas, params=hdr).text))
            df_task = extract_worker_data_turns(df_task)
        
            return df_task
        except Exception as e:
            print(f"Error para Turnos  {str(e)}")
            
def extract_data_project():
        url_tareas = f"https://sercom.dsy.cl/api/1/companies/1/projects?expand[]=project_detail"

        try:
            df_task = json_normalize(json.loads(requests.get(url_tareas, params=hdr).text))
            df_task.rename(columns={'add':'CeCo'},inplace=True)
            return df_task
        except Exception as e:
            print(f"Error para Proyectos  {str(e)}")

def extract_data_element():
        url_tareas = f"https://sercom.dsy.cl/api/1/companies/1/elements?expand[]=element_detail&expand[]=element_list"
        
        try:
            df_task = json_normalize(json.loads(requests.get(url_tareas, params=hdr).text))
            df_task = df_task[['element_type_id','commune_name','id','name','latitude','longitude','address','deleted_at','enabled','external_id']]
            df_task['deleted_at'] = df_task['deleted_at'].apply(lambda x: dateutil.parser.parse(x).replace(tzinfo=None) if pd.notnull(x) else x)
            return df_task
        except Exception as e:
            print(f"Error para elementos {str(e)}")

def extract_data_task_last():
        engine = pyodbc.connect(connection_string)
        
            # Ajuste usando parámetros
        query = 'SELECT id,updated_at FROM Sercom_API_task'
        existing_ids = pd.read_sql_query(query, con=engine)
        return existing_ids

def extract_worker_data_turns(df_task):
        try:
            # Manejar el caso donde 'workers' pueda ser nulo o vacío
            if 'workers' in df_task and not df_task['workers'].isnull().all():
                df_result = json_normalize(df_task['workers'])
                df_result = df_result.iloc[:, :4]  # Asegúrate de que haya al menos 4 columnas
                df = pd.json_normalize(data=df_result.to_dict(orient='records'))
                columns = ['0.worker.name', '0.worker.rut', '1.worker.name', '1.worker.rut', '2.worker.name', '2.worker.rut', '3.worker.name', '3.worker.rut']
                
                # Filtra solo las columnas que existen en el DataFrame actual
                df = df[[col for col in columns if col in df.columns]]
                df_combined = pd.concat([df_task, df], axis=1)
                
                # Eliminar columnas innecesarias
                df_combined.drop(columns=['workers'], inplace=True)

                # Utilizando .str.split() para dividir por la letra "T" y quedarte solo con la parte anterior
                df_combined['date'] = df_combined['date'].str.split('T').str[0]
                
                df_combined = df_combined.rename(columns={'0.worker.name': 'worker_name_1','0.worker.rut':'worker_rut_1','1.worker.name': 'worker_name_2','1.worker.rut':'worker_rut_2','2.worker.name': 'worker_name_3','2.worker.rut':'worker_rut_3','3.worker.name': 'worker_name_4','3.worker.rut':'worker_rut_4'})
                df_combined = df_combined.rename(columns=lambda x: x.replace('.', '_'))            
                return df_combined
            else:
                print("No se encontraron datos de trabajadores en el turno.")
                return df_task
        except Exception as e:
            print(f"Error al extraer datos de trabajadores: {str(e)}")

def trans_data (df) : 
        
        df = df.rename(columns=lambda x: x.replace('.', '_'))
        df = df[['created_by_name', 'update_by_name', 'state_name', 'task_type_id',
        'task_type_name', 'element_id', 'project_id', 'id', 'description',
        'observations', 'assigned_at', 'started_at', 'original_started_at',
        'finished_at', 'original_finisched_at', 'created_at', 'updated_at',
            'project_name', 'project_header','team_name', 'team_members_name', 'team_id',
        'team_team_group', 'team_team_company', 'turn_id',
            'project_ot_number', 'project_central_title']]
        
        date_columns = ['original_started_at', 'original_finisched_at', 'assigned_at', 'started_at', 'finished_at', 'created_at', 'updated_at']

        # Filtrar las columnas de fecha y hora
        df_dates = df[date_columns].copy()

        # Convertir a formato de fecha y hora
        df_dates = df_dates.applymap(lambda x: dateutil.parser.parse(x).replace(tzinfo=None) if pd.notnull(x) else x)
            
        # Reemplazar las columnas originales con las convertidas
        df[date_columns] = df_dates
        
        df = df.loc[:, ~df.columns.duplicated()]
        # Asegurarte de que las columnas team_id y turn_id sean de tipo int

        return df

def separador_task(df_task):
        # Obtener los IDs existentes de la base de datos para comparar
        df_existencia =  extract_data_task_last() 
        # Convertir a set para búsqueda rápida
    
        # Combinar 'df_task' con 'df_existencia' basado en la columna 'id'
        df_task = df_task.merge(df_existencia[['id', 'updated_at']], on='id', how='left', suffixes=('', '_existente'))

        # Identificar registros nuevos (cuando 'updated_at_existente' es nulo)
        new_records = df_task[df_task['updated_at_existente'].isnull()]

        # Identificar registros actualizados (cuando 'updated_at' es mayor a 'updated_at_existente')
        df_to_update_filtered = df_task[df_task['updated_at'] > df_task['updated_at_existente']]

        # Eliminar la columna 'updated_at_existente' de los registros actualizados
        df_to_update_filtered = df_to_update_filtered.drop(columns=['updated_at_existente'])
        new_records = new_records.drop(columns=['updated_at_existente'])
        
        return new_records,df_to_update_filtered

def eliminar_Crear():
    conn = pyodbc.connect(connection_string)
        
    cursor = conn.cursor()
    script_turns ="""Delete from Sercom_API_turns """
    script_project ="""Delete from Sercom_API_project """ 
    script_element ="""Delete from Sercom_DSY_API_element """ 
        
    cursor.execute(script_turns)
    cursor.execute(script_project)
    cursor.execute(script_element)
    
    print('se elimino y creo exitosamente')


    conn.commit()
    conn.close()   
            
def insert_data(df_task, df_turn, df_project, df_element):
        
        engine = create_engine(f'mssql+pyodbc:///?odbc_connect={connection_string}')
   

        new_records_task, df_to_update_filtered_task = separador_task(df_task) 
        eliminar_Crear()
        
        try:
            with engine.begin() as connection:
                df_element.to_sql('Sercom_DSY_API_element', con=connection, schema='dbo', if_exists='append', index=False)
                print(f"Se ha completado la inserción/actualización en Sercom_API_element.{len(df_element)}")
                
                # Convertir df_turn a un formato que SQL Server pueda leer
                df_turn.to_sql('Sercom_API_turns', con=connection, schema='dbo', if_exists='append', index=False)
                print(f"Se ha completado la inserción/actualización en Sercom_API_turn.{len(df_turn)}")
                
                # Manejar la tabla Sercom_API_project
                df_project.to_sql('Sercom_API_project', con=connection, schema='dbo', if_exists='append', index=False)
                print(f"Se ha completado la inserción/actualización en Sercom_API_project.{len(df_project)}")

              
        

                if not new_records_task.empty:
                    new_records_task.to_sql('Sercom_API_task', con=connection, if_exists='append', index=False)
                    print(f"{len(new_records_task)} registros nuevos insertados en Sercom_API_task.")

                if not df_to_update_filtered_task.empty:
                    paralel_data(df_to_update_filtered_task)
                    print(f"{len(df_to_update_filtered_task)} registros actualizados en Sercom_API_task.")

                print("Inserción y actualización completadas exitosamente.")

        except Exception as e:
            print(f"Error en la inserción o actualización: {str(e)}")

def paralel_data(df_final):

        # Obtén la cantidad total de filas en el DataFrame
        total_rows = len(df_final)

        # Divide el DataFrame en N partes (aquí, asumimos que hay 4 particiones)
        num_partitions = os.cpu_count() or 4
        partition_size = (total_rows + num_partitions - 1) // num_partitions 

        # Ejecuta actualizaciones en paralelo usando ThreadPoolExecutor
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_partitions) as executor:
            futures = []
            for i in range(num_partitions):
                start = i * partition_size
                end = start + partition_size if i < num_partitions - 1 else None

                # Obtén la subsección del DataFrame
                df_subset = df_final.iloc[start:end]

                # Agrega la tarea al executor
                futures.append(executor.submit(update_data_task, df_subset))

            # Espera a que todas las tareas se completen
            concurrent.futures.wait(futures)

def update_data_task(df_task_update):
        # Actualizar tareas
        data_to_update_task = [(row['created_by_name'], row['update_by_name'], row['state_name'],
                    row['task_type_id'], row['task_type_name'], row['element_id'],
                    row['project_id'], row['description'], row['observations'],
                    row['assigned_at'], row['started_at'], row['original_started_at'],
                    row['finished_at'], row['original_finisched_at'], row['created_at'], row['updated_at'],
                    row['project_name'], row['project_header'], row['team_name'], row['team_members_name'],
                    row['team_id'], row['team_team_group'], row['team_team_company'], row['turn_id'],
                    row['project_ot_number'], row['project_central_title'], row['id'])
                    for _, row in df_task_update.iterrows()]

        update_sql_task = '''
            UPDATE Sercom_API_task
            SET created_by_name = ?,
                update_by_name = ?,
                state_name = ?,
                task_type_id = ?,
                task_type_name = ?,
                element_id = ?,
                project_id = ?,
                description = ?,
                observations = ?,
                assigned_at = ?,
                started_at = ?,
                original_started_at = ?,
                finished_at = ?,
                original_finisched_at = ?,
                created_at= ?,
                updated_at= ?,
                project_name = ?,
                project_header = ?,
                team_name = ?,
                team_members_name = ?,
                team_id = ?,
                team_team_group = ?,
                team_team_company = ?,
                turn_id = ?,
                project_ot_number = ?,
                project_central_title = ?
            WHERE id = ?
        '''

        connection_string = os.environ['Conection_string']
        cnxn = pyodbc.connect(connection_string)

        # Crear un cursor
        cursor = cnxn.cursor()

        # Ejecutar la consulta de actualización en bloque
        cursor.executemany(update_sql_task, data_to_update_task)
        print('Listo', len(data_to_update_task))

        # Confirmar la transacción y cerrar la conexión
        cnxn.commit()
        cursor.close()
        cnxn.close()
            


       


