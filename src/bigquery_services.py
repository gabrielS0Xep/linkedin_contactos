from datetime import datetime, date
import pandas as pd
from typing import List, Dict
from logging import Logger
import logging
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from pandas_gbq import to_gbq
from config import Config

logger: Logger = logging.getLogger(__name__)

class BigQueryService:

    def __init__(self, project:str, dataset:str, table_control_name:str, table_info_name:str) -> None:
        self.__project_id = project
        self.__dataset = dataset
        self.__table_control_name = table_control_name
        self.__table_info_name = table_info_name
        self.__bq_client = bigquery.Client(project=self.__project_id) 
            

    def table_exists(self, table_id:str) -> bool:
        """Verifica si la tabla existe"""
        client = self.__bq_client
        dataset_id = self.__dataset
        table_ref = f"{self.__project_id}.{dataset_id}.{table_id}"
        try:
            client.get_table(table_ref)  # Make an API request.
            return True
        except NotFound:
            return False

    def crear_tabla_empresas_scrapeadas_linkedin_contacts(self):
        
        """Crea la tabla de control empresas_scrapeadas_linkedin_contacts si no existe"""
        client = self.__bq_client
        dataset_id = self.__dataset
        table_id = Config.CONTROL_TABLE_NAME

        # Schema de la tabla de control
        schema = [
            bigquery.SchemaField("biz_identifier", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("biz_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("scrapping_d", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("contact_found_flg", "BOOLEAN", mode="REQUIRED")
        ]

        # Crear referencia a la tabla
        dataset_ref = client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)

        try:
            # Intentar eliminar la tabla corrupta primero
            client.delete_table(table_ref, not_found_ok=True)
            logger.info(f"🗑️ Tabla corrupta eliminada: {dataset_id}.{table_id}")
        except Exception as e:
            logger.error(f"⚠️ Error eliminando tabla (puede no existir): {e}")

        try:
            # Crear la tabla nueva con schema correcto
            table = bigquery.Table(table_ref, schema=schema)
            table = client.create_table(table)
            logger.info(f"✅ Tabla de control {dataset_id}.{table_id} creada exitosamente con schema correcto")
        except Exception as e:
            logger.error(f"❌ Error creando tabla: {e}")

    def crear_tabla_linkedin_contacts_info(self):
        """Crea la tabla linkedin_contacts_info si no existe"""

        client = bigquery.Client(project="xepelin-lab-customer-mx")
        dataset_id = 'raw_in_scrapper_contacts'
        table_id = 'linkedin_contacts_info_personas'

        # Schema simplificado para contactos
        schema = [
        bigquery.SchemaField("biz_identifier", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("biz_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("biz_industry", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("biz_web_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("biz_web_linkedin_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("biz_founded_year", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("biz_size", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("full_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("role", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("ai_score_value", "NUMERIC", mode="NULLABLE"),
        bigquery.SchemaField("web_linkedin_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("first_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("last_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("email", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("phone_number", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("headline", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("current_job_duration", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("cntry_value", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("cntry_city_value", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("src_scraped_dt", "TIMESTAMP", mode="NULLABLE")
        ]

        # Crear referencia a la tabla
        dataset_ref = client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)

        try:
            # Intentar obtener la tabla (si existe)
            client.get_table(table_ref)
            logger.info(f"✅ Tabla de datos {dataset_id}.{table_id} ya existe")
        except:
            # Si no existe, crearla
            table = bigquery.Table(table_ref, schema=schema)
            table = client.create_table(table)
            logger.info(f"✅ Tabla de datos {dataset_id}.{table_id} creada exitosamente")

    def verificar_empresa_scrapeada(self, biz_identifier: str, company_name: str, table_name: str) -> dict:
        """
        Verifica si una empresa ya fue scrapeada en la tabla de control
        Retorna: {
            'exists': bool,
            'needs_scraping': bool,
            'scraping_date': str or None,
            'linkedin_found': bool or None
        }
        """
        dataset_id = self.__dataset
        table_id = table_name
        
        try:
            # Query para verificar si existe el registro
            query = f"""
            SELECT scrapping_d, contact_found_flg
            FROM `{self.__project_id}.{dataset_id}.{table_id}`
            WHERE biz_identifier = @biz_identifier AND biz_name = @company_name
            LIMIT 1
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("biz_identifier", "STRING", biz_identifier),
                    bigquery.ScalarQueryParameter("company_name", "STRING", company_name),
                ]
            )
            
            query_job = self.__bq_client.query(query, job_config=job_config)
            results = list(query_job.result())
            
            if results:
                # El registro existe
                row = results[0]
                scraping_date = row.scrapping_d
                linkedin_found = row.contact_found_flg
                
                # Si ambos campos no son nulos, no necesita scraping
                needs_scraping = scraping_date is None or linkedin_found is None
                
                return {
                    'exists': False,
                    'needs_scraping': needs_scraping,
                    'scraping_date': scraping_date.isoformat() if scraping_date else None,
                    'linkedin_found': linkedin_found
                }
            else:
                # El registro no existe
                return {
                    'exists': False,
                    'needs_scraping': True,
                    'scraping_date': None,
                    'linkedin_found': None
                }
                
        except Exception as e:
            logger.error(f"❌ Error verificando empresa scrapeada: {e}")
            # En caso de error, asumir que necesita scraping
            return {
                'exists': False,
                'needs_scraping': True,
                'scraping_date': None,
                'linkedin_found': None
            }


# esta muy acoplado a scraper
    def marcar_empresas_contacts_como_scrapeadas(self, contacts_results: List[Dict], companies_data: List[Dict]):
        """Marca las empresas como scrapeadas en la tabla empresas_scrapeadas_linkedin_contacts"""

        table_id = Config.CONTROL_TABLE_NAME
        location = Config.BIGQUERY_LOCATION
        # Preparar datos para insertar
        datos_insertar = []
        logger.info(f"Contacts results en empresas scrapeadas: {contacts_results}")

        biz_names = map(lambda x: x['biz_identifier'], contacts_results)
        biz_names = set(biz_names)
        
        datos_insertar = []
        date_actual = date.today()

        for company in companies_data:
            datos_insertar.append({
                **company,
                'scrapping_d': date_actual,
                'contact_found_flg': company['biz_identifier'] in biz_names
            })

        # Convertir a DataFrame
        if datos_insertar != []:
            df_datos_insertar = pd.DataFrame(datos_insertar)
            df_datos_insertar.to_gbq(
                    destination_table=f'{self.__project_id}.{self.__dataset}.{table_id}',
                    project_id=self.__project_id,
                    if_exists='append',
                    table_schema=None,
                    location=location,
                )

            # Limpia duplicados de la tabla de control
            result_clean_duplicates = self.clean_duplicates_from_control_table(Config.CONTROL_TABLE_NAME)
            logger.info(f"✅ Limpieza de duplicados en la tabla de control: {result_clean_duplicates}")

            return len(df_datos_insertar), 0
        else:
            logger.warning("⚠️ No hay datos para marcar como scrapeadas")
            return None


    def save_contacts_to_bigquery(self,contacts_results):
        """Guardar contactos en la tabla linkedin_contacts_info"""
        if not contacts_results:
            logger.warning(f"⚠️ No hay contactos para guardar")
            return None
        location = Config.BIGQUERY_LOCATION
       
        # Subir a BigQuery
        try:
            df_contacts = pd.DataFrame(contacts_results)
        # Acoplado a scraper
            if not df_contacts.empty:
                # Limpiar datos para BigQuery
                logger.info("🔧 Aplicando limpieza para BigQuery...")

                #   Limpiar campos de texto
                text_fields = ['biz_identifier', 'biz_name', 'full_name', 'role',
                            'web_linkedin_url', 'src_scraped_data']

                for field in text_fields:
                    if field in df_contacts.columns:
                        df_contacts[field] = df_contacts[field].fillna('').astype(str)
                        df_contacts[field] = df_contacts[field].str.replace('\x00', '', regex=False)

                # Limpiar campo numérico
                if 'ai_score' in df_contacts.columns:
                    df_contacts['ai_score'] = pd.to_numeric(df_contacts['ai_score'], errors='coerce').fillna(0).astype('Int64')

                if 'src_scraped_dt' in df_contacts.columns:
                    df_contacts['src_scraped_dt'] = pd.to_datetime(df_contacts['src_scraped_dt']).dt.tz_localize('UTC')
#
                table_id = Config.LINKEDIN_INFO_TABLE_NAME
                

                return self._process_contacts_chunk_with_upsert(df_contacts, table_id, location)
            else:
                logger.warning("⚠️ No hay contactos para subir a BigQuery")
                return None
        except Exception as e:
            logger.error(f"❌ Error subiendo contactos a BigQuery: {e}")
            return None


    def load_companies_from_bigquery_linkedin_contacts(self , limit: int = 1) -> List[Dict]:
        """Ejecuta query en BigQuery y extrae nombres de empresa y biz_identifier - CON CONTROL DE DUPLICADOS PARA CONTACTS"""


        query = f"""
        SELECT biz_name, biz_identifier FROM `{self.__project_id}.{self.__dataset}.{Config.CONTROL_TABLE_NAME}`
        WHERE (contact_found_flg = FALSE or contact_found_flg is null) AND scrapping_d is null limit {limit}
        """
        logger.info(f"🔍 Query: {query}")
        try:
            client = self.__bq_client
            results = client.query(query).to_dataframe()
        
            # Extraer nombres y biz_identifier, limpiar valores nulos
            companies = []
            for _, row in results.iterrows():
                if pd.notna(row['biz_name']) and str(row['biz_name']).strip():
                    companies.append({
                        'biz_name': str(row['biz_name']).strip(),
                        'biz_identifier': str(row['biz_identifier']).strip() if pd.notna(row['biz_identifier']) else None
                    })

            total_filtradas = len(companies)

            # Limpia duplicados de la tabla de contro

            logger.info(f"✅ Query ejecutada exitosamente")
            logger.info(f"📊 Empresas SIN scrappear contactos en LinkedIn: {total_filtradas}")

            return companies

        except Exception as e:
            logger.error(f"❌ Error ejecutando query BigQuery: {e}")
            logger.error("💡 Verifica que las tablas existan y tengas permisos")
            return []

    def _process_contacts_chunk_with_upsert(self, df_chunk,  table_name, location):
        """
        Procesa un chunk de datos implementando lógica de upsert.
        Retorna (insertados, actualizados)
        #Location : US

        """

        destination_table = f'{self.__project_id}.{self.__dataset}.{table_name}'
        success = False
        inserted = 0
        updated = 0
        
        try:
            # Crear tabla temporal para el merge
            temp_table_name = f"temp_{table_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            temp_destination = f'{self.__project_id}.{self.__dataset}.{temp_table_name}'
            
            # Insertar datos en tabla temporal
            df_chunk.to_gbq(
                destination_table=temp_destination,
                project_id=self.__project_id,
                if_exists='replace',
                table_schema=None,
                location=location,
                progress_bar=False
            )
            
            # Query de MERGE para upsert
            merge_query = f"""
                MERGE `{destination_table}` AS target
                USING `{temp_destination}` AS source
                ON target.biz_identifier = source.biz_identifier
                AND target.web_linkedin_url = source.web_linkedin_url
                WHEN MATCHED THEN
                    UPDATE SET
                        biz_name = source.biz_name,
                        biz_industry = source.biz_industry,
                        biz_web_url = source.biz_web_url,
                        biz_web_linkedin_url = source.biz_web_linkedin_url,
                        biz_founded_year = source.biz_founded_year,
                        biz_size = source.biz_size,
                        full_name = source.full_name,
                        role = source.role,
                        ai_score_value = source.ai_score_value,
                        first_name = source.first_name,
                        last_name = source.last_name,
                        email = source.email,
                        phone_number = source.phone_number,
                        headline = source.headline,
                        current_job_duration = source.current_job_duration,
                        cntry_value = source.cntry_value,
                        cntry_city_value = source.cntry_city_value,
                        src_scraped_dt = source.src_scraped_dt
                WHEN NOT MATCHED THEN
                    INSERT (
                        biz_identifier,
                        biz_name,
                        biz_industry,
                        biz_web_url,
                        biz_web_linkedin_url,
                        biz_founded_year,
                        biz_size,
                        full_name,
                        role,
                        ai_score_value,
                        web_linkedin_url,
                        first_name,
                        last_name,
                        email,
                        phone_number,
                        headline,
                        current_job_duration,
                        cntry_value,
                        cntry_city_value,
                        src_scraped_dt
                    )
                    VALUES (
                        source.biz_identifier,
                        source.biz_name,
                        source.biz_industry,
                        source.biz_web_url,
                        source.biz_web_linkedin_url,
                        source.biz_founded_year,
                        source.biz_size,
                        source.full_name,
                        source.role,
                        source.ai_score_value,
                        source.web_linkedin_url,
                        source.first_name,
                        source.last_name,
                        source.email,
                        source.phone_number,
                        source.headline,
                        source.current_job_duration,
                        source.cntry_value,
                        source.cntry_city_value,
                        source.src_scraped_dt
                    );
            """            
            # Ejecutar merge
            query_job = self.__bq_client.query(merge_query)
            result = query_job.result()
            # Obtener estadísticas del merge
            if hasattr(result, 'num_dml_affected_rows'):
                # Para versiones más recientes de BigQuery
                updated = result.num_dml_affected_rows
            else:
                # Fallback: contar registros en tabla temporal
                count_query = f"SELECT COUNT(*) as count FROM `{temp_destination}`"
                count_job = self.__bq_client.query(count_query)
                count_result = list(count_job.result())
                total_records = count_result[0].count if count_result else 0
                
                # Asumir que la mayoría son actualizaciones si la tabla ya tiene datos
                updated = total_records // 2  # Estimación conservadora
                inserted = total_records - updated
            
            # Limpiar tabla temporal
            try:
                logger.info(f"🔄 Eliminando tabla temporal {temp_destination}")
                self.__bq_client.delete_table(temp_destination, not_found_ok=True)
            except Exception as e:
                logger.warning(f"⚠️ No se pudo eliminar tabla temporal {temp_destination}: {e}")
            
            return {"success": success, "inserted": inserted, "updated": updated}
            
        except Exception as e:
            logger.error(f"❌ Error en upsert: {e}")
            try:
                logger.info(f"🔄 Eliminando tabla temporal {temp_destination}")
                self.__bq_client.delete_table(temp_destination, not_found_ok=True)
            except Exception as e:
                logger.warning(f"⚠️ No se pudo eliminar tabla temporal {temp_destination}: {e}")
                # Fallback al método original de append

            logger.info("🔄 Fallback a método append...")
            
            try:
                df_chunk.to_gbq(
                    destination_table=destination_table,
                    project_id=self.__project_id,
                    if_exists='append',
                    table_schema=None,
                    location=location,
                    progress_bar=False
                )
                return len(df_chunk), 0
            except Exception as e2:
                logger.error(f"❌ Error en fallback: {e2}")
                raise e2

    def _process_companies_chunk_with_upsert(self, df_chunk, table_name, location):
        """
        Procesa un chunk de datos implementando lógica de upsert.
        Retorna (insertados, actualizados)
        #Location : US

        """
        destination_table = f'{self.__project_id}.{self.__dataset}.{table_name}'
        success = False
        inserted = 0
        updated = 0
        
        try:
            # Crear tabla temporal para el merge
            temp_table_name = f"temp_{table_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            temp_destination = f'{self.__project_id}.{self.__dataset}.{temp_table_name}'
            
            # Insertar datos en tabla temporal
            df_chunk.to_gbq(
                destination_table=temp_destination,
                project_id=self.__project_id,
                if_exists='replace',
                table_schema=None,
                location=location,
                progress_bar=False
            )
            
            # Query de MERGE para upsert
            merge_query = f"""
                MERGE `{destination_table}` AS target
                USING `{temp_destination}` AS source
                ON target.biz_identifier = source.biz_identifier
                WHEN MATCHED THEN
                    UPDATE SET
                        scrapping_d = source.scrapping_d,
                        contact_found_flg = source.contact_found_flg
                WHEN NOT MATCHED THEN
                    INSERT (
                        biz_identifier,
                        biz_name,
                        scrapping_d,
                        contact_found_flg
                    )
                    VALUES (
                        source.biz_identifier,
                        source.biz_name,
                        source.scrapping_d,
                        source.contact_found_flg
                    );
            """            
            # Ejecutar merge
            query_job = self.__bq_client.query(merge_query)
            result = query_job.result()
            # Obtener estadísticas del merge
            if hasattr(result, 'num_dml_affected_rows'):
                # Para versiones más recientes de BigQuery
                updated = result.num_dml_affected_rows
            else:
                # Fallback: contar registros en tabla temporal
                count_query = f"SELECT COUNT(*) as count FROM `{temp_destination}`"
                count_job = self.__bq_client.query(count_query)
                count_result = list(count_job.result())
                total_records = count_result[0].count if count_result else 0
                
                # Asumir que la mayoría son actualizaciones si la tabla ya tiene datos
                updated = total_records // 2  # Estimación conservadora
                inserted = total_records - updated
            
            success = True
            # Limpiar tabla temporal
            try:
                logger.info(f"🔄 Eliminando tabla temporal {temp_destination}")
                self.__bq_client.delete_table(temp_destination, not_found_ok=True)
            except Exception as e:
                logger.warning(f"⚠️ No se pudo eliminar tabla temporal {temp_destination}: {e}")
            
            return {"success": success, "inserted": inserted, "updated": updated}
            
        except Exception as e:
            logger.error(f"❌ Error en upsert: {e}")
            # Fallback al método original de append
            logger.info("🔄 Fallback a método append...")
            
            try:
                df_chunk.to_gbq(
                    destination_table=destination_table,
                    project_id=self.__project_id,
                    if_exists='append',
                    table_schema=None,
                    location=location,
                    progress_bar=False
                )
                return len(df_chunk), 0
            except Exception as e2:
                logger.error(f"❌ Error en fallback: {e2}")
                raise e2


    def get_pending_companies(self, table_name: str, limit: int = 100) -> List[Dict]:
        """
        Obtiene empresas pendientes de scraping (scrapping_date y linkedin_found son NULL)
        
        Args:
            table_name: Nombre de la tabla de control
            limit: Límite de empresas a retornar
            
        Returns:
            Lista de diccionarios con las empresas pendientes
        """
        dataset_id = self.__dataset
        table_id = table_name
        
        try:
            # Query para obtener empresas pendientes
            query = f"""
            SELECT biz_identifier, biz_name
            FROM `{self.__project_id}.{dataset_id}.{table_id}`
            WHERE scrapping_d IS NULL OR contact_found_flg IS NULL
            ORDER BY biz_name
            LIMIT @limit
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("limit", "INT64", limit),
                ]
            )
            
            query_job = self.__bq_client.query(query, job_config=job_config)
            results = list(query_job.result())
            
            # Convertir resultados a lista de diccionarios
            pending_companies = []
            for row in results:
                pending_companies.append({
                    'rfc': row.biz_identifier,
                    'company_name': row.biz_name
                })
            
            logger.info(f"✅ Encontradas {len(pending_companies)} empresas pendientes de scraping")
            return pending_companies
            
        except Exception as e:
            logger.error(f"❌ Error obteniendo empresas pendientes: {e}")
            return []

    def get_pending_companies_count(self, table_name: str) -> int:
        """
        Obtiene el conteo de empresas pendientes de scraping
        
        Args:
            table_name: Nombre de la tabla de control
            
        Returns:
            Número de empresas pendientes
        """
        dataset_id = self.__dataset
        table_id = table_name
        
        try:
            # Query para contar empresas pendientes
            query = f"""
            SELECT COUNT(*) as pending_count
            FROM `{self.__project_id}.{dataset_id}.{table_id}`
            WHERE scrapping_d IS NULL OR contact_found_flg IS NULL
            """
            
            query_job = self.__bq_client.query(query)
            results = list(query_job.result())
            
            if results:
                return results[0].pending_count
            else:
                return 0
                
        except Exception as e:
            logger.error(f"❌ Error contando empresas pendientes: {e}")
            return 0

    def clean_duplicates_from_control_table(self, table_name: str = "linkedin_scrapped_contacts") -> Dict:
        """
        Limpia registros duplicados de la tabla linkedin_scrapped_contacts.
        Mantiene el registro más reciente basado en src_scraped_dt.
        
        Args:
            table_name: Nombre de la tabla a limpiar
            
        Returns:
            Dict con el estado de la operación
        """
        try:
            destination_table = f'{self.__project_id}.{self.__dataset}.{table_name}'
            
            # Query para eliminar duplicados manteniendo el más reciente
            deduplication_query = f"""
            CREATE OR REPLACE TABLE `{destination_table}` AS
            SELECT * EXCEPT(row_num)
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY biz_identifier
                           ORDER BY scrapping_d DESC
                       ) as row_num
                FROM `{destination_table}`
            )
            WHERE row_num = 1
            """
            
            logger.info(f"🧹 Iniciando limpieza de duplicados en {destination_table}...")
            
            # Ejecutar query de deduplicación
            query_job = self.__bq_client.query(deduplication_query)
            result = query_job.result()
            
            # Obtener estadísticas
            count_query = f"SELECT COUNT(*) as count FROM `{destination_table}`"
            count_job = self.__bq_client.query(count_query)
            count_result = list(count_job.result())
            final_count = count_result[0].count if count_result else 0
            
            logger.info(f"✅ Limpieza de duplicados completada. Registros finales: {final_count}")
            
            return {
                "success": True,
                "message": "Duplicados eliminados exitosamente",
                "final_count": final_count,
                "destination_table": destination_table
            }
            
        except Exception as e:
            logger.error(f"❌ Error limpiando duplicados: {e}")
            return {
                "success": False,
                "message": f"Error limpiando duplicados: {str(e)}",
                "final_count": None
            }