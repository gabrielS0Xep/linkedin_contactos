from datetime import datetime, date
from math import log
import os
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

    def __init__(self, project:str, dataset:str) -> None:
        self.__project_id = project
        self.__dataset = dataset
        self.__bq_client = bigquery.Client(project=self.__project_id) 
            

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
            print(f"🗑️ Tabla corrupta eliminada: {dataset_id}.{table_id}")
        except Exception as e:
            print(f"⚠️ Error eliminando tabla (puede no existir): {e}")

        try:
            # Crear la tabla nueva con schema correcto
            table = bigquery.Table(table_ref, schema=schema)
            table = client.create_table(table)
            print(f"✅ Tabla de control {dataset_id}.{table_id} creada exitosamente con schema correcto")
        except Exception as e:
            print(f"❌ Error creando tabla: {e}")

    def crear_tabla_linkedin_contacts_info():
        """Crea la tabla linkedin_contacts_info si no existe"""

        client = bigquery.Client(project="xepelin-lab-customer-mx")
        dataset_id = 'raw_in_scrapper_contacts'
        table_id = 'linkedin_contacts_info_personas'

        # Schema simplificado para contactos
        schema = [
            bigquery.SchemaField("biz_identifier", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("biz_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("full_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("role", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("web_linkedin_url", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("ai_score", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("src_scraped_data", "JSON", mode="NULLABLE"),  # JSON como STRING
            bigquery.SchemaField("src_scraped_dt", "TIMESTAMP", mode="REQUIRED")
        ]

        # Crear referencia a la tabla
        dataset_ref = client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)

        try:
            # Intentar obtener la tabla (si existe)
            client.get_table(table_ref)
            print(f"✅ Tabla de datos {dataset_id}.{table_id} ya existe")
        except:
            # Si no existe, crearla
            table = bigquery.Table(table_ref, schema=schema)
            table = client.create_table(table)
            print(f"✅ Tabla de datos {dataset_id}.{table_id} creada exitosamente")

# esta muy acoplado a scraper
    def marcar_empresas_contacts_como_scrapeadas(self,scraper):
        """Marca las empresas como scrapeadas en la tabla empresas_scrapeadas_linkedin_contacts"""

        if not scraper.test_metrics['companies_processed']:
            print("⚠️ No hay empresas para marcar como scrapeadas")
            return

        client = self.__bq_client
        dataset_id = self.__dataset
        table_id = Config.CONTROL_TABLE_NAME

        # Preparar datos para insertar
        datos_insertar = []
        timestamp_actual = datetime.now()

        for company_name in scraper.test_metrics['companies_processed']:
            # Obtener biz_identifier del mapeo
            biz_identifier = scraper.company_biz_mapping.get(company_name, '')

            # Verificar si encontró contactos válidos
            contactos_empresa = [c for c in scraper.contacts_results if c['biz_name'] == company_name]
            encontro_linkedin = len(contactos_empresa) > 0

            datos_insertar.append({
                'biz_identifier': biz_identifier,
                'biz_name': company_name,
                'scrapping_d': timestamp_actual,
                'contact_found_flg': encontro_linkedin
            })

        # Convertir a DataFrame
        df_empresas = pd.DataFrame(datos_insertar)

        if not df_empresas.empty:
            try:
                # Insertar en BigQuery
                destination_table = f'{scraper.project_id}.{dataset_id}.{table_id}'

                df_empresas.to_gbq(
                    destination_table=destination_table,
                    project_id=scraper.project_id,
                    if_exists='append'
                )

                print(f"✅ Marcadas {len(df_empresas)} empresas como scrapeadas en BigQuery")
                print(f"   📊 Con contactos encontrados: {len([d for d in datos_insertar if d['encontro_linkedin']])}")
                print(f"   📊 Sin contactos: {len([d for d in datos_insertar if not d['encontro_linkedin']])}")

            except Exception as e:
                print(f"❌ Error marcando empresas como scrapeadas: {e}")
        else:
            print("⚠️ No hay datos para marcar como scrapeadas")

    def save_contacts_to_bigquery(self,contacts_results):
        """Guardar contactos en la tabla linkedin_contacts_info"""
        if not contacts_results:
            print(f"⚠️ No hay contactos para guardar")
            return None

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"linkedin_contacts_{timestamp}.csv"
        """
        No me interesa guardar CSV localmente ahora, solo subo a BigQuery por ahi lo subo a GCS
        # Guardar CSV local primero
        fieldnames = [
            'biz_identifier', 'biz_name', 'contact_name', 'contact_position',
            'linkedin_profile_url', 'ai_score', 'scraped_data', 'scraped_at'
        ]

        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(scraper.contacts_results)

        print(f"💾 Contactos guardados localmente en: {filename}")
        """
       
        # Subir a BigQuery
        try:
            df_contacts = pd.DataFrame(contacts_results)
        # Acoplado a scraper
            if not df_contacts.empty:
                # Limpiar datos para BigQuery
                print("🔧 Aplicando limpieza para BigQuery...")

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


                # Convertir timestamp
                if 'scraped_at' in df_contacts.columns:
                    df_contacts['scraped_at'] = pd.to_datetime(df_contacts['scraped_at'])

                table_id = Config.LINKEDIN_INFO_TABLE_NAME
                destination_table = f'{self.__project_id}.{self.__dataset}.{table_id}'

                # Subir usando pandas-gbq
                df_contacts.to_gbq(
                    destination_table=destination_table,
                    project_id=scraper.project_id,
                    if_exists='append',
                    table_schema=None,
                    location='US',
                    progress_bar=False
                )

                print(f"📊 Contactos subidos a BigQuery: {destination_table}")
                print(f"📈 Registros guardados: {len(df_contacts)}")
            else:
                print("⚠️ No hay contactos para subir a BigQuery")

        except Exception as e:
            print(f"❌ Error subiendo contactos a BigQuery: {e}")
            print("💾 Los datos se guardaron localmente en CSV")

        return filename

    def load_companies_from_bigquery_linkedin_contacts(self) -> List[Dict]:
        """Ejecuta query en BigQuery y extrae nombres de empresa y biz_identifier - CON CONTROL DE DUPLICADOS PARA CONTACTS"""

        query = """
        SELECT distinct biz_identifier, biz_name as name, biz_archetype_cluster1_prob FROM `lib-cdp-mx.trf_businesses.vw_businesses_opportunities`
        WHERE cust_type = 'PM'
        AND prohibited_industries_filtered_words IS NULL
        AND list_blocklist_flg = 0
        AND prohibited_scian_code_flg = 0
        AND archived = FALSE
        AND isEnrolled IS NULL
        AND enrollmentDate IS NULL
        AND activationDate IS NULL
        AND biz_lead_quality_cat IN ('4.Alta', '3.Media')
        AND COALESCE(flag_whatsapp,0) = 0
        ORDER BY biz_archetype_cluster1_prob DESC
        LIMIT 10
            """

        query_real = f"""
        SELECT * FROM `{self.__project_id}.{self.__dataset}.{Config.CONTROL_TABLE_NAME}`
        WHERE contact_found_flg = FALSE AND scrapping_d is null
        """

        try:
            client = self.__bq_client
            results = client.query(query).to_dataframe()

            # Extraer nombres y biz_identifier, limpiar valores nulos
            companies = []
            for _, row in results.iterrows():
                if pd.notna(row['name']) and str(row['name']).strip():
                    companies.append({
                        'name': str(row['name']).strip(),
                        'biz_identifier': str(row['biz_identifier']).strip() if pd.notna(row['biz_identifier']) else None
                    })

            total_filtradas = len(companies)

            print(f"✅ Query ejecutada exitosamente")
            print(f"📊 Empresas SIN scrappear contactos en LinkedIn: {total_filtradas}")

            return companies

        except Exception as e:
            print(f"❌ Error ejecutando query BigQuery: {e}")
            print("💡 Verifica que las tablas existan y tengas permisos")
            return []