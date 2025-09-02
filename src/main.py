from flask import Flask, request, jsonify
from flask_cors import CORS
from config import Config
import logging
from bigquery_services import BigQueryService
from google import genai
from google.genai import types

from typing import List, Dict, Tuple
from apify_client import ApifyClient

import re
from urllib.parse import quote
from datetime import datetime
import csv
import pandas as pd
from google.cloud import bigquery
from concurrent.futures import ThreadPoolExecutor, as_completed
from pandas_gbq import to_gbq
from config import Config
from bigquery_services import BigQueryService

from linkedin_contacts_scrapper import LinkedInContactsSelectiveScraper
from secret_manager_service import SecretManager




logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Crear aplicación Flask
app = Flask(__name__)
CORS(app)  # Habilitar CORS para requests cross-origin

bigquery_service = None
secret_manager = None


def get_services():
    try: 
        # Inicializar BigQuery service
        bigquery_service = BigQueryService(
            project=Config.GOOGLE_CLOUD_PROJECT_ID,
            dataset=Config.BIGQUERY_DATASET
        )
        secret_manager = SecretManager(project=Config.GOOGLE_CLOUD_PROJECT_ID)
        logger.info("✅ Servicios inicializados correctamente")
    except Exception as e:
        logger.error(f"❌ Error inicializando servicios: {e}")
        raise

    return bigquery_service , secret_manager


@app.route("/status", methods=['GET'])
def health_check():
    return {"status": "OK"}



@app.route("/companies", methods=['POST'])
def companies():
    data = request.json

    companies = data.get('companies')

    return jsonify({"message": "Proceso completado exitosamente , companies: " + companies}), 200


@app.route("/scrape", methods=['GET'])
def scrape():

# ================================
# FUNCIONES PARA CONTROL DE SCRAPPING - ADAPTADAS PARA CONTACTS
# =============================
    # 🔑 API KEYS CONFIGURADAS
    bigquery_service,secret_manager_services = get_services()
    SERPER_API_KEY  = secret_manager_services.get_secret('api_key_serper_linkedin_contactos')
    # 🆕 API KEY DE APIFY
    APIFY_TOKEN = secret_manager_services.get_secret('apify_token')
    
    # PASO 0: Crear tablas si no existen
    if not bigquery_service.table_exists(Config.CONTROL_TABLE_NAME):
        bigquery_service.crear_tabla_empresas_scrapeadas_linkedin_contacts()
    if not bigquery_service.table_exists(Config.LINKEDIN_INFO_TABLE_NAME):
        bigquery_service.crear_tabla_linkedin_contacts_info()

    # Cargar empresas desde BigQuery (SIN las ya scrapeadas)
    companies_data = bigquery_service.load_companies_from_bigquery_linkedin_contacts()

    if not companies_data:
        logger.error("❌ No se pudieron cargar empresas desde BigQuery o todas ya fueron scrapeadas. ")
        return jsonify(
            {"error": "No se pudieron cargar empresas desde BigQuery o todas ya fueron scrapeadas. "}
            ), 400

    # Extraer nombres y crear mapeo de biz_identifier
    companies = [company['biz_name'] for company in companies_data]
    company_biz_mapping = {company['biz_name']: company['biz_identifier'] for company in companies_data}

    scraper = LinkedInContactsSelectiveScraper(SERPER_API_KEY, APIFY_TOKEN)
    # Configurar mapeo de biz_identifier
    scraper.set_company_biz_mapping(company_biz_mapping)

    print("🚀 LINKEDIN CONTACTS SCRAPER PARA XEPELIN ACTIVADO")
    print("="*80)
    print("🏢 Project: xepelin-lab-customer-mx")
    print("📊 Dataset: raw_in_scrapper_contacts")
    print("🛡️ Control de duplicados: empresas_scrapeadas_linkedin_contacts")
    print("💾 Datos de contactos: linkedin_contacts_info")
    print("🤖 Usando OpenAI para evaluación")
    print("🕷️ Usando Apify para scraping detallado")
    print(f"📊 Empresas a procesar (SIN duplicados): {len(companies)}")
    print(f"💰 Costo estimado total: ~${len(companies) * 0.5:.2f}")
    print("="*80)

    try:
        # Ejecutar scraping selectivo
        results = scraper.run_selective_test(
            companies=companies,
            max_per_company=15,  # Máximo 15 perfiles por empresa
            min_score=7          # Solo scrapear perfiles con score >= 7
        )

        if not results:
            logger.error("❌ No se obtuvieron resultados")
            return jsonify({"error": "No se obtuvieron resultados"}), 400

    except Exception as e:
        #logger.error("📝 Marcando empresas procesadas como scrapeadas...")
        #bigquery_service.marcar_empresas_contacts_como_scrapeadas(scraper)
        return jsonify({"error": "Proceso interrumpido por el usuario"}), 400

    # Solo mostrar estadísticas finales si el proceso se completó
    print("\n📝 MARCANDO EMPRESAS COMO SCRAPEADAS...")
    #bigquery_service.marcar_empresas_contacts_como_scrapeadas(scraper)

    # Guardar contactos en BigQuery
    print("\n💾 GUARDANDO CONTACTOS EN BIGQUERY...")
    #filename = bigquery_service.save_contacts_to_bigquery(scraper)

    # Descargar archivo CSV automáticamente en Colab
    """
    if filename:
        from google.colab import files
        files.download(filename)
    """
    # Estadísticas finales
    logger.info(f"\n{'='*80}")
    logger.info("📊 ESTADÍSTICAS FINALES LINKEDIN CONTACTS SCRAPER")
    logger.info(f"{'='*80}")
    logger.info(f"Empresas procesadas: {len(scraper.test_metrics['companies_processed'])}")
    logger.info(f"Total perfiles encontrados: {scraper.test_metrics['total_profiles_found']} \
        Perfiles evaluados: {scraper.test_metrics['profiles_evaluated']} \
        Perfiles seleccionados: {scraper.test_metrics['high_score_profiles']} \
        Perfiles scrapeados: {scraper.test_metrics['profiles_scraped']} \
        CONTACTOS FINALES OBTENIDOS: {len(scraper.contacts_results)} \
        Costo total estimado: ${scraper.test_metrics['cost_estimate']:.2f}")

    if scraper.contacts_results:
        logger.info(f"\n🏆 MUESTRA DE CONTACTOS OBTENIDOS:")
        for i, contact in enumerate(scraper.contacts_results[:5], 1):
            logger.info(f"  {i}. {contact['contact_name']} - {contact['contact_position']}")
            logger.info(f" Empresa: {contact['biz_name']} , ID: {contact['biz_identifier']}, Score IA: {contact['ai_score']}, \
            LinkedIn:{contact['linkedin_profile_url']}")

        # Análisis por empresa
        empresas_con_contactos = {}
        for contact in scraper.contacts_results:
            empresa = contact['biz_name']
            if empresa not in empresas_con_contactos:
                empresas_con_contactos[empresa] = 0
            empresas_con_contactos[empresa] += 1

        logger.info(f"\n🏢 CONTACTOS POR EMPRESA:")
        for empresa, count in sorted(empresas_con_contactos.items(), key=lambda x: x[1], reverse=True)[:10]:
            logger.info(f"  {empresa}: {count} contactos")

        # Análisis por posición
        posiciones = {}
        for contact in scraper.contacts_results:
            posicion = contact['contact_position'].lower()
            # Simplificar posiciones similares
            if any(word in posicion for word in ['cfo', 'chief financial', 'director financiero']):
                posicion_key = 'CFO/Director Financiero'
            elif any(word in posicion for word in ['ceo', 'chief executive', 'director general']):
                posicion_key = 'CEO/Director General'
            elif any(word in posicion for word in ['controller', 'contralor']):
                posicion_key = 'Controller'
            elif any(word in posicion for word in ['manager', 'gerente']):
                posicion_key = 'Manager/Gerente'
            else:
                posicion_key = 'Otros'

            if posicion_key not in posiciones:
                posiciones[posicion_key] = 0
            posiciones[posicion_key] += 1

        logger.info(f"\n🎯 CONTACTOS POR TIPO DE POSICIÓN:")
        for posicion, count in sorted(posiciones.items(), key=lambda x: x[1], reverse=True):
            logger.info(f"  {posicion}: {count} contactos")

    return jsonify({"message": "Proceso completado exitosamente"}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)