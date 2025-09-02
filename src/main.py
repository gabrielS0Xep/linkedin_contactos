from flask import Flask, request, jsonify
from flask_cors import CORS
from config import Config
import logging
from bigquery_services import BigQueryService

import time 
from datetime import datetime, date
from functools import wraps




logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Crear aplicación Flask
app = Flask(__name__)
CORS(app)  # Habilitar CORS para requests cross-origin

bigquery_service = None
pub_sub_services = None
cloud_tasks_service = None


def get_services():
    try: 
        # Inicializar BigQuery service
        bigquery_service = BigQueryService(
            project=Config.GOOGLE_CLOUD_PROJECT_ID,
            dataset=Config.BIGQUERY_DATASET
        )
        
        logger.info("✅ Servicios inicializados correctamente")
    except Exception as e:
        logger.error(f"❌ Error inicializando servicios: {e}")
        raise

    return bigquery_service


from google import genai
from google.genai import types


import requests
import json
import time
from typing import List, Dict, Tuple
from apify_client import ApifyClient
import openai
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

# ================================
# FUNCIONES PARA CONTROL DE SCRAPPING - ADAPTADAS PARA CONTACTS
# ================================




def main():
    # 🔑 API KEYS CONFIGURADAS
    secretManager = SecretManager(project=Config.GOOGLE_CLOUD_PROJECT_ID)
    SERPER_API_KEY  = secretManager.get_secret('api_key_serper_linkedin_contactos')
    # 🆕 API KEY DE APIFY
    APIFY_TOKEN = secretManager.get_secret('apify_token')


    print("🔧 PREPARANDO TABLAS DE LINKEDIN CONTACTS EN XEPELIN...")
    print("="*80)
    bigquery_service = BigQueryService(
            project=Config.GOOGLE_CLOUD_PROJECT_ID,
            dataset=Config.BIGQUERY_DATASET
        )
    # PASO 0: Crear tablas si no existen
    bigquery_service.crear_tabla_empresas_scrapeadas_linkedin_contacts()
    bigquery_service.crear_tabla_linkedin_contacts_info()

    # Cargar empresas desde BigQuery (SIN las ya scrapeadas)
    companies_data = bigquery_service.load_companies_from_bigquery_linkedin_contacts()

    if not companies_data:
        print("❌ No se pudieron cargar empresas desde BigQuery o todas ya fueron scrapeadas. Saliendo...")
        return

    # Extraer nombres y crear mapeo de biz_identifier
    companies = [company['name'] for company in companies_data]
    company_biz_mapping = {company['name']: company['biz_identifier'] for company in companies_data}

    scraper = LinkedInContactsSelectiveScraper(SERPER_API_KEY, APIFY_TOKEN)

    # Configurar mapeo de biz_identifier
    scraper.company_biz_mapping = company_biz_mapping

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
            print("❌ No se obtuvieron resultados")
            return

    except KeyboardInterrupt:
        print("\n\n⚠️ PROCESO INTERRUMPIDO CON Ctrl+C")
        # Marcar empresas como scrapeadas incluso si se interrumpe
        print("📝 Marcando empresas procesadas como scrapeadas...")
        bigquery_service.marcar_empresas_contacts_como_scrapeadas(scraper)
        return

    # Solo mostrar estadísticas finales si el proceso se completó
    print("\n📝 MARCANDO EMPRESAS COMO SCRAPEADAS...")
    bigquery_service.marcar_empresas_contacts_como_scrapeadas(scraper)

    # Guardar contactos en BigQuery
    print("\n💾 GUARDANDO CONTACTOS EN BIGQUERY...")
    filename = bigquery_service.save_contacts_to_bigquery(scraper)

    # Descargar archivo CSV automáticamente en Colab
    """
    if filename:
        from google.colab import files
        files.download(filename)
    """
    # Estadísticas finales
    print(f"\n{'='*80}")
    print("📊 ESTADÍSTICAS FINALES LINKEDIN CONTACTS SCRAPER")
    print(f"{'='*80}")
    print(f"Empresas procesadas: {len(scraper.test_metrics['companies_processed'])}")
    print(f"Total perfiles encontrados: {scraper.test_metrics['total_profiles_found']}")
    print(f"Perfiles evaluados: {scraper.test_metrics['profiles_evaluated']}")
    print(f"Perfiles seleccionados: {scraper.test_metrics['high_score_profiles']}")
    print(f"Perfiles scrapeados: {scraper.test_metrics['profiles_scraped']}")
    print(f"✅ CONTACTOS FINALES OBTENIDOS: {len(scraper.contacts_results)}")
    print(f"💰 Costo total estimado: ${scraper.test_metrics['cost_estimate']:.2f}")

    if scraper.contacts_results:
        print(f"\n🏆 MUESTRA DE CONTACTOS OBTENIDOS:")
        for i, contact in enumerate(scraper.contacts_results[:5], 1):
            print(f"  {i}. {contact['contact_name']} - {contact['contact_position']}")
            print(f"      Empresa: {contact['biz_name']} (ID: {contact['biz_identifier']})")
            print(f"      Score IA: {contact['ai_score']}")
            print(f"      LinkedIn: {contact['linkedin_profile_url']}")
            print()

        # Análisis por empresa
        empresas_con_contactos = {}
        for contact in scraper.contacts_results:
            empresa = contact['biz_name']
            if empresa not in empresas_con_contactos:
                empresas_con_contactos[empresa] = 0
            empresas_con_contactos[empresa] += 1

        print(f"\n🏢 CONTACTOS POR EMPRESA:")
        for empresa, count in sorted(empresas_con_contactos.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"  {empresa}: {count} contactos")

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

        print(f"\n🎯 CONTACTOS POR TIPO DE POSICIÓN:")
        for posicion, count in sorted(posiciones.items(), key=lambda x: x[1], reverse=True):
            print(f"  {posicion}: {count} contactos")

    print(f"\n✅ PROCESO COMPLETADO EXITOSAMENTE")
    print(f"📊 Revisa las tablas en BigQuery:")
    print(f"   📋 Control: xepelin-lab-customer-mx.raw_in_scrapper_contacts.empresas_scrapeadas_linkedin_contacts")
    print(f"   💾 Contactos: xepelin-lab-customer-mx.raw_in_scrapper_contacts.linkedin_contacts_info")

# ================================
# CELDA 4: EJECUTAR EL PROGRAMA
# ================================

if __name__ == "__main__":
    main()