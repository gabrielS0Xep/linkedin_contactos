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
            project = Config.GOOGLE_CLOUD_PROJECT_ID,
            dataset = Config.BIGQUERY_DATASET,
            table_control_name = Config.CONTROL_TABLE_NAME,
            table_info_name = Config.LINKEDIN_INFO_TABLE_NAME
        )
        logger.info("✅ Servicios inicializados correctamente")
    except Exception as e:
        logger.error(f"❌ Error inicializando servicios: {e}")
        raise

    return bigquery_service , secret_manager


@app.route("/status", methods=['GET'])
def health_check():
    return {"status": "OK"}



@app.route("/scrape", methods=['POST'])
def scrape():
    """
    Endpoint para hacer scraping de contactos de LinkedIn
    
    Body JSON (opcional):
    {
        "batch_size": 10,
        "max_per_company": 4
    }
    
    Retorna:
    {
    "contactos": [
        {
            "ai_score_value": 10,
            "biz_founded_year": "2018",
            "biz_identifier": "CDE1706071C3",
            "biz_industry": "Management Consulting",
            "biz_name": "CONSULTORÍA EN DESEMPEÑO ENERGÉTICO S.C",
            "biz_size": "11-50",
            "biz_web_linkedin_url": "linkedin.com/company/csrconsultingmx",
            "biz_web_url": "csrconsulting.mx",
            "cntry_city_value": "Monterrey, Nuevo León, Mexico",
            "cntry_value": "Mexico",
            "current_job_duration": "7 yrs 4 mos",
            "email": "None",
            "first_name": "PhD Carmelo",
            "full_name": "PhD Carmelo Santillán Ramos",
            "headline": "None",
            "last_name": "Santillán Ramos",
            "phone_number": "None",
            "role": "CEO",
            "src_scraped_dt": "Mon, 08 Sep 2025 14:31:50 GMT",
            "web_linkedin_url": "https://mx.linkedin.com/in/carmelosantillan/en"
        }
    ],
    "contactos finales obtenidos": 0,
    "costo total estimado": 0.01,
    "empresas procesadas": 2,
    "message": "Proceso completado exitosamente",
    "perfiles evaluados": 1,
    "perfiles scrapeados": 1,
    "perfiles seleccionados": 1,
    "total perfiles encontrados": 1
}
    
    
    """
    # 🔑 API KEYS CONFIGURADAS
    SERPER_API_KEY  = Config.SERPER_API_KEY
    # 🆕 API KEY DE APIFY
    APIFY_TOKEN = Config.APIFY_TOKEN
    
    bigquery_service = get_services()
    # PASO 0: Crear tablas si no existen
    if not bigquery_service.table_exists(Config.CONTROL_TABLE_NAME):
        bigquery_service.crear_tabla_empresas_scrapeadas_linkedin_contacts()
    if not bigquery_service.table_exists(Config.LINKEDIN_INFO_TABLE_NAME):
        bigquery_service.crear_tabla_linkedin_contacts_info()


    data = request.get_json()
    
    batch_size = int(str(data.get('batch_size', 1)))
    max_per_company = int(str(data.get('max_per_company', 4)))


    # Cargar empresas no scrapeadas
    companies_data = bigquery_service.load_companies_from_bigquery_linkedin_contacts(batch_size)

    if not companies_data:
        logger.error("❌ No se pudieron cargar empresas desde BigQuery o todas ya fueron scrapeadas. ")
        return jsonify(
            {"error": "No se pudieron cargar empresas desde BigQuery o todas ya fueron scrapeadas. "}
            ), 400

    # Extraer nombres y crear mapeo de biz_identifier
    companies = [company['biz_name'] for company in companies_data]
    company_biz_mapping = {company['biz_name']: company['biz_identifier'] for company in companies_data}

    scraper = LinkedInContactsSelectiveScraper(SERPER_API_KEY, APIFY_TOKEN, company_biz_mapping)

    try:
        # Ejecutar scraping selectivo
        results = scraper.run_selective_test(
            companies=companies,
            max_per_company=max_per_company,  # Máximo 15 perfiles por empresa
        )

        if not results:
            logger.info("❌ No se obtuvieron resultados de acuerdo a los criterios de busqueda")
            """
            return jsonify({
                "status": "success",
                "message": "No se obtuvieron resultados de acuerdo a los criterios de busqueda"}), 200
            """
    except Exception as error:

        logger.error("❌ Error en scraping: {error}")
        return jsonify({f"error": f"{error}"}), 400

    # Solo mostrar estadísticas finales si el proceso se completó
    logger.info("📝 MARCANDO EMPRESAS COMO SCRAPEADAS...")
    logger.info(f"Contacts results: {results}")
    
    contacts_data = scraper.format_contacts_for_bigquery(results)

    logger.info(f"Contacts data: {contacts_data}")
    logger.info(f"Companies data: {companies_data}")
    
    logger.info("Marcando empresas como scrapeadas")
    bigquery_service.marcar_empresas_contacts_como_scrapeadas(contacts_data, companies_data)

    # Guardar contactos en BigQuery
    logger.info("\n💾 GUARDANDO CONTACTOS EN BIGQUERY...")

    logger.info(f"Contactos: {contacts_data}")
    bigquery_service.save_contacts_to_bigquery(contacts_data)

    return jsonify(
        {"message": "Proceso completado exitosamente",
        "empresas procesadas": len(companies),
        "perfiles evaluados": len(results),
        "perfiles seleccionados": len(results),
        "perfiles scrapeados": len(results),
        "contactos finales obtenidos": len(contacts_data),
        "costo total estimado": scraper.test_metrics['cost_estimate'],
        "contactos": contacts_data
    }), 200



@app.route('/validate', methods=['POST'])
def validate_request():
    """
    Endpoint para validar parámetros del request y verificar empresas pendientes
    
    Body JSON (opcional):
    {
        "companies": [
            {
                "rfc": "ABC123456789",
                "company_name": "Empresa 1"
            }
        ]
    }
    
    Si no vienen parámetros, verifica todas las empresas pendientes en la tabla de control.
    Si vienen parámetros, verifica si esas empresas específicas están pendientes.
    
    Retorna:
    {
        "success": true/false,
        "validation_type": "no_params" | "with_params",
        "pending_companies": [
            {
                "rfc": "ABC123456789",
                "company_name": "Empresa 1"
            }
        ],
        "total_pending": 10,
        "message": "Descripción del resultado",
        "timestamp": "2024-01-01T00:00:00"
    }
    """
    try:
        # Obtener servicios
        bigquery_service, _ = get_services()
        
        # Verificar si hay datos en el request
        if not request.is_json:
            # No hay parámetros JSON, verificar todas las empresas pendientes
            logger.info("🔍 Validando empresas pendientes sin parámetros específicos")
            
            pending_companies = bigquery_service.get_pending_companies(Config.CONTROL_TABLE_NAME)
            total_pending = bigquery_service.get_pending_companies_count(Config.CONTROL_TABLE_NAME)
            
            return jsonify({
                "success": True,
                "validation_type": "no_params",
                "pending_companies": pending_companies,
                "total_pending": total_pending,
                "message": f"Se encontraron {total_pending} empresas pendientes de scraping en total",
                "timestamp": datetime.now().isoformat()
            })
        
        data = request.get_json()
        
        # Si hay datos pero no tienen la estructura esperada
        if not data or 'companies' not in data:
            return jsonify({
                "success": False,
                "error": "Si se proporcionan parámetros, debe incluirse el campo 'companies' con un array de empresas",
                "timestamp": datetime.now().isoformat()
            }), 400
        
        companies_data = data['companies']
        
        # Validar que companies sea una lista
        if not isinstance(companies_data, list):
            return jsonify({
                "success": False,
                "error": "El campo 'companies' debe ser un array",
                "timestamp": datetime.now().isoformat()
            }), 400
        
        # Validar estructura de cada empresa
        for i, company in enumerate(companies_data):
            if not isinstance(company, dict) or 'rfc' not in company or 'company_name' not in company:
                return jsonify({
                    "success": False,
                    "error": f"Empresa en posición {i} debe tener campos 'rfc' y 'company_name'",
                    "timestamp": datetime.now().isoformat()
                }), 400
        
        logger.info(f"🔍 Validando {len(companies_data)} empresas específicas")
        
        # Verificar cuáles de las empresas proporcionadas están pendientes
        pending_companies = []
        for company in companies_data:
            rfc = company['rfc'].strip()
            company_name = company['company_name'].strip()
            
            # Verificar si la empresa está pendiente
            verification = bigquery_service.verificar_empresa_scrapeada(
                biz_identifier=rfc,
                company_name=company_name,
                table_name=Config.CONTROL_TABLE_NAME
            )
            
            if verification['needs_scraping']:
                pending_companies.append({
                    'rfc': rfc,
                    'company_name': company_name
                })
        
        return jsonify({
            "success": True,
            "validation_type": "with_params",
            "requested_companies": len(companies_data),
            "pending_companies": pending_companies,
            "total_pending": len(pending_companies),
            "message": f"De {len(companies_data)} empresas solicitadas, {len(pending_companies)} están pendientes de scraping",
            "timestamp": datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"❌ Error en validación: {e}")
        
        return jsonify({
            "success": False,
            "error": f"Error interno del servidor: {str(e)}",
            "timestamp": datetime.now().isoformat()
        }), 500



if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)



    