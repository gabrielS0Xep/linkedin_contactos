"""
Configuración del LinkedIn Scraper API
Maneja variables de entorno y configuraciones del proyecto
"""

import os
from dotenv import load_dotenv
import json
from secret_manager_service import SecretManager
# Cargar variables de entorno desde .env
load_dotenv()

class Config:
    GOOGLE_CLOUD_PROJECT_ID = os.getenv('GOOGLE_CLOUD_PROJECT_ID','qa-cdp-mx')
    LOCATION = os.getenv('LOCATION', 'us-central1')
    BIGQUERY_LOCATION = os.getenv('BIGQUERY_LOCATION', 'US')
    # Configuración BigQuery
    BIGQUERY_DATASET = os.getenv('BIGQUERY_DATASET', 'raw_in_scrapper')
    CONTROL_TABLE_NAME = os.getenv('CONTROL_TABLE_NAME', 'linkedin_scraped_contacts')
    LINKEDIN_INFO_TABLE_NAME = os.getenv('LINKEDIN_INFO_TABLE_NAME', 'linkedin_contacts_info')

    # Google Search Service URL
    GOOGLE_SEARCH_SERVICE_URL = os.getenv('GOOGLE_SEARCH_SERVICE_URL', 'https://google-search-contacts-601063044530.us-central1.run.app/search')
    
    # Configuración Google Cloud Storage
    #GCS_BUCKET = os.getenv('GCS_BUCKET', 'scrapper_contacts_data')  # Bucket para guardar CSVs
    #GCS_FOLDER = os.getenv('GCS_FOLDER', 'linkedin_data')  # Carpeta dentro del bucket

    """Clase de configuración para el LinkedIn Scraper API"""
    secret_manager_services = SecretManager(project=GOOGLE_CLOUD_PROJECT_ID)
    # API Keys
    SERPER_API_KEY  = secret_manager_services.get_secret('api_key_serper_linkedin_contactos')
    # 🆕 API KEY DE APIFY
    APIFY_TOKEN = secret_manager_services.get_secret('apify_token')
    
    # Service Account Configuration - múltiples opciones
    # GOOGLE_APPLICATION_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')  
    
    # Configuración Flask
    FLASK_HOST = os.getenv('FLASK_HOST', '0.0.0.0')
    PORT = int(os.getenv('PORT', '5000'))
    FLASK_DEBUG = os.getenv('FLASK_DEBUG', 'False').lower() == 'true'
    
    # Configuración de threading
    MAX_WORKERS = int(os.getenv('MAX_WORKERS', '5'))  # Reducido para API
    
    # Configuración de modelos Gemini
    GEMINI_MODEL_NAME = os.getenv('GEMINI_MODEL_NAME', 'gemini-2.5-flash-lite')
    
    # Configuración de generación Gemini
    GEMINI_CONFIG = {
        "temperature": 0.3,
        "top_p": 0.95,
        "top_k": 20,
        "max_output_tokens": 2000,
        "candidate_count": 1
    }
    
    # Límites de requests por modelo
    MAX_REQUESTS_PER_MODEL = 9500
    
    # Timeout para requests
    REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', '300'))  # 5 minutos
    
    # Configuración de reintentos y timeouts
    MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))  # Número máximo de reintentos
    RETRY_DELAY = int(os.getenv('RETRY_DELAY', '5'))  # Segundos entre reintentos
    BATCH_TIMEOUT = int(os.getenv('BATCH_TIMEOUT', '600'))  # 10 minutos para batch completo
    INDIVIDUAL_TIMEOUT = int(os.getenv('INDIVIDUAL_TIMEOUT', '120'))  # 2 minutos por empresa

    @classmethod
    def validate(cls):
        """Valida que todas las variables de entorno requeridas estén configuradas"""
        # Variables siempre requeridas
        required_vars = [
            ('SERPER_API_KEY', cls.SERPER_API_KEY),
            ('ENRICHLAYER_API_KEY', cls.ENRICHLAYER_API_KEY)
        ]
        
        missing_vars = []
        for var_name, var_value in required_vars:
            if not var_value:
                missing_vars.append(var_name)
        
        if missing_vars:
            raise ValueError(f"Faltan las siguientes variables de entorno: {', '.join(missing_vars)}")
        
        return True 