from annotated_types import LowerCase
import requests
import json
import time
from typing import List, Dict, Tuple
from apify_client import ApifyClient
from datetime import datetime
from config import Config
from urllib.parse import urlparse

import logging
logger = logging.getLogger(__name__)

class LinkedInContactsSelectiveScraper:
    def __init__(self, serper_api_key: str, apify_token: str):
        self.serper_api_key = serper_api_key
    
        self.apify_client = ApifyClient(apify_token)

        # Configuración de proyecto y dataset específicos
        self.project_id = Config.GOOGLE_CLOUD_PROJECT_ID
        self.dataset_id = Config.BIGQUERY_DATASET
        self.location = Config.LOCATION
        

        # Métricas de prueba
        self.test_metrics = {
            'start_time': None,
            'end_time': None,
            'total_profiles_found': 0,
            'profiles_evaluated': 0,
            'high_score_profiles': 0,
            'profiles_scraped': 0,
            'profiles_with_emails': 0,
            'cost_estimate': 0,
            'companies_processed': []
        }

        # Resultados finales para guardar en BigQuery
        self.contacts_results = []

    def scrape_selected_profiles(self, selected_profiles: List[Dict]) -> Dict:
        """
        Scrapea solo los perfiles seleccionados con dev_fusion
        """

        if not selected_profiles:
            logger.error("❌ No hay perfiles seleccionados para scrapear")
            raise Exception("No hay perfiles seleccionados para scrapear")

        logger.info(f"\n🚀 Scrapeando {len(selected_profiles)} perfiles seleccionados...")

        # Extraer URLs
        profile_urls = [profile['web_linkedin_url'] for profile in selected_profiles]

        # Calcular costo estimado
        estimated_cost = (len(profile_urls) / 1000) * 10
        self.test_metrics['cost_estimate'] = estimated_cost
        logger.info(f"💰 Costo estimado: ${estimated_cost:.2f}")

        try:
            run_input = {
                "profileUrls": profile_urls
            }

            logger.info("⏳ Ejecutando dev_fusion/linkedin-profile-scraper...")
            start_time = time.time()

            run = self.apify_client.actor("dev_fusion/linkedin-profile-scraper").call(run_input=run_input)

            scraping_time = time.time() - start_time
            
            logger.info(f"⏱️ Tiempo de scraping: {scraping_time:.1f} segundos")

            # Obtener resultados
            scraped_profiles = []
            for item in self.apify_client.dataset(run["defaultDatasetId"]).iterate_items():
                logger.info(f"🔍 Scraping: {item}")
                scraped_profiles.append(item)

            logger.info(f"✅ Scraping completado:")
            logger.info(f"  Perfiles scrapeados: {len(scraped_profiles)}")

            return {
                'success': True,
                'scraped_profiles': scraped_profiles,
                'scraping_time': scraping_time,
                'run_info': run
            }

        except Exception as e:
            logger.error(f"❌ Error en scraping: {e}")
            return {'success': False, 'error': e , 'scraped_profiles': []}

    def clean_scraped_data(self, scraped_data: List[Dict]) -> Dict:
        """
        Limpia los datos scrapeados
        """
        clean_data_list = []
        clean_data = {}
        for scraped in scraped_data:
            clean_data['linkedinUrl'] = str(scraped.get('linkedinUrl', '')).strip()
            clean_data['fullName'] = str(scraped.get('fullName', '')).strip()
            clean_data['firstName'] = str(scraped.get('firstName', '')).strip()
            clean_data['lastName'] = str(scraped.get('lastName', '')).strip()
            clean_data['email'] = str(scraped.get('email', '')).strip()
            clean_data['mobileNumber'] = str(scraped.get('mobileNumber', '')).strip()
            clean_data['headline'] = str(scraped.get('headline', '')).strip()
            clean_data['jobTitle'] = str(scraped.get('jobTitle', '')).strip()
            clean_data['companyName'] = str(scraped.get('companyName', '')).strip()
            clean_data['companyIndustry'] = str(scraped.get('companyIndustry', '')).strip()
            clean_data['companyWebsite'] = str(scraped.get('companyWebsite', '')).strip()
            clean_data['companyLinkedin'] = str(scraped.get('companyLinkedin', '')).strip()
            clean_data['companyFoundedIn'] = str(scraped.get('companyFoundedIn', '')).strip()
            clean_data['companySize'] = str(scraped.get('companySize', '')).strip()
            clean_data['currentJobDuration'] = str(scraped.get('currentJobDuration', '')).strip()
            clean_data['currentJobDurationInYrs'] = str(scraped.get('currentJobDurationInYrs', '')).strip()
            clean_data['topSkillsByEndorsements'] = str(scraped.get('topSkillsByEndorsements', '')).strip()
            clean_data['addressCountryOnly'] = str(scraped.get('addressCountryOnly', '')).strip()
            clean_data['addressWithCountry'] = str(scraped.get('addressWithCountry', '')).strip()
            clean_data_list.append(clean_data)
            clean_data = {}

        return clean_data_list


    def standardize_url(self, url: str) -> str:
        """
        Standardizes a URL to ensure consistent keys for a dictionary.
        
        """
        if not url:
            return ''
        # Normalize URL by parsing and re-forming it
        parsed_url = urlparse(url)
        return parsed_url.path

    def merge_evaluation_and_scraping(self, selected_profiles: List[Dict], scraped_data: List[Dict]) -> List[Dict]:
        """
        Combina los datos de evaluación con los datos scrapeados
        """
        logger.info("\n🔗 Combinando datos de evaluación con scraping...")


        # Crear un diccionario para mapeo rápido por URL
        scraped_by_url_map = {
            self.standardize_url(scraped.get('linkedinUrl')): scraped
        for scraped in scraped_data
        }

        merged_profiles = []

        try:
            for evaluation in selected_profiles:
                try:
                    
                    original_url = evaluation['url']

                    normalized_url = self.standardize_url(original_url)

                    # Buscar datos scrapeados correspondientes
                    scraped_data_match = scraped_by_url_map.get(normalized_url)

                    logger.info(f"🔍 Scraped data match: {scraped_data_match}")
                    
                    # Preferir campos de evaluación (incluida 'explicacion') y fusionar datos scrapeados si existen
                    scraped_fields = scraped_data_match or {}
                    merged_profile = {
                        **scraped_fields,   # datos del scraper (nombre, empresa, etc.)
                        **evaluation,       # mantiene 'explicacion' y demás campos de IA
                    }

                    merged_profiles.append(merged_profile)

                except Exception as e:
                    logger.error(f"❌ Error formateando los datos del perfil: {original_url}  msg:{e}")
                    continue
            
            logger.info(f"✅ Perfiles combinados: {merged_profile}")
            print(f"✅ Combinados {len(merged_profiles)} perfiles")
            return merged_profiles

        except Exception as e:
            logger.error(f"❌ Hubo un error fatal en merge_evaluation_and_scraping: {e}")
            return []

    def format_contacts_for_bigquery(self, merged_profiles: List[Dict]):
        """
        Procesa los perfiles para crear registros individuales de contactos
        """
        print("\n📊 Procesando contactos para BigQuery...")

        contacts_data = []

        for profile in merged_profiles:
            if not profile['scraping_success']:
                continue
            # Crear registro de contacto
            contact_record = {
                'biz_identifier': profile['biz_identifier'],
                'biz_name': profile['biz_name'],
                'biz_industry': profile['companyIndustry'],
                'biz_web_url': profile['companyWebsite'],
                'biz_web_linkedin_url': profile['companyLinkedin'],
                'biz_founded_year': profile['companyFoundedIn'],
                'biz_size': profile['companySize'],
                'full_name': profile['fullName'],
                'role': profile['jobTitle'],
                'web_linkedin_url': profile['linkedinUrl'],
                'first_name': profile['firstName'],
                'last_name': profile['lastName'],
                'email': profile['email'],
                'phone_number': profile['mobileNumber'],
                'headline': profile['headline'],
                'current_job_duration': profile['currentJobDuration'],
                'cntry_value': profile['addressCountryOnly'],
                'cntry_city_value': profile['addressWithCountry'],
                'src_scraped_dt': datetime.now(),
                'ai_score_cat': profile['ai_score_cat'],
                'ai_explanation': profile['ia_explanation'],
                'ai_current_biz_flg': profile['ai_current_biz_flg'],
                'ai_role_finance_flg': profile['ai_role_finance_flg']
            }

            contacts_data.append(contact_record)
            print(f"  ✅ Contacto procesado: {contact_record['full_name']} - {contact_record['role']}")

    #tendria que pushear
            
        print(f"\n📈 Total contactos procesados: {len(contacts_data)}")
        return contacts_data


    def scrape_linkedin_profiles(self, profiles: List[Dict] ):
        """
        Ejecuta el test selectivo completo
        """

        selected_profiles = profiles

        logger.info(f"🔍 Selected profiles: {selected_profiles}")

        scraping_results = self.scrape_selected_profiles(selected_profiles)

        if not scraping_results['success']:
            logger.error(f"❌ Error en scraping: {scraping_results['error']}")
            return []
        
        logger.info(f"🔍 Scraping results: {scraping_results['scraped_profiles']}")

        # . Limpia los datos scrapeados
        cleaned_scraped_data = self.clean_scraped_data(scraping_results['scraped_profiles'])

        logger.info(f"🔍 Cleaned scraped data: {cleaned_scraped_data}")

        # . Combinar datos de evaluación con scraping

        merged_profiles = self.merge_evaluation_and_scraping(
            selected_profiles,
            cleaned_scraped_data
        )
        
 
        return merged_profiles

    def check_exists_in_bigquery(self, companies: List[Dict]):
        """
        Verifica si las empresas ya existen en BigQuery
        """
        for company in companies:
            if self.bigquery_service.check_exists_in_bigquery(company['biz_identifier']):
                return True


        return False
