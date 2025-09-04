import requests
import json
import time
from typing import List, Dict, Tuple
from apify_client import ApifyClient
from datetime import datetime
from config import Config
from urllib.parse import urlparse

from genia_service import GenIaService

import logging
logger = logging.getLogger(__name__)

class LinkedInContactsSelectiveScraper:
    def __init__(self, serper_api_key: str, apify_token: str , company_biz_mapping: Dict):
        self.serper_api_key = serper_api_key
    
        self.apify_client = ApifyClient(apify_token)
        self.company_biz_mapping = company_biz_mapping  # Mapeo de nombres a biz_identifier

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

    def set_company_biz_mapping(self, company_biz_mapping: Dict):
        self.company_biz_mapping = company_biz_mapping

    def search_company_profiles(self, company_name: str, max_profiles: int = 20) -> List[Dict]:
        """
        Busca máximo 20 perfiles de LinkedIn por empresa usando Google Dorks
        """
        print(f"\n🔍 Buscando perfiles para: {company_name} (máx. {max_profiles})")

        linkedin_profiles = []

        # Queries más específicas para México (adaptado del original)
        search_queries = [
            f'site:linkedin.com/in/ "{company_name}" México (CFO OR CEO OR Controller OR "Finance Director")',
            f'site:linkedin.com/in/ "{company_name}" México (finanzas OR contabilidad OR tesorería)',
            f'site:linkedin.com/in/ "{company_name}" México ("Financial Manager" OR "Accounting Manager")',
            f'site:linkedin.com/in/ "{company_name}" "Ciudad de México" (Finance OR Treasury OR Credit)',
            f'site:mx.linkedin.com/in/ "{company_name}"',

        ]
        for query in search_queries:
            if len(linkedin_profiles) >= max_profiles:
                break

            try:
                url = "https://google.serper.dev/search"
                payload = json.dumps({
                    "q": query,
                    "num": 8,  # Menos resultados por query
                    "gl": "mx",
                    "hl": "es"
                })
                headers = {
                    'X-API-KEY': self.serper_api_key,
                    'Content-Type': 'application/json'
                }

                response = requests.post(url, headers=headers, data=payload)

                if response.status_code == 200:
                    results = response.json()

                    if 'organic' in results:
                        for result in results['organic']:
                            link = result.get('link', '')
                            title = result.get('title', '')
                            snippet = result.get('snippet', '')

                            if 'linkedin.com/in/' in link and len(linkedin_profiles) < max_profiles:
                                # Verificar duplicados
                                logger.info(f"🔍 Perfil encontrado: {link}")
                                existing_urls = [p['url'] for p in linkedin_profiles]
                                if link not in existing_urls:
                                    linkedin_profiles.append({
                                        'url': link,
                                        'title': title,
                                        'snippet': snippet,
                                        'biz_name': company_name,
                                        'biz_identifier': self.company_biz_mapping.get(company_name, ''),
                                        'query_used': query
                                    })
                                    logger.info(f"  ✓ Perfil {len(linkedin_profiles)}: {title[:50]}...")

                time.sleep(1)  # Rate limiting

            except Exception as e:
                logger.error(f"  ❌ Error en búsqueda: {str(e)}")
                continue

        return linkedin_profiles

###Esta funcion es la que encuentra con serper api los links de los perfiles de linkedin

    

    def select_best_profiles(self, all_profiles: List[Dict], min_score: int = 7) -> Tuple[List[Dict], List[Dict]]:
        """
        Evalúa todos los perfiles y selecciona solo los mejores
        """
        evaluated_profiles = []
        high_score_profiles = []
        genia_service = GenIaService(self.project_id, self.location)
        logger.info(f"Se inicializo el genia service")
        for profile_data in all_profiles:
            try:
                logger.info(f"🔍 Evaluando perfil: {profile_data['url']}")
                result = genia_service.evaluate_profile_relevance_detailed(
                    profile_data
                )

                structured_info = genia_service.extract_structured_info(result)
        
                evaluation = {
                    **profile_data,
                    **structured_info,
                    'evaluation_timestamp': datetime.now().isoformat()
                }
                logger.info(f"🔍 Evaluacion: {evaluation}")
                evaluated_profiles.append(evaluation)

                if structured_info['score'] >= min_score:
                    high_score_profiles.append(evaluation)
                    logger.info(f"    ✅ SELECCIONADO - Score: {structured_info['score']} - {structured_info['rol_finanzas'][:50]}...")
                else:
                    logger.info(f"    ❌ Descartado - Score: {structured_info['score']} - {structured_info['rol_finanzas'][:50]}...")

                time.sleep(0.3)  # Rate limiting para OpenAI

            except Exception as e:
                logger.info(f"    ❌ Error evaluando perfil: {str(e)}")
                continue

        self.test_metrics['profiles_evaluated'] = len(evaluated_profiles)
        self.test_metrics['high_score_profiles'] = len(high_score_profiles)

        logger.info(f"\n📊 RESULTADOS DE EVALUACIÓN:")
        logger.info(f"  Perfiles evaluados: {len(evaluated_profiles)}")
        logger.info(f"  Perfiles seleccionados: {len(high_score_profiles)}")

        return high_score_profiles, evaluated_profiles

    def scrape_selected_profiles(self, selected_profiles: List[Dict]) -> Dict:
        """
        Scrapea solo los perfiles seleccionados con dev_fusion
        """
        if not selected_profiles:
            logger.error("❌ No hay perfiles seleccionados para scrapear")
            raise Exception("No hay perfiles seleccionados para scrapear")

        logger.info(f"\n🚀 Scrapeando {len(selected_profiles)} perfiles seleccionados...")

        # Extraer URLs
        profile_urls = [profile['url'] for profile in selected_profiles]

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

            self.test_metrics['profiles_scraped'] = len(scraped_profiles)

            # Analizar resultados
            profiles_with_emails = 0
            for profile in scraped_profiles:
                if profile.get('email') or profile.get('emails'):
                    profiles_with_emails += 1

            self.test_metrics['profiles_with_emails'] = profiles_with_emails

            logger.info(f"✅ Scraping completado:")
            logger.info(f"  Perfiles scrapeados: {len(scraped_profiles)}")
            logger.info(f"  Perfiles con email: {profiles_with_emails}")
            logger.info(f"  Tasa de emails: {profiles_with_emails/len(scraped_profiles)*100:.1f}%")

            return {
                'success': True,
                'scraped_profiles': scraped_profiles,
                'scraping_time': scraping_time,
                'profiles_with_emails': profiles_with_emails,
                'run_info': run
            }

        except Exception as e:
            logger.error(f"❌ Error en scraping: {e}")
            return {'success': False, 'error': str(e)}


    def clean_scraped_data(self, scraped_data: List[Dict]) -> Dict:
        """
        Limpia los datos scrapeados
        """
        clean_data_list = []
        clean_data = {}
        for scraped in scraped_data:
            clean_data['linkedinUrl'] = scraped.get('linkedinUrl', '').strip()
            clean_data['fullName'] = scraped.get('fullName', '').strip()
            clean_data['firstName'] = scraped.get('firstName', '').strip()
            clean_data['lastName'] = scraped.get('lastName', '').strip()
            clean_data['email'] = scraped.get('email', '').strip()
            clean_data['mobileNumber'] = scraped.get('mobileNumber', '').strip()
            clean_data['headline'] = scraped.get('headline', '').strip()
            clean_data['jobTitle'] = scraped.get('jobTitle', '').strip()
            clean_data['companyName'] = scraped.get('companyName', '').strip()
            clean_data['companyIndustry'] = scraped.get('companyIndustry', '').strip()
            clean_data['companyWebsite'] = scraped.get('companyWebsite', '').strip()
            clean_data['companyLinkedin'] = scraped.get('companyLinkedin', '').strip()
            clean_data['companyFoundedIn'] = scraped.get('companyFoundedIn', '').strip()
            clean_data['companySize'] = scraped.get('companySize', '').strip()
            clean_data['currentJobDuration'] = scraped.get('currentJobDuration', '').strip()
            clean_data['currentJobDurationInYrs'] = scraped.get('currentJobDurationInYrs', '').strip()
            clean_data['topSkillsByEndorsements'] = scraped.get('topSkillsByEndorsements', '').strip()
            clean_data['addressCountryOnly'] = scraped.get('addressCountryOnly', '').strip()
            clean_data['addressWithCountry'] = scraped.get('addressWithCountry', '').strip()
            clean_data_list.append(clean_data)
            clean_data = {}

        return clean_data_list


    def standardize_url(url: str) -> str:
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
            self.standardize_url(scraped.get('linkedinUrl') or scraped.get('url', '')): scraped
        for scraped in scraped_data
        }

        merged_profiles = []

        try:
            for evaluation in selected_profiles:
                original_url = evaluation['url']

                normalized_url = self.standardize_url(original_url)

                # Buscar datos scrapeados correspondientes
                scraped_data_match = scraped_by_url_map.get(normalized_url)

                logger.info(f"🔍 Scraped data match: {scraped_data_match}")
                
                merged_profile = {
                    # Datos de evaluación
                    **evaluation,
                    **scraped_data_match,
                    'scraping_success': scraped_data_match is not None
                }
                logger.info(f"✅ Combinados {len(merged_profiles)} perfiles")
                logger.info(f"✅ Perfiles combinados: {merged_profile}")
                merged_profiles.append(merged_profile)

            print(f"✅ Combinados {len(merged_profiles)} perfiles")
            return merged_profiles

        except Exception as e:
            logger.error(f"❌ Error en merge_evaluation_and_scraping: {e}")
            return []

    def process_contacts_for_bigquery(self, merged_profiles: List[Dict]):
        """
        Procesa los perfiles para crear registros individuales de contactos
        """
        print("\n📊 Procesando contactos para BigQuery...")

        contacts_data = []

        for profile in merged_profiles:
            if not profile['scraping_success']:
                continue

            scraped_data = profile['scraped_data']
            biz_name = profile['original_search']['biz_name']

            # Obtener biz_identifier del mapeo
            biz_identifier = self.company_biz_mapping.get(biz_name, '')

            # Extraer nombre del contacto
            contact_name = scraped_data.get('fullName', '') or scraped_data.get('name', '')
            if not contact_name and 'firstName' in scraped_data and 'lastName' in scraped_data:
                contact_name = f"{scraped_data.get('firstName', '')} {scraped_data.get('lastName', '')}".strip()

            # Extraer posición actual
            contact_position = ''
            if 'positions' in scraped_data and scraped_data['positions']:
                # Tomar la primera posición (más reciente)
                first_position = scraped_data['positions'][0]
                contact_position = first_position.get('title', '')
            elif 'headline' in scraped_data:
                contact_position = scraped_data.get('headline', '')

            # Crear registro de contacto
            contact_record = {
                'biz_identifier': biz_identifier,
                'biz_name': biz_name,
                'contact_full_name': contact_name,
                'contact_role': contact_position,
                'linkedin_profile_url': profile['original_search']['url'],
                'ai_score': profile['ai_evaluation']['score'],
                'scraped_data': json.dumps(scraped_data, ensure_ascii=False, default=str),
                'scraped_at': datetime.now().isoformat()
            }

            contacts_data.append(contact_record)
            print(f"  ✅ Contacto procesado: {contact_name} - {contact_position}")

    #tendria que pushear
            
        self.contacts_results = contacts_data
        print(f"\n📈 Total contactos procesados: {len(contacts_data)}")

    def run_selective_test(self, companies: List[str], max_per_company: int = 20, min_score: int = 7):
        """
        Ejecuta el test selectivo completo
        """
  
        logger.info(f"  Empresas: {len(companies)}")
        logger.info(f"  Máx. perfiles por empresa: {max_per_company}")
        logger.info(f"  Score mínimo para scraping: {min_score}")

        self.test_metrics['start_time'] = datetime.now()
        self.test_metrics['companies_processed'] = companies

        # 1. Buscar perfiles por empresa (máx. limite selecionado por empresa)
        all_profiles = []
        for company in companies:
            logger.info(f"🔍 Empresa: {company}")
            company_profiles = self.search_company_profiles(company, max_per_company)
            all_profiles.extend(company_profiles)
            time.sleep(1)  # Pausa entre empresas

        self.test_metrics['total_profiles_found'] = len(all_profiles)

        if all_profiles == []:
            logger.error("❌ No se encontraron perfiles que cumplan con los criterios de busqueda")
            return []

        # 2. Evaluar TODOS los perfiles y seleccionar los mejores
        logger.info(f"🔍 Perfiles encontrados: {len(all_profiles)}")
        logger.info(f"🔍 Perfiles: {all_profiles}")

        selected_profiles, _ = self.select_best_profiles(all_profiles, min_score)

        if selected_profiles == []:
            logger.error("❌ Ningún perfil alcanzó el score mínimo")
            return []

        # 3. Scrapear SOLO los perfiles seleccionados
        scraping_results = self.scrape_selected_profiles(selected_profiles)

        if not scraping_results['success']:
            logger.error(f"❌ Error en scraping: {scraping_results['error']}")
            return []
        
        logger.info(f"🔍 Scraping results: {scraping_results['scraped_profiles']}")

        # 4. Limpia los datos scrapeados
        cleaned_scraped_data = self.clean_scraped_data(scraping_results['scraped_profiles'])

        logger.info(f"🔍 Cleaned scraped data: {cleaned_scraped_data}")

        # 4. Combinar datos de evaluación con scraping
        merged_profiles = self.merge_evaluation_and_scraping(
            selected_profiles,
            cleaned_scraped_data
        )

        # 5. NUEVO: Procesar contactos para BigQuery
        #self.process_contacts_for_bigquery(merged_profiles)

        return merged_profiles
