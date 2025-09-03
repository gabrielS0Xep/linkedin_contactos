import requests
import json
import time
from typing import List, Dict, Tuple
from apify_client import ApifyClient
import re
from datetime import datetime
from config import Config

from genia_service import GenIaService

import logging
logger = logging.getLogger(__name__)

class LinkedInContactsSelectiveScraper:
    def __init__(self, serper_api_key: str, apify_token: str):
        self.serper_api_key = serper_api_key
    
        self.apify_client = ApifyClient(apify_token)
        self.company_biz_mapping = {}  # Mapeo de nombres a biz_identifier

        # NUEVO: Configuración de proyecto y dataset específicos
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
                                        'company_searched': company_name,
                                        'query_used': query
                                    })
                                    logger.info(f"  ✓ Perfil {len(linkedin_profiles)}: {title[:50]}...")

                time.sleep(1)  # Rate limiting

            except Exception as e:
                print(f"  ❌ Error en búsqueda: {str(e)}")
                continue

        print(f"  📊 Total encontrados para {company_name}: {len(linkedin_profiles)}")
        return linkedin_profiles

###Esta funcion es la que encuentra con serper api los links de los perfiles de linkedin

    

    def select_best_profiles(self, all_profiles: List[Dict], min_score: int = 7) -> Tuple[List[Dict], List[Dict]]:
        """
        Evalúa todos los perfiles y selecciona solo los mejores
        """
        print(f"\n🤖 Evaluando {len(all_profiles)} perfiles con IA...")
        print(f"🎯 Buscando perfiles con score >= {min_score}")

        evaluated_profiles = []
        high_score_profiles = []
        genia_service = GenIaService(self.project_id, self.location)

        for i, profile_data in enumerate(all_profiles, 1):
            try:
                print(f"  Evaluando {i}/{len(all_profiles)}: {profile_data['title'][:40]}...")


                result = genia_service.evaluate_profile_relevance_detailed(
                    profile_data,
                    profile_data['company_searched']
                )

                structured_info = genia_service.extract_structured_info(result)


                score = structured_info['score']
                explanation = structured_info['explicacion']
                details = structured_info['details']

                evaluation = {
                    **profile_data,
                    'ai_score': score,
                    'ai_explanation': explanation,
                    'ai_details': details,
                    'evaluation_timestamp': datetime.now().isoformat()
                }

                evaluated_profiles.append(evaluation)

                if score >= min_score:
                    high_score_profiles.append(evaluation)
                    print(f"    ✅ SELECCIONADO - Score: {score} - {explanation[:50]}...")
                else:
                    print(f"    ❌ Descartado - Score: {score} - {explanation[:50]}...")

                time.sleep(0.3)  # Rate limiting para OpenAI

            except Exception as e:
                print(f"    ❌ Error evaluando perfil: {str(e)}")
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
        print(f"💰 Costo estimado: ${estimated_cost:.2f}")

        try:
            run_input = {
                "profileUrls": profile_urls
            }

            print("⏳ Ejecutando dev_fusion/linkedin-profile-scraper...")
            start_time = time.time()

            run = self.apify_client.actor("dev_fusion/linkedin-profile-scraper").call(run_input=run_input)

            scraping_time = time.time() - start_time
            print(f"⏱️ Tiempo de scraping: {scraping_time:.1f} segundos")

            # Obtener resultados
            scraped_profiles = []
            for item in self.apify_client.dataset(run["defaultDatasetId"]).iterate_items():
                scraped_profiles.append(item)

            self.test_metrics['profiles_scraped'] = len(scraped_profiles)

            # Analizar resultados
            profiles_with_emails = 0
            for profile in scraped_profiles:
                if profile.get('email') or profile.get('emails'):
                    profiles_with_emails += 1

            self.test_metrics['profiles_with_emails'] = profiles_with_emails

            print(f"✅ Scraping completado:")
            print(f"  Perfiles scrapeados: {len(scraped_profiles)}")
            print(f"  Perfiles con email: {profiles_with_emails}")
            print(f"  Tasa de emails: {profiles_with_emails/len(scraped_profiles)*100:.1f}%")

            return {
                'success': True,
                'scraped_profiles': scraped_profiles,
                'scraping_time': scraping_time,
                'profiles_with_emails': profiles_with_emails,
                'run_info': run
            }

        except Exception as e:
            print(f"❌ Error en scraping: {str(e)}")
            return {'success': False, 'error': str(e)}

    def merge_evaluation_and_scraping(self, selected_profiles: List[Dict], scraped_data: List[Dict]) -> List[Dict]:
        """
        Combina los datos de evaluación con los datos scrapeados
        """
        print("\n🔗 Combinando datos de evaluación con scraping...")

        merged_profiles = []

        # Crear un diccionario para mapeo rápido por URL
        scraped_by_url = {}
        for scraped in scraped_data:
            profile_url = scraped.get('profileUrl') or scraped.get('url', '')
            if profile_url:
                scraped_by_url[profile_url] = scraped

        for evaluation in selected_profiles:
            original_url = evaluation['url']

            # Buscar datos scrapeados correspondientes
            scraped_data_match = None
            for url, scraped in scraped_by_url.items():
                if original_url in url or url in original_url:
                    scraped_data_match = scraped
                    break

            merged_profile = {
                # Datos de evaluación
                'original_search': {
                    'url': evaluation['url'],
                    'title': evaluation['title'],
                    'snippet': evaluation['snippet'],
                    'company_searched': evaluation['company_searched'],
                    'query_used': evaluation['query_used']
                },
                'ai_evaluation': {
                    'score': evaluation['ai_score'],
                    'explanation': evaluation['ai_explanation'],
                    'details': evaluation['ai_details']
                },
                # Datos scrapeados (si existen)
                'scraped_data': scraped_data_match if scraped_data_match else None,
                'scraping_success': scraped_data_match is not None
            }
            logger.info(f"✅ Combinados {len(merged_profiles)} perfiles")
            logger.info(f"✅ Perfiles combinados: {merged_profile}")
            merged_profiles.append(merged_profile)

        print(f"✅ Combinados {len(merged_profiles)} perfiles")
        return merged_profiles

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
            company_name = profile['original_search']['company_searched']

            # Obtener biz_identifier del mapeo
            biz_identifier = self.company_biz_mapping.get(company_name, '')

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
                'biz_name': company_name,
                'contact_name': contact_name,
                'contact_position': contact_position,
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

        # 1. Buscar perfiles por empresa (máx. 20 cada una)
        all_profiles = []
        for company in companies:
            company_profiles = self.search_company_profiles(company, max_per_company)
            all_profiles.extend(company_profiles)
            time.sleep(1)  # Pausa entre empresas

        self.test_metrics['total_profiles_found'] = len(all_profiles)

        if not all_profiles:
            print("❌ No se encontraron perfiles")
            return None

        # 2. Evaluar TODOS los perfiles y seleccionar los mejores
        selected_profiles, all_evaluations = self.select_best_profiles(all_profiles, min_score)

        if not selected_profiles:
            logger.error("❌ Ningún perfil alcanzó el score mínimo")
            return None

        # 3. Scrapear SOLO los perfiles seleccionados
        scraping_results = self.scrape_selected_profiles(selected_profiles)

        if not scraping_results['success']:
            print(f"❌ Error en scraping: {scraping_results['error']}")
            return None

        # 4. Combinar datos de evaluación con scraping
        merged_profiles = self.merge_evaluation_and_scraping(
            selected_profiles,
            scraping_results['scraped_profiles']
        )

        # 5. NUEVO: Procesar contactos para BigQuery
        self.process_contacts_for_bigquery(merged_profiles)

        return merged_profiles
