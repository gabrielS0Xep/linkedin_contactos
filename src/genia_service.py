import logging
from google import genai
from google.genai import types
from config import Config
from typing import Dict, Tuple
import re


logger = logging.getLogger(__name__)

class GenIaService:
    def __init__(self, project_id: str, location: str):
        self.genai_client = genai.Client(
            vertexai = True,
            project=project_id,
            location=location,
            http_options=types.HttpOptions(api_version='v1')
        )
    
    def evaluate_profile_relevance_detailed(self, profile_data: Dict) :
        """
        Evalúa un perfil usando IA con información más detallada
        """
        profile_text = f"""
        URL: {profile_data['url']}
        Título: {profile_data['title']}
        Snippet: {profile_data['snippet']}
        Empresa buscada: {profile_data['biz_name']}
        """

        prompt = f"""
        Evalúa este perfil de LinkedIn para decisiones financieras en la empresa "{profile_data['biz_name']}":

        {profile_text}

        Evalúa:
        1. ¿Trabaja ACTUALMENTE en "{profile_data['biz_name']}"? (crítico)
        2. ¿Su rol actual es de finanzas/contabilidad? (importante, considera roles con nombres en inglés como finance también parte de roles estratégicos)
        3. ¿Tiene poder de decisión financiera? (importante)
        4. ¿Nivel de seniority? (relevante)

        Scoring:
        - 9-10: CEO/CFO actual de la empresa
        - 7-8: Finance Director/Controller actual
        - 5-6: Finance Manager/Analyst actual
        - 3-4: Finance junior o ex-empleado
        - 1-2: No relevante o no trabaja en la empresa

        Responde en formato:
        SCORE: X
        EMPRESA_ACTUAL: Sí/No/Incierto
        ROL_FINANZAS: Sí/No/Incierto
        EXPLICACION: [breve explicación]
        """

        try:
            """
            response = self.openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=500,
                temperature=0.3
            )
            """
            response = self.genai_client.models.generate_content(
                model=Config.GEMINI_MODEL_NAME,
                contents=prompt,
                config=Config.GEMINI_CONFIG

            )
            result = response.text.strip()

            logger.info(f"🔍 Evaluación de perfil: {result}")
            
            return result
        except Exception as e:
            logger.error(f"❌ Error evaluando perfil: {str(e)}")
            raise e

    def extract_structured_info(self, result: str) :
        """
        Extrae información estructurada de la respuesta
        """ 
        try:
            # Extraer información estructurada de la respuesta
            score_match = re.search(r'SCORE:\s*(\d+)', result)
            empresa_match = re.search(r'EMPRESA_ACTUAL:\s*([^\n]+)', result)
            rol_match = re.search(r'ROL_FINANZAS:\s*([^\n]+)', result)
            explicacion_match = re.search(r'EXPLICACION:\s*([^\n]+)', result)

            score = int(score_match.group(1)) if score_match else 0
            empresa_actual = empresa_match.group(1).strip() if empresa_match else "Incierto"
            rol_finanzas = rol_match.group(1).strip() if rol_match else "Incierto"
            explicacion = explicacion_match.group(1).strip() if explicacion_match else "Sin explicación"
        
            structured_info = {
                'score': score,
                'empresa_actual': empresa_actual,
                'rol_finanzas': rol_finanzas,
                'explicacion': explicacion
            }

            logger.info(f"🔍 Evaluación informacion estructurada: {structured_info}")
            return structured_info

        except Exception as e:
            logger.error(f"❌ Error evaluando: {str(e)}")
            raise e