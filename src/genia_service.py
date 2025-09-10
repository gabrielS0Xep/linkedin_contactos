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

        url = profile_data['url']
        title = profile_data['title']
        snippet = profile_data['snippet']
        biz_name = profile_data['biz_name']

        new_prompt = f"""
        #CONTEXT#
        # Analiza la información traída por {url} de la búsqueda con los links proporcionados. Clasifica los contactos encontrados según los criterios de calidad y roles especificados.
        # #OBJECTIVE#
        
        Clasificar los contactos estructurados de la empresa {biz_name} en tres categorías: Tomadores de Decisión, Referenciadores y No referenciadores, y devolver la información en formato JSON ordenada por estas categorías.
    `    #INSTRUCTIONS#

        1. Utiliza únicamente la información contenida en {url} de la búsqueda con los links proporcionados y la informacion de {title} y {snippet}. No realices búsquedas externas ni utilices información fuera de la proporcionada.

        2. Para cada contacto, verifica que actualmente trabaje en {biz_name}. Si no es así, descártalo.

        3. Clasifica cada contacto según su título o rol:

        - Tomadores de Decisión: Dueño, Gerente General, Director de Finanzas, CFO, Gerente de Administración y Finanzas, Jefe de Tesorería, Controller Financiero, Gerente de Planeación Financiera, Contador, Presidente.

        - Referenciadores: Personas que no toman decisiones pero tienen contacto directo con los tomadores de decisión (ejemplo: Secretaria, Analista, Gerente Generales, Gerente de Operaciones, Roles de gerencias en general).

        - No Referenciadores: Personas que no toman decisiones ni pueden redirigir o comunicar con los tomadores de decisión.

        - El rol puede variar sutilmente pero si sigue la línea del perfil o esta en otro idioma, clasifícalo en la categoría correspondiente.

        4. Para cada contacto, incluye los siguientes campos: SCORE, EMPRESA_ACTUAL,ROL_FINANZAS,EXPLICACION
        5. Asegurate de que el contacto obtenido, trabaje actualmente en {biz_name}, sino es el caso descartalo como posible contacto.
        6. Devuelve la información en formato de texto, donde cada contacto es un objeto con su información respectiva. Responde en formato:
            SCORE: X
            EMPRESA_ACTUAL: Sí/No/Incierto
            ROL_FINANZAS: Sí/No/Incierto
            EXPLICACION: [breve explicación]

        #EXAMPLES#

        Input:

        "Contactos Estructurados": [

            "nombre": "Juan Pérez", "título": "Gerente General", "url": "https://linkedin.com/in/juanperez",

            "nombre": "Ana López", "título": "Secretaria", "url": "https://linkedin.com/in/analopez",

            "nombre": "Carlos Ruiz", "título": "Analista", "url": "https://linkedin.com/in/carlosruiz"

        ]



        Output esperado:

        [

        

            "nombre": "Juan Pérez",

            "título": "Gerente General",

            "url": "https://linkedin.com/in/juanperez",

            "calidad": "Tomador de Decisión",

            "empresa": "empresa 1",

        ,

        

            "nombre": "Ana López",

            "título": "Secretaria",

            "url": "https://linkedin.com/in/analopez",

            "calidad": "Referenciador",

            "empresa": "empresa 1",

        ,

        

            "nombre": "Carlos Ruiz",

            "título": "Analista",

            "url": "https://linkedin.com/in/carlosruiz",

            "calidad": "Referenciador",

            "empresa": "empresa 1",

        

        ]
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