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

        url = profile_data['url']
        title = profile_data['title']
        snippet = profile_data['snippet']
        biz_name = profile_data['biz_name']

        prompt = f"""
                #CONTEXT#
        Actúa como un analista de riesgos especializado en la clasificación de contactos corporativos. Tu objetivo es utilizar la información proporcionada para clasificar con precisión a cada individuo según su rol y su conexión con el departamento de finanzas, siguiendo las reglas establecidas.
        Analiza la información traída por {url} de la búsqueda. Clasifica el perfil del contacto según los criterios de calidad y roles especificados.
        
        # #OBJECTIVE#
        Clasificar el perfil del contacto de la empresa {biz_name} en tres categorías: Tomadores de Decisión, Referenciadores y No referenciadores.
        #INSTRUCTIONS#

        1. Utiliza únicamente la información contenida en {url} de la búsqueda con los links proporcionados y la informacion de {title} y {snippet}. No realices búsquedas externas ni utilices información fuera de la proporcionada.
        2. Clasifica el perfil del contacto según su título o rol:

        - Tomador de Decisión: Dueño, Gerente General, Director de Finanzas, CFO, Gerente de Administración y Finanzas, Jefe de Tesorería, Controller Financiero, Gerente de Planeación Financiera, Contador, Presidente.

        - Referenciador: Personas que no toman decisiones pero tienen contacto directo con los tomadores de decisión (ejemplo: Secretaria, Analista, Gerente Generales, Gerente de Operaciones, Roles de gerencias en general).

        - No Referenciador: Personas que no toman decisiones ni pueden redirigir o comunicar con los tomadores de decisión.

        -Invalido: Si esta persona no trabaja en la empresa (Puede ser que haya trabajado antes pero actualmente no y seguiria siendo invalido)

        - El rol puede variar sutilmente pero si sigue la línea del perfil o esta en otro idioma, clasifícalo en la categoría correspondiente.

        3. Para el perfil del contacto, incluye los siguientes campos: SCORE, EMPRESA_ACTUAL,ROL_FINANZAS,EXPLICACION
        4. Asegurate de que el contacto obtenido, trabaje actualmente en {biz_name}, sino es el caso descartalo como posible contacto y marcalo en su score como invalido.
        5. Devuelve la información en formato de texto, donde el perfil del contacto es un objeto con su información respectiva. Responde en formato:
            SCORE: Tomador de Decisión/Referenciador/No Referenciador/Invalido
            EMPRESA_ACTUAL: Sí/No/Incierto
            ROL_FINANZAS: Sí/No/Incierto
            EXPLICACION: [breve explicación de porque se tomo la decision]

        #EXAMPLES#
        Input 1:
        Tittle = "Gerente General" 
        url = "https://linkedin.com/in/juanperez"
        Snippet = "Gerente General en Maderas y Materiales la Silla, SA DE CV · Experiencia: Maderas y Materiales la Silla, SA DE CV · Ubicación: San Nicolás de los Garza ..."
        biz_name = 'Maderas y materiales la silla, SA DE CV'

        Output 1:
        SCORE: Tomador de Decisión
        EMPRESA_ACTUAL: Sí
        ROL_FINANZAS: Sí
        EXPLICACION: La persona trabaja en un rol central de la empresa, esta relacionado con finanzas y toma las decisiones dentro de la misma.

        Input 2:
        Tittle = "Analista de sistemas"
        url = "https://linkedin.com/in/sofiaruiz"
        Snippet = "Analista de Sistemas en Maderas y Materiales la Silla, SA DE CV · Experiencia en desarrollo de software..."
        biz_name = 'Maderas y materiales la silla, SA DE CV'

        Output 2:
        SCORE: No Referenciador
        EMPRESA_ACTUAL: Sí
        ROL_FINANZAS: No
        EXPLICACION: La persona trabaja en la empresa pero su rol de "Analista de Sistemas" no tiene relación directa con finanzas ni con la toma de decisiones financieras o administrativas.

        Input 3:
        Tittle = "Gerente de Finanzas"
        url = "https://linkedin.com/in/carlosgomez"
        Snippet = "Gerente de Finanzas en 'Empresa XYZ' · Experiencia previa en Maderas y Materiales la Silla, SA DE CV (2018-2022)."
        biz_name = 'Maderas y materiales la silla, SA DE CV'

        Output 3:
        SCORE: Invalido
        EMPRESA_ACTUAL: No
        ROL_FINANZAS: Sí
        EXPLICACION: El contacto no trabaja actualmente en la empresa objetivo. Su rol de "Gerente de Finanzas" es de un empleo anterior.


        Input 4:
        Tittle = "Senior Manager"
        url = "https://linkedin.com/in/lauramartinez"
        Snippet = "Senior Manager en Maderas y Materiales la Silla, SA DE CV."
        biz_name = 'Maderas y materiales la silla, SA DE CV'

        Output 4:
        SCORE: Referenciador
        EMPRESA_ACTUAL: Sí
        ROL_FINANZAS: Incierto
        EXPLICACION: El título de "Senior Manager" es ambiguo. No especifica un área (finanzas, operaciones, marketing, etc.), por lo que no es posible confirmar si tiene un rol de finanzas. Se clasifica como Referenciador por su posición jerárquica.
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

            score = score_match.group(1).strip() if score_match else "Invalido"
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