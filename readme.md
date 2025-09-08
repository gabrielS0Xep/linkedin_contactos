Refactorizacion de el google collab de linkedin contactos
# LinkedIn Scraper API

Este proyecto ha sido transformado de un scraper batch a una **API REST con Flask**. Ahora puedes scraper empresas individuales y obtener la informacion de su contacto enviando requests HTTP, eliminando completamente la dependencia de BigQuery.

## 📁 Estructura del Proyecto

```
scraper-linkedin/
├── .env                    # Variables de entorno (crear manualmente)
├── config.py              # Configuración y variables de entorno
├── linkedin_contacts_scraper.py    # Clase principal del scraper
├── main.py                # API Flask
├── *_services.py       # Interfaces para evitar acople con las distintas herramientas de GCP
├── requirements.txt       # Dependencias (actualizado)
└── README.md # Este archivo

## 🎯 Mejoras de la API

### ✅ Arquitectura REST
- API REST con Flask
- Endpoints claros y documentados
- Respuestas JSON estructuradas

### ✅ Procesamiento Individual
- Scraping por empresa individual
- No más procesamiento batch
- Respuestas inmediatas

### ✅ Sin Dependencias Externas
- No requiere BigQuery
- No requiere bases de datos
- Solo APIs externas necesarias

### ✅ Fácil Integración
- API HTTP estándar
- Formato JSON
- CORS habilitado

## Formato de solicitud
"/scrape" , metodo POST

Body JSON (opcional):
    {
        "batch_size": 10,
        "min_score": 7,
        "max_per_company": 4
    }


## 📝 Formato de Respuesta

### Respuesta Exitosa
```json
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
```

### Respuesta de Error
```json
{
  "success": false,
  "error": "No se encontraron páginas de LinkedIn válidas según IA",
  "company_name": "EmpresaInexistente",
  "search_results": [],
  "detailed_data": null,
  "processing_time_seconds": 8.2,
  "timestamp": "2024-01-15T10:30:45.123Z"
}
```


## 🐛 Solución de Problemas

### Error: "Faltan variables de entorno"
- Verifica que el archivo `.env` existe
- Confirma que todas las API keys están configuradas

### Error: "Content-Type debe ser application/json"
- Asegúrate de enviar header `Content-Type: application/json`
- Verifica que el body sea JSON válido

### Error de conexión
- Verifica que el servidor esté corriendo
- Confirma el puerto correcto (por defecto 5000)

## 📈 Beneficios de la API

1. **Procesamiento Individual**: Scraper empresas segun la demanda
3. **Integración Fácil**: API REST estándar compatible con cualquier lenguaje
4. **Respuestas Inmediatas**: Resultados en tiempo real
5. **Escalable**: Puede manejarse con load balancers y múltiples instancias
6. **Flexible**: Configuración sencilla y personalizable

## 🔐 Seguridad

- ⚠️ **NUNCA** subas el archivo `.env` al repositorio
- ⚠️ **NUNCA** hardcodees API keys en el código
- ✅ Usa variables de entorno para configuración sensible
- ✅ Agrega `.env` a `.gitignore`
- 🔒 Considera usar HTTPS en producción
- 🚧 Implementa autenticación si es necesario

## 📞 Soporte

Si tienes problemas con la API:
1. Verifica que todos los archivos fueron creados correctamente
2. Confirma que el archivo `.env` tiene todas las variables
3. Ejecuta `pip install -r requirements.txt` nuevamente
4. Verifica que el servidor esté corriendo en el puerto correcto
5. Prueba los endpoints con curl o Postman

## 📊 Monitoreo

La API incluye endpoints útiles para monitoreo:
- `GET /status` - Estado detallado del servicio
