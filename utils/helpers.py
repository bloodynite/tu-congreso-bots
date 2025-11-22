import requests
import os
from dotenv import load_dotenv

load_dotenv()

class APIHelper:
    def __init__(self):
        self.api_url = os.getenv('API_URL')
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
    
    def fetch_parlamentarios_data(self):
        """Obtiene los datos de la API del Senado"""
        try:
            print(f"🔍 Conectando a: {self.api_url}")
            response = requests.get(self.api_url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Navegar por la estructura JSON para encontrar los parlamentarios
            page_props = data.get('pageProps', {})
            components = page_props.get('resource', {}).get('components', [])
            
            # Buscar el componente que contiene los parlamentarios
            for component in components:
                if component.get('type') == 'paragraph--component_api_reference':
                    computed_components = component.get('computedComponents', {})
                    if 'data' in computed_components and 'parlamentarios' in computed_components['data']:
                        parlamentarios_data = computed_components['data']['parlamentarios']
                        return parlamentarios_data.get('data', [])
            
            print("❌ No se encontraron datos de parlamentarios en la respuesta")
            return []
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Error de conexión: {e}")
            return []
        except Exception as e:
            print(f"❌ Error procesando datos: {e}")
            return []
    
    def process_parlamentario_data(self, raw_data):
        """Procesa y limpia los datos de un parlamentario"""
        try:
            # Asegurar que los campos anidados estén en formato correcto
            processed = raw_data.copy()
            
            # Convertir comité a JSON string si es un diccionario
            comite = processed.get('COMITE')
            if comite and isinstance(comite, dict):
                processed['COMITE'] = comite
            
            # Convertir períodos a JSON string si es una lista
            periodos = processed.get('PERIODOS')
            if periodos and isinstance(periodos, list):
                processed['PERIODOS'] = periodos
            
            return processed
            
        except Exception as e:
            print(f"Error procesando datos del parlamentario: {e}")
            return raw_data