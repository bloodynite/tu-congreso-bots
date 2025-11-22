import requests
import os
import json
from dotenv import load_dotenv

load_dotenv()

class APIHelper:
    def __init__(self):
        self.api_url = os.getenv('API_URL', 'https://www.senado.cl/_next/data/2nIj_T31TxUMBaXNPeOA5/senadoras-y-senadores/listado-de-senadoras-y-senadores.json')
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
    
    def get_api_url(self):
        """Obtiene la URL configurada de la API"""
        return self.api_url
    
    def fetch_parlamentarios_data(self):
        """Obtiene ambos conjuntos de datos de la API"""
        try:
            response = requests.get(self.api_url, headers=self.headers, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            result = {'cargos': [], 'parlamentarios': []}
            components = data.get('pageProps', {}).get('resource', {}).get('components', [])
            
            for component in components:
                if component.get('type') == 'paragraph--component_api_reference':
                    computed = component.get('computedComponents', {})
                    
                    # Primer componente: Presidente/Vicepresidente
                    if computed.get('data', {}).get('data') and not computed.get('data', {}).get('parlamentarios'):
                        result['cargos'] = computed['data']['data']
                    
                    # Segundo componente: Parlamentarios
                    elif computed.get('data', {}).get('parlamentarios'):
                        result['parlamentarios'] = computed['data']['parlamentarios']['data']
            
            return result
            
        except Exception as e:
            print(f"❌ Error obteniendo datos: {str(e)}")
            return {'cargos': [], 'parlamentarios': []}
    
    def process_parlamentario_data(self, raw_data):
        """Procesa datos de parlamentario manteniendo estructura original"""
        try:
            if not raw_data or not raw_data.get('UUID'):
                print("⚠️ Datos inválidos - Falta UUID")
                return None
                
            # Conservar todos los campos originales
            processed = {
                'UUID': raw_data['UUID'],
                'SLUG': raw_data.get('SLUG', ''),  # Mantener string vacío si no existe
                'NOMBRE': raw_data.get('NOMBRE', ''),
                'APELLIDO_PATERNO': raw_data.get('APELLIDO_PATERNO', ''),
                'APELLIDO_MATERNO': raw_data.get('APELLIDO_MATERNO', ''),
                'NOMBRE_COMPLETO': raw_data.get('NOMBRE_COMPLETO', ''),
                'PERIODOS': raw_data.get('PERIODOS', []),
                'COMITE': raw_data.get('COMITE', [])
            }
            return processed
            
        except Exception as e:
            print(f"❌ Error procesando datos: {str(e)}")
            return None