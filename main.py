import os
import time
import requests
from dotenv import load_dotenv
from services.supabase_service import SupabaseService
from utils.helpers import APIHelper
import warnings
from psycopg2 import OperationalError

load_dotenv()

warnings.filterwarnings('ignore', message='invalid configuration parameter name "supautils.disable_program"')

class ParlamentariosBot:
    def __init__(self):
        self.supabase_service = SupabaseService()
        self.api_helper = APIHelper()
    
    def run(self):
        """Ejecuta el flujo principal del bot"""
        print("🤖 Iniciando bot de parlamentarios...")
        try:
            # Obtener datos manteniendo estructura original
            api_data = self.api_helper.fetch_parlamentarios_data()
            
            # Procesar cargos primero (si existen)
            if api_data.get('cargos'):
                self.supabase_service.procesar_cargos_senado({'data': {'data': api_data['cargos']}})
            
            # Procesar parlamentarios (flujo original)
            parlamentarios_data = api_data.get('parlamentarios', [])
            print(f"📊 Se encontraron {len(parlamentarios_data)} parlamentarios en la API")
            
            # Obtener UUIDs existentes para verificación rápida
            print("🔍 Verificando parlamentarios existentes...")
            existing_uuids = self.supabase_service.get_existing_uuids()
            print(f"📋 Hay {len(existing_uuids)} parlamentarios en la base de datos")
            
            # Procesar cada parlamentario
            nuevos = 0
            existentes = 0
            errores = 0
            uuid_invalidos = 0
            
            for data in parlamentarios_data:
                # Procesar datos y saltar si son inválidos
                processed_data = self.api_helper.process_parlamentario_data(data)
                if not processed_data:
                    print(f"⚠️ Saltando datos inválidos: {data.get('NOMBRE', 'Sin nombre')}")
                    continue
                    
                # Procesar parlamentario
                if self.supabase_service.check_parlamentario_exists(processed_data['UUID']):
                    existentes += 1
                else:
                    nuevos += 1
                    
                if not self.supabase_service.insert_parlamentario(processed_data):
                    errores += 1
                
                # Contar UUIDs inválidos en comités (maneja tanto strings como dicts)
                comites = processed_data.get('COMITE', [])
                if isinstance(comites, dict):
                    comites = [comites]
                elif isinstance(comites, str):
                    comites = []
                    
                if any(isinstance(c, dict) and c.get('UUID') in ['uuid', None] for c in comites):
                    uuid_invalidos += 1
                    
                time.sleep(0.1)

            print("\n" + "="*50)
            print(f"📊 RESUMEN FINAL - PARLAMENTARIOS")
            print("="*50)
            print(f"✅ Nuevos insertados: {nuevos}")
            print(f"🔄 Actualizados: {existentes}")
            print(f"⚠️ Con UUID inválidos: {uuid_invalidos}")
            print(f"❌ Errores: {errores}")
            print(f"📈 Total procesados: {len(parlamentarios_data)}")
            print("="*50 + "\n")
        except OperationalError as e:
            print(f"Error de operación: {e}")

if __name__ == "__main__":
    bot = ParlamentariosBot()
    bot.run()