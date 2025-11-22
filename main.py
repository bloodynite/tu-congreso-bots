import os
import time
from dotenv import load_dotenv
from services.supabase_service import SupabaseService
from utils.helpers import APIHelper

load_dotenv()

class ParlamentariosBot:
    def __init__(self):
        self.supabase_service = SupabaseService()
        self.api_helper = APIHelper()
    
    def run(self):
        """Ejecuta el proceso principal del bot"""
        print("🤖 Iniciando bot de parlamentarios...")
        
        # Obtener datos de la API
        print("📡 Obteniendo datos de la API...")
        parlamentarios_data = self.api_helper.fetch_parlamentarios_data()
        
        if not parlamentarios_data:
            print("❌ No se pudieron obtener datos de la API")
            return
        
        print(f"📊 Se encontraron {len(parlamentarios_data)} parlamentarios en la API")
        
        # Obtener UUIDs existentes para verificación rápida
        print("🔍 Verificando parlamentarios existentes...")
        existing_uuids = self.supabase_service.get_existing_uuids()
        print(f"📋 Hay {len(existing_uuids)} parlamentarios en la base de datos")
        
        # Procesar cada parlamentario
        nuevos = 0
        existentes = 0
        errores = 0
        
        for parlamentario in parlamentarios_data:
            uuid = parlamentario.get('UUID')
            
            if not uuid:
                print("⚠️ Parlamentario sin UUID, omitiendo...")
                errores += 1
                continue
            
            # Verificar si ya existe
            if uuid in existing_uuids:
                print(f"⏭️ Parlamentario {parlamentario.get('NOMBRE_COMPLETO')} ya existe, omitiendo")
                existentes += 1
                continue
            
            # Procesar datos
            processed_data = self.api_helper.process_parlamentario_data(parlamentario)
            
            # Insertar en la base de datos
            if self.supabase_service.insert_parlamentario(processed_data):
                nuevos += 1
            else:
                errores += 1
            
            # Pequeña pausa para no saturar la API/BD
            time.sleep(0.1)
        
        # Resumen
        print("\n" + "="*50)
        print("📊 RESUMEN DE EJECUCIÓN")
        print("="*50)
        print(f"✅ Nuevos insertados: {nuevos}")
        print(f"⏭️ Ya existentes: {existentes}")
        print(f"❌ Errores: {errores}")
        print(f"📈 Total procesados: {len(parlamentarios_data)}")
        print("="*50)

if __name__ == "__main__":
    bot = ParlamentariosBot()
    bot.run()