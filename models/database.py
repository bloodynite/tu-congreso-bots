import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import os.path

# Cargar variables de entorno desde config/.env
dotenv_path = os.path.join(os.path.dirname(__file__), '..', 'config', '.env')
load_dotenv(dotenv_path)

class Database:
    def __init__(self):
        self.db_params = {
            'dbname': os.getenv('DB_NAME', 'postgres'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', ''),
            'host': os.getenv('DB_HOST', 'db.moevijsrcacibstkhkxq.supabase.co'),
            'port': os.getenv('DB_PORT', '5432'),
            'sslmode': 'require',
            'connect_timeout': 10
        }
        
        # Debug: Verificar que las variables se cargan
        print("🔧 Configuración de base de datos cargada:")
        print(f"   Host: {self.db_params['host']}")
        print(f"   Base de datos: {self.db_params['dbname']}")
        print(f"   Usuario: {self.db_params['user']}")
        
        # Crear la conexión
        self.connection = self._create_connection()
    
    def _create_connection(self):
        """Crea una nueva conexión a la base de datos"""
        try:
            conn = psycopg2.connect(**self.db_params)
            conn.autocommit = False
            return conn
        except Exception as e:
            print(f"❌ Error al conectar a la base de datos: {e}")
            raise
    
    def get_connection(self):
        """Obtiene una conexión a la base de datos"""
        try:
            if self.connection.closed:
                self.connection = self._create_connection()
            return self.connection
        except Exception as e:
            print(f"❌ Error al obtener conexión: {e}")
            raise
    
    def execute_query(self, query, params=None, fetch_all=True):
        """Ejecuta una consulta y devuelve los resultados"""
        conn = self.get_connection()
        cursor = None
        try:
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute(query, params or ())
            if fetch_all:
                result = cursor.fetchall()
                # Convertir cada fila a dict si es necesario
                return [dict(row) if not isinstance(row, dict) else row for row in result]
            else:
                result = cursor.fetchone()
                return dict(result) if result and not isinstance(result, dict) else result
        except Exception as e:
            print(f"❌ Error al ejecutar consulta: {e}")
            conn.rollback()
            raise
        finally:
            if cursor:
                cursor.close()
            conn.commit()
    
    def close(self):
        """Cierra la conexión a la base de datos"""
        if hasattr(self, 'connection') and self.connection and not self.connection.closed:
            self.connection.close()
    
    def __del__(self):
        """Asegura que la conexión se cierre cuando el objeto es destruido"""
        self.close()