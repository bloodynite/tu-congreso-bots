from models.database import Database
from datetime import datetime
import json
import psycopg2
from psycopg2 import sql
from typing import List, Dict, Any
import uuid

class SupabaseService:
    def __init__(self):
        self.db = Database()
    
    def check_parlamentario_exists(self, uuid: str) -> bool:
        """Verifica si un parlamentario ya existe en la BD"""
        try:
            query = "SELECT id FROM parlamentarios WHERE uuid = %s"
            result = self.db.execute_query(query, (uuid,))
            return len(result) > 0
        except Exception as e:
            print(f"Error verificando parlamentario {uuid}: {e}")
            return False
    
    def get_or_create_comite(self, comite_data: Dict[str, Any]) -> int:
        """Obtiene o crea un comité y devuelve su ID"""
        try:
            # Buscar comité por abreviatura o nombre
            query = """
            SELECT id FROM comites 
            WHERE abreviatura = %(abreviatura)s OR nombre = %(nombre)s
            LIMIT 1
            """
            result = self.db.execute_query(query, {
                'abreviatura': comite_data.get('abreviatura'),
                'nombre': comite_data.get('nombre')
            }, fetch_all=False)

            if result:
                return result['id']
            
            # Si no existe, crearlo
            query = """
            INSERT INTO comites (id_comite, uuid, nombre, abreviatura)
            VALUES (%(id_comite)s, %(uuid)s, %(nombre)s, %(abreviatura)s)
            RETURNING id
            """
            result = self.db.execute_query(query, {
                'id_comite': comite_data.get('id_comite', ''),
                'uuid': comite_data.get('uuid'),
                'nombre': comite_data['nombre'],
                'abreviatura': comite_data.get('abreviatura')
            }, fetch_all=False)
            
            return result['id'] if result else None
            
        except Exception as e:
            print(f"Error obteniendo/creando comité {comite_data.get('nombre')}: {e}")
            return None

    def link_parlamentario_comite(self, parlamentario_id: int, comite_id: int) -> bool:
        """Establece la relación entre un parlamentario y un comité"""
        try:
            query = """
            INSERT INTO parlamentario_comite (parlamentario_id, comite_id)
            VALUES (%s, %s)
            ON CONFLICT (parlamentario_id, comite_id) DO NOTHING
            RETURNING id
            """
            result = self.db.execute_query(query, (parlamentario_id, comite_id), fetch_all=False)
            return result is not None
        except Exception as e:
            print(f"Error vinculando parlamentario {parlamentario_id} con comité {comite_id}: {e}")
            return False

    def insert_parlamentario_periodos(self, parlamentario_id: int, periodos_data: List[Dict[str, Any]]) -> bool:
        """Inserta los períodos de un parlamentario"""
        try:
            for periodo in periodos_data:
                query = """
                INSERT INTO periodos (
                    id_periodo, uuid, parlamentario_id, camara, 
                    desde, hasta, vigente
                ) VALUES (
                    %(id_periodo)s, %(uuid)s, %(parlamentario_id)s, %(camara)s,
                    %(desde)s, %(hasta)s, %(vigente)s
                ) ON CONFLICT (parlamentario_id, id_periodo) DO UPDATE SET
                    hasta = EXCLUDED.hasta,
                    vigente = EXCLUDED.vigente
                RETURNING id
                """
                self.db.execute_query(query, {
                    'id_periodo': periodo.get('id_periodo'),
                    'uuid': periodo.get('uuid'),
                    'parlamentario_id': parlamentario_id,
                    'camara': periodo.get('camara', 'S'),  # 'S' por defecto para Senado
                    'desde': periodo.get('desde'),
                    'hasta': periodo.get('hasta'),
                    'vigente': periodo.get('vigente', False)
                })
            return True
        except Exception as e:
            print(f"Error insertando períodos para parlamentario {parlamentario_id}: {e}")
            return False

    def insert_parlamentario(self, parlamentario_data):
        """Inserta o actualiza un parlamentario en la BD con sus relaciones"""
        conn = None
        cursor = None
        try:
            # Iniciar una transacción
            conn = self.db.get_connection()
            cursor = conn.cursor()
            
            # Verificar si los datos están en una propiedad 'data'
            data = parlamentario_data.get('data', parlamentario_data)
            
            # Validar UUID
            uuid_value = data.get('UUID')
            if not uuid_value or uuid_value.lower() == 'uuid':
                print(f"⚠️ Skipping parlamentario {data.get('NOMBRE_COMPLETO')} - Invalid UUID: {uuid_value}")
                return False
            
            try:
                uuid.UUID(uuid_value)
            except ValueError:
                print(f"⚠️ Skipping parlamentario {data.get('NOMBRE_COMPLETO')} - Invalid UUID: {uuid_value}")
                return False
            
            # 1. Check if parlamentario exists usando UUID como identificador único
            check_query = "SELECT id FROM parlamentarios WHERE uuid = %s"
            cursor.execute(check_query, (data.get('UUID'),))
            existing = cursor.fetchone()

            # Procesar el nombre completo para dividirlo en nombre y apellidos
            nombre_completo = data.get('NOMBRE_COMPLETO', '')
            apellidos = ''
            nombre = nombre_completo
            
            if nombre_completo and ',' in nombre_completo:
                # Formato: "Apellido, Nombre"
                apellidos, nombre = [s.strip() for s in nombre_completo.split(',', 1)]
            
            apellido_paterno = apellidos.split()[0] if apellidos else ''
            apellido_materno = ' '.join(apellidos.split()[1:]) if apellidos and len(apellidos.split()) > 1 else ''

            if existing:
                # UPDATE existing parlamentario
                query = """
                UPDATE parlamentarios SET
                    id_parlamentario = %(id_parlamentario)s,
                    slug = %(slug)s,
                    nombre = %(nombre)s,
                    apellido_paterno = %(apellido_paterno)s,
                    apellido_materno = %(apellido_materno)s,
                    camara = %(camara)s,
                    partido_id = %(partido_id)s,
                    partido = %(partido)s,
                    circunscripcion_id = %(circunscripcion_id)s,
                    region = %(region)s,
                    region_id = %(region_id)s,
                    fono = %(fono)s,
                    email = %(email)s,
                    sexo = %(sexo)s,
                    imagen = %(imagen)s,
                    imagen_120 = %(imagen_120)s,
                    imagen_450 = %(imagen_450)s,
                    imagen_600 = %(imagen_600)s,
                    nombre_completo = %(nombre_completo)s,
                    sexo_etiqueta = %(sexo_etiqueta)s,
                    sexo_etiqueta_abreviatura = %(sexo_etiqueta_abreviatura)s,
                    updated_at = NOW()
                WHERE uuid = %(uuid)s
                RETURNING id
                """
                parlamentario_id = existing[0]
            else:
                # INSERT new parlamentario
                query = """
                INSERT INTO parlamentarios (
                    id_parlamentario, uuid, slug, nombre, apellido_paterno, apellido_materno,
                    camara, partido_id, partido, circunscripcion_id, region, region_id,
                    fono, email, sexo, imagen, imagen_120, imagen_450, imagen_600,
                    nombre_completo, sexo_etiqueta, sexo_etiqueta_abreviatura
                ) VALUES (
                    %(id_parlamentario)s, %(uuid)s, %(slug)s, %(nombre)s, %(apellido_paterno)s, 
                    %(apellido_materno)s, %(camara)s, %(partido_id)s, %(partido)s, 
                    %(circunscripcion_id)s, %(region)s, %(region_id)s, %(fono)s, %(email)s, 
                    %(sexo)s, %(imagen)s, %(imagen_120)s, %(imagen_450)s, %(imagen_600)s,
                    %(nombre_completo)s, %(sexo_etiqueta)s, %(sexo_etiqueta_abreviatura)s
                )
                RETURNING id
                """
            
            params = {
                'id_parlamentario': data.get('ID_PARLAMENTARIO'),
                'uuid': data.get('UUID'),
                'slug': data.get('SLUG', '').lower(),
                'nombre': nombre,
                'apellido_paterno': apellido_paterno,
                'apellido_materno': apellido_materno,
                'camara': data.get('CAMARA', 'S'),
                'partido_id': data.get('PARTIDO_ID'),
                'partido': data.get('PARTIDO'),
                'circunscripcion_id': data.get('CIRCUNSCRIPCION_ID'),
                'region': data.get('REGION'),
                'region_id': data.get('REGION_ID'),
                'fono': data.get('FONO'),
                'email': data.get('EMAIL'),
                'sexo': data.get('SEXO', 1),
                'imagen': data.get('IMAGEN'),
                'imagen_120': data.get('IMAGEN_120'),
                'imagen_450': data.get('IMAGEN_450'),
                'imagen_600': data.get('IMAGEN_600'),
                'nombre_completo': nombre_completo,
                'sexo_etiqueta': data.get('SEXO_ETIQUETA', 'No Especificado'),
                'sexo_etiqueta_abreviatura': data.get('SEXO_ETIQUETA_ABREVIATURA', '')
            }
            
            # Ejecutar la consulta
            print(f"🚀 Ejecutando consulta:\n{query}\nCon parámetros:\n{params}")
            cursor.execute(query, params)
            result = cursor.fetchone()
            
            if not result:
                raise Exception("No se pudo insertar el parlamentario")
            
            parlamentario_id = result[0]
            
            # 2. Procesar comités si existen
            comites = data.get('COMITE', [])
            if isinstance(comites, dict):
                comites = [comites]
            
            for comite in comites:
                if not comite:
                    continue
                    
                # Validar campos requeridos
                comite_id = comite.get('ID')  # Cambiado de ID_COMITE a ID
                if not comite_id:
                    print(f"⚠️ Skipping comité {comite.get('NOMBRE')} - ID es requerido")
                    continue
                    
                comite_uuid = comite.get('UUID')
                if not comite_uuid or comite_uuid.lower() == 'uuid':
                    print(f"⚠️ Usando NULL para UUID de comité {comite.get('NOMBRE')} - Valor inválido: {comite_uuid}")
                    comite_uuid = None
                    
                # Verificar si la constraint ya existe primero
                cursor.execute("""
                SELECT 1 FROM pg_constraint 
                WHERE conname = 'comites_id_comite_unique' AND conrelid = 'comites'::regclass
                """)
                
                if not cursor.fetchone():
                    cursor.execute("""
                    ALTER TABLE comites 
                    ADD CONSTRAINT comites_id_comite_unique 
                    UNIQUE (id_comite)
                    """)
                    
                # Luego insertar/actualizar
                query_comite = """
                INSERT INTO comites (id_comite, uuid, nombre, abreviatura)
                VALUES (%(id_comite)s, %(uuid)s, %(nombre)s, %(abreviatura)s)
                ON CONFLICT (id_comite) DO UPDATE SET
                    nombre = EXCLUDED.nombre,
                    abreviatura = EXCLUDED.abreviatura,
                    uuid = COALESCE(EXCLUDED.uuid, comites.uuid)
                RETURNING id
                """
                cursor.execute(query_comite, {
                    'id_comite': comite_id,
                    'uuid': comite_uuid,
                    'nombre': comite.get('NOMBRE'),
                    'abreviatura': comite.get('ABREVIATURA')
                })
                comite_result = cursor.fetchone()
                
                if comite_result:
                    comite_id = comite_result[0]
                    
                    # Insertar la relación parlamentario-comité
                    query_relacion = """
                    INSERT INTO parlamentario_comite (parlamentario_id, comite_id)
                    VALUES (%s, %s)
                    ON CONFLICT (parlamentario_id, comite_id) DO NOTHING
                    """
                    print(f"🚀 Ejecutando consulta:\n{query_relacion}\nCon parámetros:\n{(parlamentario_id, comite_id)}")
                    cursor.execute(query_relacion, (parlamentario_id, comite_id))
            
            # 3. Procesar períodos si existen
            periodos = data.get('PERIODOS', [])
            if isinstance(periodos, dict):
                periodos = [periodos]
            
            for periodo in periodos:
                if not periodo:
                    continue
                
                # Manejar UUID inválido (0 o vacío)
                periodo_uuid = periodo.get('UUID')
                if not periodo_uuid or str(periodo_uuid) == '0':
                    print(f"⚠️ Usando NULL para UUID de período {periodo.get('DESDE')}-{periodo.get('HASTA')}")
                    periodo_uuid = None
                
                # Insertar/actualizar período (sin referencia a parlamentario)
                query_periodo = """
                INSERT INTO periodos (
                    id_periodo, uuid, camara, desde, hasta, vigente
                ) VALUES (
                    %(id_periodo)s, %(uuid)s, %(camara)s, %(desde)s, %(hasta)s, %(vigente)s
                ) ON CONFLICT (id_periodo) DO UPDATE SET
                    uuid = COALESCE(EXCLUDED.uuid, periodos.uuid),
                    camara = EXCLUDED.camara,
                    desde = EXCLUDED.desde,
                    hasta = EXCLUDED.hasta,
                    vigente = EXCLUDED.vigente
                RETURNING id
                """
                cursor.execute(query_periodo, {
                    'id_periodo': periodo.get('ID'),
                    'uuid': periodo_uuid,
                    'camara': periodo.get('CAMARA'),
                    'desde': periodo.get('DESDE'),
                    'hasta': periodo.get('HASTA'),
                    'vigente': bool(periodo.get('VIGENTE', 0))
                })
                periodo_id = cursor.fetchone()[0]
                
                # Insertar relación en tabla intermedia
                query_relacion = """
                INSERT INTO parlamentario_periodo (parlamentario_id, periodo_id)
                VALUES (%s, %s)
                ON CONFLICT (parlamentario_id, periodo_id) DO NOTHING
                """
                cursor.execute(query_relacion, (parlamentario_id, periodo_id))
            
            # Confirmar la transacción
            conn.commit()
            print(f"✅ Parlamentario {nombre_completo} insertado/actualizado correctamente (ID: {parlamentario_id})")
            return True
            
        except Exception as e:
            # Revertir en caso de error
            if conn:
                conn.rollback()
            print(f"❌ Error en la transacción para {data.get('NOMBRE_COMPLETO', 'parlamentario desconocido')}: {str(e)}")
            return False
            
        finally:
            # Cerrar cursor y conexión
            if cursor:
                cursor.close()
            if conn and not conn.closed:
                conn.close()

    def get_existing_uuids(self) -> set:
        """Obtiene todos los UUID existentes en la BD para verificación rápida"""
        try:
            query = "SELECT uuid FROM parlamentarios"
            result = self.db.execute_query(query)
            return {item['uuid'] for item in result} if result else set()
        except Exception as e:
            print(f"Error obteniendo UUIDs existentes: {e}")
            return set()