#!/usr/bin/env python3
import subprocess
import sys
import os

def check_venv():
    """Verifica si estamos en un entorno virtual"""
    if hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix):
        print("✅ Entorno virtual activado")
        return True
    else:
        print("❌ No estás en un entorno virtual. Actívalo con: source venv/bin/activate")
        return False

def install_requirements():
    """Instala los requirements necesarios"""
    if not check_venv():
        return False
    
    try:
        # Actualizar pip primero
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "pip"])
        
        # Instalar dependencias
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("✅ Dependencias instaladas correctamente")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Error instalando dependencias: {e}")
        return False

if __name__ == "__main__":
    install_requirements()