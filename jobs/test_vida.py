import socket
import ssl
import time
import sys

HOST = "stats.nba.com"
PORT = 443

print(f"🕵️‍♂️ INICIANDO DIAGNÓSTICO FORENSE PARA: {HOST}")
print("-" * 50)

# 1. PRUEBA DE DNS
print(f"1️⃣  [DNS] Resolviendo nombre de dominio...")
try:
    ip_address = socket.gethostbyname(HOST)
    print(f"   ✅ ÉXITO: {HOST} se traduce a la IP {ip_address}")
except Exception as e:
    print(f"   ❌ FALLO CRÍTICO DE DNS: No se puede resolver la IP. Causa: {e}")
    print("   👉 DIAGNÓSTICO: Tu Docker no tiene servidor DNS configurado o no alcanza internet.")
    sys.exit(1)

# 2. PRUEBA DE CONEXIÓN TCP (Ping a nivel de puerto)
print(f"\n2️⃣  [TCP] Intentando conectar al puerto {PORT}...")
try:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5) # 5 segundos de timeout estricto
    start = time.time()
    sock.connect((HOST, PORT))
    end = time.time()
    print(f"   ✅ ÉXITO: Conexión TCP establecida en {end - start:.4f} segundos.")
    sock.close()
except Exception as e:
    print(f"   ❌ FALLO TCP: Los paquetes salen pero no vuelven. Causa: {e}")
    print("   👉 DIAGNÓSTICO: Firewall o problema de enrutamiento en Docker.")
    sys.exit(1)

# 3. PRUEBA DE HANDSHAKE SSL (Aquí es donde suele morir Docker por MTU)
print(f"\n3️⃣  [SSL/TLS] Intentando negociación segura...")
try:
    # Creamos un contexto SSL simple
    context = ssl.create_default_context()
    sock = socket.create_connection((HOST, PORT), timeout=10)
    ssock = context.wrap_socket(sock, server_hostname=HOST)
    
    print(f"   ✅ ÉXITO: Handshake SSL completado.")
    print(f"   🔒 Cifrado: {ssock.cipher()}")
    
    # 4. PRUEBA DE HTTP (Enviar petición real)
    print(f"\n4️⃣  [HTTP] Enviando petición GET simple...")
    request = f"GET / HTTP/1.1\r\nHost: {HOST}\r\nUser-Agent: Docker-Test\r\nConnection: close\r\n\r\n"
    ssock.send(request.encode())
    
    response = ssock.recv(1024) # Leemos el primer chunk
    if response:
        print(f"   ✅ ÉXITO: El servidor respondió {len(response)} bytes.")
        print(f"   📄 Cabecera respuesta: {response.decode('utf-8', errors='ignore').splitlines()[0]}")
    else:
        print(f"   ⚠️ ALERTA: Conexión OK, pero respuesta vacía.")

    ssock.close()

except ssl.SSLError as e:
    print(f"   ❌ FALLO SSL: {e}")
    print("   👉 DIAGNÓSTICO: Problema de certificados. Docker no confía en la NBA.")
except socket.timeout:
    print(f"   ❌ TIMEOUT EN SSL: El servidor dejó de responder durante la negociación.")
    print("   👉 DIAGNÓSTICO MUY PROBABLE: Problema de MTU (Paquetes grandes se pierden).")
except Exception as e:
    print(f"   ❌ ERROR DESCONOCIDO: {e}")

print("-" * 50)