"""
Script: download_cae_images.py
Descrição: Descarrega imagens do JSON do Apify (Instagram CAE) e faz upload para Google Drive

Requisitos (instala no PowerShell):
    pip install requests google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client

Como usar:
    1. Preenche CLIENT_ID e CLIENT_SECRET abaixo
    2. Coloca o ficheiro JSON do Apify na mesma pasta
    3. Corre: python download_cae_images.py
    4. Na primeira execução abre o browser para autorizar o acesso ao Drive
"""

import json
import os
import time
import requests
from pathlib import Path
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# ─── CONFIGURAÇÃO ────────────────────────────────────────────────────────────

# Cola aqui as tuas chaves do Google Cloud Console
try:
    from segredos import CLIENT_ID, CLIENT_SECRET
except ImportError:
    CLIENT_ID     = "COLA_AQUI"
    CLIENT_SECRET = "COLA_AQUI"

# Nome do ficheiro JSON exportado do Apify (coloca na mesma pasta do script)
JSON_FILE = "dataset_instagram-scraper_2026-03-28_08-19-44-810.json"

# Nome da pasta no Google Drive onde as imagens serão guardadas
DRIVE_FOLDER_NAME = "CAE Eventos - Cartazes"

# Pasta local temporária
LOCAL_TEMP_FOLDER = "cae_imagens_temp"

# Scopes
SCOPES = ["https://www.googleapis.com/auth/drive.file"]

# Headers para simular browser
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://www.instagram.com/",
    "Accept": "image/webp,image/apng,image/*,*/*;q=0.8",
}

# ─── AUTENTICAÇÃO GOOGLE DRIVE ───────────────────────────────────────────────

def autenticar_google_drive():
    client_config = {
        "installed": {
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": ["urn:ietf:wg:oauth:2.0:oob", "http://localhost"]
        }
    }

    creds = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_config(client_config, SCOPES)
            creds = flow.run_local_server(port=0)

        with open("token.json", "w") as token:
            token.write(creds.to_json())

    return build("drive", "v3", credentials=creds)


# ─── FUNÇÕES GOOGLE DRIVE ────────────────────────────────────────────────────

def criar_ou_obter_pasta(service, nome_pasta):
    query = f"name='{nome_pasta}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    resultado = service.files().list(q=query, fields="files(id, name)").execute()
    pastas = resultado.get("files", [])

    if pastas:
        pasta_id = pastas[0]["id"]
        print(f"✅ Pasta existente: '{nome_pasta}' (ID: {pasta_id})")
        return pasta_id

    metadata = {
        "name": nome_pasta,
        "mimeType": "application/vnd.google-apps.folder",
    }
    pasta = service.files().create(body=metadata, fields="id").execute()
    pasta_id = pasta.get("id")
    print(f"📁 Pasta criada: '{nome_pasta}' (ID: {pasta_id})")
    return pasta_id


def upload_para_drive(service, caminho_local, nome_ficheiro, pasta_id):
    metadata = {"name": nome_ficheiro, "parents": [pasta_id]}
    media = MediaFileUpload(caminho_local, mimetype="image/jpeg", resumable=True)
    ficheiro = service.files().create(
        body=metadata, media_body=media, fields="id, webViewLink"
    ).execute()
    return ficheiro.get("webViewLink", "")


# ─── FUNÇÕES DE DOWNLOAD ─────────────────────────────────────────────────────

def descarregar_imagem(url, caminho_destino):
    try:
        resposta = requests.get(url, headers=HEADERS, timeout=30)
        resposta.raise_for_status()
        with open(caminho_destino, "wb") as f:
            f.write(resposta.content)
        return True
    except Exception as e:
        print(f"   ⚠️  Erro: {e}")
        return False


def extrair_url_imagem(post):
    if post.get("images") and len(post["images"]) > 0:
        return post["images"][0]
    if post.get("displayUrl"):
        return post["displayUrl"]
    return None


# ─── PROGRAMA PRINCIPAL ───────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("📸 Download de Cartazes CAE → Google Drive")
    print("=" * 60)

    if "COLA_AQUI" in CLIENT_ID or "COLA_AQUI" in CLIENT_SECRET:
        print("\n❌ ERRO: Preenche CLIENT_ID e CLIENT_SECRET no script antes de correr!")
        return

    print(f"\n📂 A carregar: {JSON_FILE}")
    with open(JSON_FILE, "r", encoding="utf-8") as f:
        posts = json.load(f)
    print(f"   {len(posts)} posts encontrados")

    Path(LOCAL_TEMP_FOLDER).mkdir(exist_ok=True)

    print("\n🔐 A autenticar Google Drive...")
    service = autenticar_google_drive()
    print("   Autenticação OK")

    print(f"\n📁 A preparar pasta: '{DRIVE_FOLDER_NAME}'")
    pasta_id = criar_ou_obter_pasta(service, DRIVE_FOLDER_NAME)

    resultados = {}

    print(f"\n🚀 A processar {len(posts)} posts...\n")

    for i, post in enumerate(posts):
        short_code = post.get("shortCode", f"post_{i}")
        tipo = post.get("type", "Image")
        likes = post.get("likesCount", 0)

        url_imagem = extrair_url_imagem(post)

        if not url_imagem:
            print(f"[{i+1}/{len(posts)}] ⏭️  {short_code} — sem imagem")
            continue

        print(f"[{i+1}/{len(posts)}] 📥 {short_code} ({tipo}, {likes} likes)")

        nome_ficheiro = f"{short_code}.jpg"
        caminho_local = os.path.join(LOCAL_TEMP_FOLDER, nome_ficheiro)

        sucesso = descarregar_imagem(url_imagem, caminho_local)

        if not sucesso:
            print(f"   ❌ Download falhou")
            resultados[short_code] = {"status": "erro", "drive_url": ""}
            continue

        try:
            drive_url = upload_para_drive(service, caminho_local, nome_ficheiro, pasta_id)
            print(f"   ✅ {drive_url}")
            resultados[short_code] = {"status": "ok", "drive_url": drive_url, "likes": likes}
        except Exception as e:
            print(f"   ❌ Upload falhou: {e}")
            resultados[short_code] = {"status": "erro_upload", "drive_url": ""}

        time.sleep(1)

    with open("resultados_drive.json", "w", encoding="utf-8") as f:
        json.dump(resultados, f, ensure_ascii=False, indent=2)

    total_ok = sum(1 for v in resultados.values() if v["status"] == "ok")
    total_erro = len(resultados) - total_ok

    print("\n" + "=" * 60)
    print("📊 RESUMO")
    print(f"   ✅ Sucesso:  {total_ok} imagens")
    print(f"   ❌ Erros:    {total_erro} imagens")
    print(f"   📄 Resultados: resultados_drive.json")
    print("=" * 60)


if __name__ == "__main__":
    main()
