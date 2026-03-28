"""
Script: eventos_figfoz.py
Descrição: Vai buscar posts ao Apify, usa Claude para extrair dados dos eventos,
           guarda no Supabase e elimina eventos passados.

Requisitos:
    pip install requests supabase

Como usar:
    1. Preenche as configurações abaixo
    2. Corre: python eventos_figfoz.py
    3. Agenda no Agendador de Tarefas do Windows para correr semanalmente
"""

import json
import re
import time
import requests
from datetime import datetime, date

# ─── CONFIGURAÇÃO ────────────────────────────────────────────────────────────

try:
    from segredos import APIFY_TOKEN, APIFY_TASK_ID, ANTHROPIC_KEY, SUPABASE_URL, SUPABASE_KEY
except ImportError:
    APIFY_TOKEN   = "COLA_AQUI"
    APIFY_TASK_ID = "COLA_AQUI"
    ANTHROPIC_KEY = "COLA_AQUI"
    SUPABASE_URL  = "COLA_AQUI"
    SUPABASE_KEY  = "COLA_AQUI"

# ─── APIFY ───────────────────────────────────────────────────────────────────

def obter_posts_apify():
    """Obtém posts do último run do Apify."""
    print("📥 A obter posts do Apify...")
    url = f"https://api.apify.com/v2/actor-tasks/{APIFY_TASK_ID}/runs/last/dataset/items?token={APIFY_TOKEN}"
    resposta = requests.get(url, timeout=60)
    resposta.raise_for_status()
    posts = resposta.json()
    print(f"   {len(posts)} posts obtidos")
    return posts


# ─── CLAUDE API ──────────────────────────────────────────────────────────────

def extrair_dados_evento(caption, short_code):
    """Usa Claude para extrair dados estruturados da legenda do Instagram."""
    if not caption or len(caption.strip()) < 20:
        return None

    prompt = f"""Analisa esta legenda de um post do Instagram do CAE (Centro de Artes e Espectáculos) da Figueira da Foz e extrai os dados do evento.

Legenda:
{caption}

Devolve APENAS um JSON válido com estes campos (sem texto extra, sem markdown):
{{
  "titulo": "título do evento ou null",
  "descricao": "descrição curta do evento (máx 200 caracteres) ou null",
  "data_evento": "data(s) do evento em formato legível (ex: '12 de junho, 21h30') ou null",
  "local_evento": "local do evento ou null",
  "preco": "preço(s) do bilhete ou 'Entrada livre' ou null",
  "e_evento": true ou false
}}

Regras:
- e_evento = true apenas se for um espectáculo, exposição, concerto, cinema ou workshop com data específica
- e_evento = false para posts de reportagem, agradecimentos ou sem data
- data_evento deve incluir o ano se mencionado
- Se não houver informação para um campo, usa null"""

    try:
        resposta = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json"
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 500,
                "messages": [{"role": "user", "content": prompt}]
            },
            timeout=30
        )
        resposta.raise_for_status()
        texto = resposta.json()["content"][0]["text"].strip()

        # Limpa possível markdown
        texto = re.sub(r"```json|```", "", texto).strip()

        dados = json.loads(texto)
        return dados
    except Exception as e:
        print(f"   ⚠️  Erro Claude: {e}")
        return None


def extrair_data_para_ordenar(data_texto):
    """Tenta extrair uma data para ordenação a partir do texto."""
    if not data_texto:
        return None

    meses = {
        "janeiro": 1, "fevereiro": 2, "março": 3, "abril": 4,
        "maio": 5, "junho": 6, "julho": 7, "agosto": 8,
        "setembro": 9, "outubro": 10, "novembro": 11, "dezembro": 12,
        "jan": 1, "fev": 2, "mar": 3, "abr": 4, "mai": 5, "jun": 6,
        "jul": 7, "ago": 8, "set": 9, "out": 10, "nov": 11, "dez": 12
    }

    texto = data_texto.lower()
    ano = 2026

    for mes_nome, mes_num in meses.items():
        if mes_nome in texto:
            numeros = re.findall(r'\d+', texto)
            dia = None
            for n in numeros:
                if 1 <= int(n) <= 31:
                    dia = int(n)
                    break
            if dia:
                try:
                    return date(ano, mes_num, dia)
                except:
                    pass
    return None


# ─── SUPABASE ────────────────────────────────────────────────────────────────

def supabase_request(method, endpoint, data=None):
    """Faz um pedido à API REST do Supabase."""
    url = f"{SUPABASE_URL}/rest/v1/{endpoint}"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }

    if method == "GET":
        return requests.get(url, headers=headers, timeout=30)
    elif method == "POST":
        headers["Prefer"] = "return=representation"
        return requests.post(url, headers=headers, json=data, timeout=30)
    elif method == "DELETE":
        return requests.delete(url, headers=headers, timeout=30)


def obter_short_codes_existentes():
    """Obtém os short_codes já guardados no Supabase."""
    resposta = supabase_request("GET", "eventos?select=short_code")
    if resposta.status_code == 200:
        return {item["short_code"] for item in resposta.json()}
    return set()


def inserir_evento(evento):
    """Insere um evento no Supabase."""
    resposta = supabase_request("POST", "eventos", evento)
    return resposta.status_code in [200, 201]


def eliminar_eventos_passados():
    """Elimina eventos cuja data já passou."""
    hoje = date.today().isoformat()
    endpoint = f"eventos?data_post=lt.{hoje}&data_post=not.is.null"
    resposta = supabase_request("DELETE", endpoint)
    if resposta.status_code in [200, 204]:
        print("   🗑️  Eventos passados eliminados")
    else:
        print(f"   ⚠️  Erro ao eliminar: {resposta.status_code}")


def extrair_imagem_url(post):
    """Extrai URL da imagem principal do post."""
    if post.get("images") and len(post["images"]) > 0:
        return post["images"][0]
    if post.get("displayUrl"):
        return post["displayUrl"]
    return None


# ─── PROGRAMA PRINCIPAL ───────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("🎭 CAE Eventos → Supabase")
    print(f"   Data: {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print("=" * 60)

    # Verifica configuração
    for nome, val in [("APIFY_TOKEN", APIFY_TOKEN), ("ANTHROPIC_KEY", ANTHROPIC_KEY),
                      ("SUPABASE_URL", SUPABASE_URL), ("SUPABASE_KEY", SUPABASE_KEY)]:
        if "COLA_AQUI" in val:
            print(f"\n❌ ERRO: Preenche {nome} no script!")
            return

    # 1. Obtém posts do Apify
    posts = obter_posts_apify()
    if not posts:
        print("❌ Sem posts.")
        return

    # 2. Obtém eventos já existentes no Supabase
    print("\n🔍 A verificar eventos existentes...")
    existentes = obter_short_codes_existentes()
    print(f"   {len(existentes)} eventos já na base de dados")

    # 3. Processa cada post
    print(f"\n🤖 A processar posts com Claude...\n")
    novos = 0
    ignorados = 0

    for i, post in enumerate(posts):
        short_code = post.get("shortCode", f"post_{i}")
        caption = post.get("caption", "")

        # Salta se já existe
        if short_code in existentes:
            print(f"[{i+1}/{len(posts)}] ⏭️  {short_code} — já existe")
            continue

        print(f"[{i+1}/{len(posts)}] 🔍 {short_code}")

        # Extrai dados com Claude
        dados = extrair_dados_evento(caption, short_code)

        if not dados or not dados.get("e_evento"):
            print(f"   ⏭️  Não é um evento, a saltar")
            ignorados += 1
            continue

        # Prepara data para ordenação
        data_post = extrair_data_para_ordenar(dados.get("data_evento"))

        # Prepara evento para inserir
        evento = {
            "short_code": short_code,
            "titulo": dados.get("titulo"),
            "descricao": dados.get("descricao"),
            "data_evento": dados.get("data_evento"),
            "local_evento": dados.get("local_evento") or "CAE — Figueira da Foz",
            "preco": dados.get("preco"),
            "imagem_url": extrair_imagem_url(post),
            "instagram_url": post.get("url"),
            "likes": post.get("likesCount", 0),
            "tipo": post.get("type", "Image"),
            "data_post": data_post.isoformat() if data_post else None
        }

        print(f"   📌 {evento['titulo'] or 'Sem título'}")
        print(f"   📅 {evento['data_evento'] or 'Sem data'}")
        print(f"   💶 {evento['preco'] or 'Sem preço'}")

        # Insere no Supabase
        if inserir_evento(evento):
            print(f"   ✅ Inserido")
            novos += 1
        else:
            print(f"   ❌ Erro ao inserir")

        # Pausa para não sobrecarregar a API do Claude
        time.sleep(0.5)

    # 4. Elimina eventos passados
    print("\n🗑️  A eliminar eventos passados...")
    eliminar_eventos_passados()

    # Resumo
    print("\n" + "=" * 60)
    print("📊 RESUMO")
    print(f"   🆕 Novos eventos: {novos}")
    print(f"   ⏭️  Ignorados:     {ignorados}")
    print(f"   ✅ Concluído!")
    print("=" * 60)


if __name__ == "__main__":
    main()