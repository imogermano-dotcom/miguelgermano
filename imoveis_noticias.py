"""
Script: imoveis_noticias.py
Descrição: Vai buscar posts ao Apify da task de imobiliário nacional,
           filtra por palavras-chave de imobiliário e habitação,
           usa Claude para extrair título e resumo,
           e guarda na tabela 'noticias_imoveis' no Supabase.

Perfis monitorizados:
    - jornaldenegocios, dinheirovivo, publico, expresso
    - imovirtual, idealista.pt, supercasa.pt, ecoeconomiaonline

Requisitos:
    pip install requests

Como usar:
    1. Preenche as configurações abaixo
    2. Corre: python imoveis_noticias.py
"""

import json
import re
import time
import statistics
import requests
from datetime import datetime, date

# ─── CONFIGURAÇÃO ────────────────────────────────────────────────────────────

from segredos import APIFY_TOKEN, ANTHROPIC_KEY, SUPABASE_URL, SUPABASE_KEY, RESEND_KEY

APIFY_TASK_ID = "6aPcxLE72D0yPyDJ3"
EMAIL_PARA    = "miguel.germano@gmail.com"
EMAIL_DE      = "noticias@miguelgermano.com"

# Nomes legíveis das fontes
NOMES_FONTES = {
    "jornaldenegocios": "Jornal de Negócios",
    "dinheirovivo": "Dinheiro Vivo",
    "publico": "Público",
    "expresso": "Expresso",
    "imovirtual": "Imovirtual",
    "idealista.pt": "Idealista",
    "supercasa.pt": "SuperCasa",
    "ecoeconomiaonline": "ECO Economia"
}

# Perfis especializados em imobiliário — não precisam de filtro por palavras-chave
PERFIS_IMOVEIS_DIRETOS = {"imovirtual", "idealista.pt", "supercasa.pt"}

# Palavras-chave para filtrar posts de jornais generalistas
PALAVRAS_IMOVEIS = [
    "imobiliário", "habitação", "casa", "apartamento", "renda", "arrendamento",
    "comprar casa", "vender casa", "mercado imobiliário", "preço casa",
    "euribor", "crédito habitação", "hipoteca", "prestação", "juro",
    "construção", "obra", "promoção imobiliária", "promotor",
    "alojamento local", "al ", "rendas", "inquilino", "senhorio",
    "nova lei", "lei habitação", "mais habitação", "programa habitação",
    "t1", "t2", "t3", "t4", "m2", "metros quadrados",
    "compra e venda", "escritura", "contrato promessa",
    "idealista", "imovirtual", "supercasa", "remax", "era imobiliária",
    "century 21", "kw ", "keller williams"
]

# ─── APIFY ───────────────────────────────────────────────────────────────────

def obter_posts_apify():
    print("📥 A obter posts do Apify (imobiliário)...")
    url = f"https://api.apify.com/v2/actor-tasks/{APIFY_TASK_ID}/runs/last/dataset/items?token={APIFY_TOKEN}"
    resposta = requests.get(url, timeout=60)
    resposta.raise_for_status()
    posts = resposta.json()
    # Filtra apenas posts válidos (não erros)
    posts_validos = [p for p in posts if p.get("shortCode") and not p.get("error")]
    print(f"   {len(posts_validos)} posts válidos obtidos (de {len(posts)} total)")
    return posts_validos


# ─── FILTRO IMOBILIÁRIO ───────────────────────────────────────────────────────

def e_sobre_imoveis(caption, username):
    """Verifica se o post é sobre imobiliário/habitação."""
    # Perfis especializados — tudo passa
    if username in PERFIS_IMOVEIS_DIRETOS:
        return True
    # Jornais generalistas — filtra por palavras-chave
    if not caption:
        return False
    texto = caption.lower()
    return any(palavra in texto for palavra in PALAVRAS_IMOVEIS)


# ─── THRESHOLD DE ENGAGEMENT ─────────────────────────────────────────────────

def calcular_thresholds(posts):
    """Calcula threshold de engagement (mediana × 0.5) por perfil."""
    likes_por_perfil = {}
    for post in posts:
        username = post.get("ownerUsername", "")
        likes = post.get("likesCount") or 0
        if likes < 0:
            likes = 0
        if username not in likes_por_perfil:
            likes_por_perfil[username] = []
        likes_por_perfil[username].append(likes)

    thresholds = {}
    print("\n📊 Thresholds calculados:")
    for username, likes_list in likes_por_perfil.items():
        if likes_list:
            mediana = statistics.median(likes_list)
            threshold = max(1, int(mediana * 0.5))
            thresholds[username] = threshold
            fonte = NOMES_FONTES.get(username, username)
            print(f"   {fonte}: mediana={int(mediana)} → threshold={threshold} likes")
        else:
            thresholds[username] = 1
    return thresholds


# ─── CLAUDE API ──────────────────────────────────────────────────────────────

def extrair_dados_noticia_imoveis(caption, fonte):
    """Usa Claude para extrair título e resumo de notícia imobiliária."""
    if not caption or len(caption.strip()) < 20:
        return None

    prompt = f"""Analisa este post do Instagram de "{fonte}" sobre imobiliário/habitação em Portugal.

Texto:
{caption[:1500]}

Devolve APENAS um JSON válido sem texto extra:
{{
  "titulo": "título claro e informativo da notícia (máx 100 caracteres) ou null",
  "resumo": "resumo conciso em 2-3 frases (máx 300 caracteres) ou null",
  "e_relevante": true ou false
}}

e_relevante = true se for sobre mercado imobiliário, habitação, rendas, crédito habitação, euribor, construção, ou legislação de habitação em Portugal.
e_relevante = false se for publicidade, conteúdo pessoal, ou não relacionado com imobiliário português."""

    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json"
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 400,
                "messages": [{"role": "user", "content": prompt}]
            },
            timeout=30
        )
        r.raise_for_status()
        texto = re.sub(r"```json|```", "", r.json()["content"][0]["text"]).strip()
        return json.loads(texto)
    except Exception as e:
        print(f"   ⚠️  Erro Claude: {e}")
        return None


# ─── SUPABASE ────────────────────────────────────────────────────────────────

def supabase_get(endpoint):
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    return requests.get(f"{SUPABASE_URL}/rest/v1/{endpoint}", headers=headers, timeout=30)


def supabase_post(tabela, data):
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=ignore-duplicates,return=minimal"
    }
    return requests.post(f"{SUPABASE_URL}/rest/v1/{tabela}", headers=headers, json=data, timeout=30)


def supabase_delete(endpoint):
    headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    return requests.delete(f"{SUPABASE_URL}/rest/v1/{endpoint}", headers=headers, timeout=30)


def obter_existentes():
    resposta = supabase_get("noticias_imoveis?select=short_code")
    if resposta.status_code == 200:
        return {item["short_code"] for item in resposta.json()}
    return set()


def obter_titulos_existentes():
    resposta = supabase_get("noticias_imoveis?select=titulo")
    if resposta.status_code == 200:
        return {item["titulo"].lower().strip() for item in resposta.json() if item.get("titulo")}
    return set()


def titulo_similar(titulo, titulos_existentes, threshold=0.7):
    if not titulo:
        return False
    titulo_lower = titulo.lower().strip()
    if titulo_lower in titulos_existentes:
        return True
    palavras = [p for p in titulo_lower.split() if len(p) > 3][:5]
    if not palavras:
        return False
    for titulo_existente in titulos_existentes:
        palavras_existente = [p for p in titulo_existente.split() if len(p) > 3][:5]
        if not palavras_existente:
            continue
        comuns = len(set(palavras) & set(palavras_existente))
        total = max(len(palavras), len(palavras_existente))
        if total > 0 and comuns / total >= threshold:
            return True
    return False


# ─── UTILITÁRIOS ─────────────────────────────────────────────────────────────

def extrair_url_imagem(post):
    if post.get("images") and len(post["images"]) > 0:
        return post["images"][0]
    if post.get("displayUrl"):
        return post["displayUrl"]
    return None


def calcular_engagement(post):
    likes = max(0, post.get("likesCount") or 0)
    comentarios = post.get("commentsCount") or 0
    visualizacoes = post.get("videoViewCount") or 0
    return likes + comentarios + (visualizacoes // 10)


# ─── PROGRAMA PRINCIPAL ───────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("🏠 Notícias Imobiliário → Supabase")
    print(f"   Data: {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print("=" * 60)

    posts = obter_posts_apify()
    if not posts:
        print("❌ Sem posts.")
        return

    # Thresholds automáticos
    thresholds = calcular_thresholds(posts)

    # Existentes no Supabase
    print("\n🔍 A verificar existentes...")
    existentes = obter_existentes()
    titulos = obter_titulos_existentes()
    print(f"   {len(existentes)} notícias já na base de dados")

    novas = 0
    filtrados_imoveis = 0
    filtrados_engagement = 0
    ignorados = 0
    lista_novas = []

    print(f"\n🤖 A processar {len(posts)} posts...\n")

    for i, post in enumerate(posts):
        short_code = post.get("shortCode", f"post_{i}")
        username = post.get("ownerUsername", "")
        caption = post.get("caption", "")
        likes = max(0, post.get("likesCount") or 0)
        comentarios = post.get("commentsCount") or 0
        visualizacoes = post.get("videoViewCount") or 0
        engagement = calcular_engagement(post)
        imagem_url = extrair_url_imagem(post)
        instagram_url = post.get("url", "")
        timestamp = post.get("timestamp")
        data_post = datetime.fromisoformat(timestamp.replace("Z", "")).date() if timestamp else None

        # Já existe?
        if short_code in existentes:
            print(f"[{i+1}/{len(posts)}] ⏭️  {short_code} — já existe")
            continue

        fonte = NOMES_FONTES.get(username, username)

        # Filtro imobiliário
        if not e_sobre_imoveis(caption, username):
            print(f"[{i+1}/{len(posts)}] 🏢 {short_code} ({fonte}) — não é imobiliário")
            filtrados_imoveis += 1
            continue

        # Filtro engagement
        threshold = thresholds.get(username, 1)
        if likes < threshold:
            print(f"[{i+1}/{len(posts)}] 📉 {short_code} ({fonte}) — engagement baixo ({likes} < {threshold})")
            filtrados_engagement += 1
            continue

        print(f"[{i+1}/{len(posts)}] 🏠 {short_code} — {fonte} ({likes} likes)")

        # Extrai dados com Claude
        dados = extrair_dados_noticia_imoveis(caption, fonte)
        if not dados or not dados.get("e_relevante"):
            print(f"   ⏭️  Não é relevante para imobiliário")
            ignorados += 1
            continue

        titulo = dados.get("titulo")

        # Verifica título similar
        if titulo_similar(titulo, titulos):
            print(f"   ⏭️  Título similar já existe")
            ignorados += 1
            continue

        noticia = {
            "short_code": short_code,
            "titulo": titulo,
            "resumo": dados.get("resumo"),
            "imagem_url": imagem_url,
            "instagram_url": instagram_url,
            "fonte": fonte,
            "likes": likes,
            "comentarios": comentarios,
            "visualizacoes": visualizacoes,
            "engagement": engagement,
            "data_post": data_post.isoformat() if data_post else None,
            "timestamp_post": timestamp
        }

        print(f"   📌 {titulo or 'Sem título'}")
        print(f"   ⚡ {engagement} engagement ({likes} likes, {comentarios} comentários)")

        r = supabase_post("noticias_imoveis", noticia)
        if r.status_code in [200, 201]:
            print(f"   ✅ Inserida")
            novas += 1
            lista_novas.append(noticia)
            if titulo:
                titulos.add(titulo.lower().strip())
        else:
            print(f"   ❌ Erro: {r.status_code} — {r.text[:100]}")

        time.sleep(0.5)

    print("\n" + "=" * 60)
    print("📊 RESUMO")
    print(f"   🏠 Novas notícias imobiliário: {novas}")
    print(f"   🏢 Filtradas (não imobiliário): {filtrados_imoveis}")
    print(f"   📉 Filtradas (engagement):      {filtrados_engagement}")
    print(f"   ⏭️  Ignoradas (outras):          {ignorados}")
    print(f"   ✅ Concluído!")
    print("=" * 60)

    # Limpeza de notícias antigas
    limpar_noticias_antigas()

    if novas > 0:
        enviar_email_resumo(novas, lista_novas)


def limpar_noticias_antigas():
    """Apaga notícias imobiliárias com mais de 30 dias."""
    from datetime import timedelta
    limite = (date.today() - timedelta(days=30)).isoformat()
    r = supabase_delete(f"noticias_imoveis?data_post=lt.{limite}&data_post=not.is.null")
    if r.status_code in [200, 204]:
        print("\n🗑️  Notícias imobiliárias antigas eliminadas (>30 dias)")


def enviar_email_resumo(novas, noticias):
    """Envia resumo de notícias imobiliárias por email via Resend."""
    data = datetime.now().strftime("%d/%m/%Y")

    itens = "".join([f"""
    <li style='margin-bottom:18px;border-left:2px solid #c9a96e;padding-left:12px'>
        <strong style='font-size:15px'>{n.get('titulo','Sem título')}</strong><br>
        <span style='color:#aaa;font-size:12px'>{n.get('fonte','') or ''} &nbsp;·&nbsp; ❤️ {n.get('likes',0)} likes</span><br>
        {f'<span style="color:#ccc;font-size:13px;line-height:1.6">{n.get("resumo","")}</span><br>' if n.get('resumo') else ''}
        {f'<a href="{n.get("instagram_url","")}" style="color:#c9a96e;font-size:12px;text-decoration:none">Ver fonte →</a>' if n.get('instagram_url') else ''}
    </li>""" for n in noticias])

    html = f"""
    <div style='font-family:sans-serif;max-width:620px;margin:0 auto;background:#0a0a0b;color:#f0ece4;padding:32px;border-radius:8px'>
        <h2 style='color:#c9a96e;font-size:24px;margin-bottom:4px;font-weight:300'>Mercado Imobiliário</h2>
        <p style='color:#6b6860;font-size:13px;margin-bottom:8px'>Resumo diário · {data}</p>
        <div style='margin-bottom:28px'>
            <span style='background:#1a1a1e;padding:4px 12px;border-radius:4px;font-size:12px;color:#c9a96e'>🏠 {novas} notícias</span>
        </div>
        <ul style='padding-left:0;list-style:none'>{itens}</ul>
        <hr style='border-color:#222;margin:28px 0'>
        <a href='https://miguelgermano.com#imoveis-noticias' style='color:#c9a96e;font-size:13px;text-decoration:none'>Ver no site →</a>
    </div>
    """

    try:
        r = requests.post(
            "https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {RESEND_KEY}", "Content-Type": "application/json"},
            json={
                "from": EMAIL_DE,
                "to": EMAIL_PARA,
                "subject": f"🏠 Imobiliário — {novas} novas notícias ({data})",
                "html": html
            },
            timeout=30
        )
        if r.status_code in [200, 201]:
            print(f"\n📧 Email enviado para {EMAIL_PARA}")
        else:
            print(f"\n⚠️  Erro ao enviar email: {r.status_code} — {r.text[:100]}")
    except Exception as e:
        print(f"\n⚠️  Erro ao enviar email: {e}")


if __name__ == "__main__":
    main()
