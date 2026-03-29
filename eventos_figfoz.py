"""
Script: eventos_figfoz.py
Descrição: Vai buscar posts ao Apify de 6 perfis Instagram,
           classifica em eventos (CAE) e notícias (restantes).
           
           Para notícias:
           - diariocoimbra e diarioasbeiras: filtra só posts sobre a Figueira da Foz
           - Todos os perfis: threshold de engagement automático (mediana por fonte)
           - Verificação de títulos similares para evitar duplicados

Requisitos:
    pip install requests

Como usar:
    1. Preenche as configurações abaixo
    2. Corre: python eventos_figfoz.py
"""

import json
import re
import time
import statistics
import requests
from datetime import datetime, date

# ─── CONFIGURAÇÃO ────────────────────────────────────────────────────────────

from segredos import APIFY_TOKEN, ANTHROPIC_KEY, SUPABASE_URL, SUPABASE_KEY, RESEND_KEY

APIFY_TASK_ID = "GYK8gkp0lebfRP4tt"
EMAIL_PARA    = "miguel.germano@gmail.com"
EMAIL_DE      = "noticias@miguelgermano.com"

# Perfis que geram EVENTOS
PERFIS_EVENTOS = {"caefigueiradafoz"}

# Perfis que geram NOTÍCIAS
PERFIS_NOTICIAS = {"diariocoimbra", "diarioasbeiras", "figueira.dafoz", "figueiranahora", "casinofigueira"}

# Perfis regionais que precisam de filtro geográfico (só notícias da Figueira)
PERFIS_REGIONAIS = {"diariocoimbra", "diarioasbeiras"}

# Nomes legíveis das fontes
NOMES_FONTES = {
    "caefigueiradafoz": "CAE Figueira da Foz",
    "diariocoimbra": "Diário de Coimbra",
    "diarioasbeiras": "Diário As Beiras",
    "figueira.dafoz": "Figueira da Foz",
    "figueiranahora": "Figueira na Hora",
    "casinofigueira": "Casino da Figueira"
}

# Palavras-chave para filtrar notícias sobre a Figueira da Foz
PALAVRAS_FIGUEIRA = [
    "figueira da foz", "figueira", "lavos", "buarcos",
    "tavarede", "alhadas", "paião", "santana", "quiaios",
    "moinhos da gândara", "maiorca", "brenha", "ferreira-a-nova"
]

# ─── FILTRO GEOGRÁFICO ────────────────────────────────────────────────────────

def e_sobre_figueira(caption):
    """Verifica se o post menciona a Figueira da Foz."""
    if not caption:
        return False
    texto = caption.lower()
    return any(palavra in texto for palavra in PALAVRAS_FIGUEIRA)


# ─── THRESHOLD DE ENGAGEMENT ─────────────────────────────────────────────────

def calcular_thresholds(posts):
    """Calcula o threshold de engagement (mediana de likes) por perfil."""
    likes_por_perfil = {}
    
    for post in posts:
        username = post.get("ownerUsername", "")
        if username not in PERFIS_NOTICIAS:
            continue
        likes = post.get("likesCount") or 0
        if username not in likes_por_perfil:
            likes_por_perfil[username] = []
        likes_por_perfil[username].append(likes)
    
    thresholds = {}
    print("\n📊 Thresholds de engagement calculados:")
    for username, likes_list in likes_por_perfil.items():
        if likes_list:
            mediana = statistics.median(likes_list)
            # Usa metade da mediana como threshold (não muito restritivo)
            threshold = max(1, int(mediana * 0.5))
            thresholds[username] = threshold
            fonte = NOMES_FONTES.get(username, username)
            print(f"   {fonte}: mediana={int(mediana)} likes → threshold={threshold} likes")
        else:
            thresholds[username] = 1
    
    return thresholds


# ─── APIFY ───────────────────────────────────────────────────────────────────

def obter_posts_apify():
    print("📥 A obter posts do Apify...")
    url = f"https://api.apify.com/v2/actor-tasks/{APIFY_TASK_ID}/runs/last/dataset/items?token={APIFY_TOKEN}"
    resposta = requests.get(url, timeout=60)
    resposta.raise_for_status()
    posts = resposta.json()
    print(f"   {len(posts)} posts obtidos")
    return posts


# ─── CLAUDE API ──────────────────────────────────────────────────────────────

def extrair_dados_evento(caption):
    if not caption or len(caption.strip()) < 20:
        return None
    prompt = f"""Analisa esta legenda do Instagram do CAE Figueira da Foz e extrai os dados do evento.

Legenda:
{caption}

Devolve APENAS um JSON válido sem texto extra:
{{
  "titulo": "título do evento ou null",
  "descricao": "descrição curta (máx 200 caracteres) ou null",
  "data_evento": "data(s) em formato legível (ex: '12 de junho, 21h30') ou null",
  "local_evento": "local ou null",
  "preco": "preço ou 'Entrada livre' ou null",
  "e_evento": true ou false
}}

e_evento = true apenas se for espectáculo, exposição, concerto, cinema ou workshop com data específica."""
    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
            json={"model": "claude-haiku-4-5-20251001", "max_tokens": 500, "messages": [{"role": "user", "content": prompt}]},
            timeout=30
        )
        r.raise_for_status()
        texto = re.sub(r"```json|```", "", r.json()["content"][0]["text"]).strip()
        return json.loads(texto)
    except Exception as e:
        print(f"   ⚠️  Erro Claude (evento): {e}")
        return None


def extrair_dados_noticia(caption, fonte):
    if not caption or len(caption.strip()) < 20:
        return None
    prompt = f"""Analisa esta legenda do Instagram de "{fonte}" e extrai os dados da notícia.

Legenda:
{caption}

Devolve APENAS um JSON válido sem texto extra:
{{
  "titulo": "título claro e conciso da notícia ou null",
  "resumo": "resumo em 2-3 linhas (máx 300 caracteres) ou null",
  "e_noticia": true ou false
}}

e_noticia = true se for notícia, acontecimento ou informação relevante.
e_noticia = false para publicidade, concursos ou conteúdo irrelevante."""
    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
            json={"model": "claude-haiku-4-5-20251001", "max_tokens": 400, "messages": [{"role": "user", "content": prompt}]},
            timeout=30
        )
        r.raise_for_status()
        texto = re.sub(r"```json|```", "", r.json()["content"][0]["text"]).strip()
        return json.loads(texto)
    except Exception as e:
        print(f"   ⚠️  Erro Claude (notícia): {e}")
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


def obter_existentes(tabela):
    resposta = supabase_get(f"{tabela}?select=short_code")
    if resposta.status_code == 200:
        return {item["short_code"] for item in resposta.json()}
    return set()


def obter_titulos_existentes(tabela):
    resposta = supabase_get(f"{tabela}?select=titulo")
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


def eliminar_eventos_passados():
    hoje = date.today().isoformat()
    r = supabase_delete(f"eventos?data_post=lt.{hoje}&data_post=not.is.null")
    if r.status_code in [200, 204]:
        print("   🗑️  Eventos passados eliminados")


# ─── UTILITÁRIOS ─────────────────────────────────────────────────────────────

def extrair_url_imagem(post):
    if post.get("images") and len(post["images"]) > 0:
        return post["images"][0]
    if post.get("displayUrl"):
        return post["displayUrl"]
    return None


def extrair_data(texto):
    if not texto:
        return None
    meses = {"janeiro":1,"fevereiro":2,"março":3,"abril":4,"maio":5,"junho":6,
             "julho":7,"agosto":8,"setembro":9,"outubro":10,"novembro":11,"dezembro":12,
             "jan":1,"fev":2,"mar":3,"abr":4,"mai":5,"jun":6,"jul":7,"ago":8,
             "set":9,"out":10,"nov":11,"dez":12}
    texto_lower = texto.lower()
    for mes_nome, mes_num in meses.items():
        if mes_nome in texto_lower:
            numeros = re.findall(r'\d+', texto_lower)
            for n in numeros:
                if 1 <= int(n) <= 31:
                    try:
                        return date(2026, mes_num, int(n))
                    except:
                        pass
    return None


def calcular_engagement(post):
    likes = post.get("likesCount") or 0
    comentarios = post.get("commentsCount") or 0
    visualizacoes = post.get("videoViewCount") or 0
    return likes + comentarios + (visualizacoes // 10)


# ─── PROGRAMA PRINCIPAL ───────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("🎭 Figueira da Foz — Eventos & Notícias")
    print(f"   Data: {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print("=" * 60)

    # Obtém posts
    posts = obter_posts_apify()
    if not posts:
        print("❌ Sem posts.")
        return

    # Calcula thresholds automáticos por perfil
    thresholds = calcular_thresholds(posts)

    # Obtém existentes
    print("\n🔍 A verificar existentes...")
    eventos_existentes = obter_existentes("eventos")
    noticias_existentes = obter_existentes("noticias")
    titulos_eventos = obter_titulos_existentes("eventos")
    titulos_noticias = obter_titulos_existentes("noticias")
    print(f"   Eventos: {len(eventos_existentes)} | Notícias: {len(noticias_existentes)}")

    novos_eventos = 0
    novas_noticias = 0
    ignorados = 0
    filtrados_geo = 0
    filtrados_engagement = 0
    lista_novos_eventos = []
    lista_novas_noticias = []

    print(f"\n🤖 A processar {len(posts)} posts...\n")

    for i, post in enumerate(posts):
        short_code = post.get("shortCode", f"post_{i}")
        username = post.get("ownerUsername", "")
        caption = post.get("caption", "")
        likes = post.get("likesCount") or 0
        comentarios = post.get("commentsCount") or 0
        visualizacoes = post.get("videoViewCount") or 0
        engagement = calcular_engagement(post)
        imagem_url = extrair_url_imagem(post)
        instagram_url = post.get("url", "")
        timestamp = post.get("timestamp")
        data_post = datetime.fromisoformat(timestamp.replace("Z", "")).date() if timestamp else None

        # ── EVENTOS (CAE) ──
        if username in PERFIS_EVENTOS:
            if short_code in eventos_existentes:
                print(f"[{i+1}/{len(posts)}] ⏭️  {short_code} — já existe")
                continue

            print(f"[{i+1}/{len(posts)}] 🎭 {short_code} — CAE")

            dados = extrair_dados_evento(caption)
            if not dados or not dados.get("e_evento"):
                print(f"   ⏭️  Não é evento")
                ignorados += 1
                continue

            titulo = dados.get("titulo")
            if titulo_similar(titulo, titulos_eventos):
                print(f"   ⏭️  Título similar já existe")
                ignorados += 1
                continue

            data_evento_obj = extrair_data(dados.get("data_evento"))
            evento = {
                "short_code": short_code,
                "titulo": titulo,
                "descricao": dados.get("descricao"),
                "data_evento": dados.get("data_evento"),
                "local_evento": dados.get("local_evento") or "CAE — Figueira da Foz",
                "preco": dados.get("preco"),
                "imagem_url": imagem_url,
                "instagram_url": instagram_url,
                "likes": likes,
                "tipo": post.get("type", "Image"),
                "data_post": data_evento_obj.isoformat() if data_evento_obj else None
            }

            print(f"   📌 {titulo or 'Sem título'}")
            r = supabase_post("eventos", evento)
            if r.status_code in [200, 201]:
                print(f"   ✅ Inserido")
                novos_eventos += 1
                lista_novos_eventos.append(evento)
                if titulo:
                    titulos_eventos.add(titulo.lower().strip())
            else:
                print(f"   ❌ Erro: {r.status_code}")

        # ── NOTÍCIAS ──
        elif username in PERFIS_NOTICIAS:
            if short_code in noticias_existentes:
                print(f"[{i+1}/{len(posts)}] ⏭️  {short_code} — já existe")
                continue

            fonte = NOMES_FONTES.get(username, username)

            # Filtro geográfico para jornais regionais
            if username in PERFIS_REGIONAIS and not e_sobre_figueira(caption):
                print(f"[{i+1}/{len(posts)}] 🗺️  {short_code} ({fonte}) — não é sobre a Figueira")
                filtrados_geo += 1
                continue

            # Filtro de engagement automático
            threshold = thresholds.get(username, 1)
            if likes < threshold:
                print(f"[{i+1}/{len(posts)}] 📉 {short_code} ({fonte}) — engagement baixo ({likes} < {threshold} likes)")
                filtrados_engagement += 1
                continue

            print(f"[{i+1}/{len(posts)}] 📰 {short_code} — {fonte} ({likes} likes)")

            dados = extrair_dados_noticia(caption, fonte)
            if not dados or not dados.get("e_noticia"):
                print(f"   ⏭️  Não é notícia relevante")
                ignorados += 1
                continue

            titulo = dados.get("titulo")
            if titulo_similar(titulo, titulos_noticias):
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

            r = supabase_post("noticias", noticia)
            if r.status_code in [200, 201]:
                print(f"   ✅ Inserida")
                novas_noticias += 1
                lista_novas_noticias.append(noticia)
                if titulo:
                    titulos_noticias.add(titulo.lower().strip())
            else:
                print(f"   ❌ Erro: {r.status_code}")

        else:
            print(f"[{i+1}/{len(posts)}] ⚠️  {short_code} — perfil desconhecido: {username}")

        time.sleep(0.5)

    # Elimina eventos passados
    print("\n🗑️  A eliminar eventos passados...")
    eliminar_eventos_passados()

    print("\n" + "=" * 60)
    print("📊 RESUMO")
    print(f"   🎭 Novos eventos:        {novos_eventos}")
    print(f"   📰 Novas notícias:       {novas_noticias}")
    print(f"   🗺️  Filtradas (geo):      {filtrados_geo}")
    print(f"   📉 Filtradas (engagement): {filtrados_engagement}")
    print(f"   ⏭️  Ignoradas (outras):   {ignorados}")
    print(f"   ✅ Concluído!")
    print("=" * 60)

    # Limpeza de notícias antigas
    limpar_noticias_antigas()

    # Envia email de resumo
    if novos_eventos > 0 or novas_noticias > 0:
        enviar_email_resumo(novos_eventos, novas_noticias, lista_novos_eventos, lista_novas_noticias)


def limpar_noticias_antigas():
    """Apaga notícias com mais de 30 dias."""
    from datetime import timedelta
    limite = (date.today() - timedelta(days=30)).isoformat()
    r = supabase_delete(f"noticias?data_post=lt.{limite}&data_post=not.is.null")
    if r.status_code in [200, 204]:
        print("\n🗑️  Notícias antigas eliminadas (>30 dias)")


def enviar_email_resumo(novos_eventos, novas_noticias, eventos, noticias):
    """Envia resumo diário por email via Resend."""
    data = datetime.now().strftime("%d/%m/%Y")

    # Constrói HTML dos eventos
    html_eventos = ""
    if eventos:
        itens = "".join([f"""
        <li style='margin-bottom:16px;border-left:2px solid #c9a96e;padding-left:12px'>
            <strong style='font-size:15px'>{e.get('titulo','Sem título')}</strong><br>
            <span style='color:#aaa;font-size:13px'>
                📅 {e.get('data_evento','') or 'Data a confirmar'} &nbsp;·&nbsp;
                📍 {e.get('local_evento','') or 'CAE'} &nbsp;·&nbsp;
                💶 {e.get('preco','') or 'Ver bilhetes'}
            </span><br>
            {f'<span style="color:#ccc;font-size:13px;line-height:1.5">{e.get("descricao","")}</span><br>' if e.get('descricao') else ''}
            {f'<a href="{e.get("instagram_url","")}" style="color:#c9a96e;font-size:12px;text-decoration:none">Ver no Instagram →</a>' if e.get('instagram_url') else ''}
        </li>""" for e in eventos])
        html_eventos = f"<h3 style='color:#c9a96e;margin:28px 0 14px;font-size:16px'>🎭 Novos Eventos ({novos_eventos})</h3><ul style='padding-left:0;list-style:none'>{itens}</ul>"

    # Constrói HTML das notícias
    html_noticias = ""
    if noticias:
        itens = "".join([f"""
        <li style='margin-bottom:16px;border-left:2px solid #444;padding-left:12px'>
            <strong style='font-size:15px'>{n.get('titulo','Sem título')}</strong><br>
            <span style='color:#aaa;font-size:12px'>{n.get('fonte','') or ''} &nbsp;·&nbsp; ❤️ {n.get('likes',0)} likes</span><br>
            {f'<span style="color:#ccc;font-size:13px;line-height:1.5">{n.get("resumo","")}</span><br>' if n.get('resumo') else ''}
            {f'<a href="{n.get("instagram_url","")}" style="color:#c9a96e;font-size:12px;text-decoration:none">Ver fonte →</a>' if n.get('instagram_url') else ''}
        </li>""" for n in noticias])
        html_noticias = f"<h3 style='color:#c9a96e;margin:28px 0 14px;font-size:16px'>📰 Notícias da Figueira ({novas_noticias})</h3><ul style='padding-left:0;list-style:none'>{itens}</ul>"

    html = f"""
    <div style='font-family:sans-serif;max-width:620px;margin:0 auto;background:#0a0a0b;color:#f0ece4;padding:32px;border-radius:8px'>
        <h2 style='color:#c9a96e;font-size:24px;margin-bottom:4px;font-weight:300'>Figueira da Foz</h2>
        <p style='color:#6b6860;font-size:13px;margin-bottom:8px'>Resumo diário · {data}</p>
        <div style='display:flex;gap:12px;margin-bottom:28px'>
            <span style='background:#1a1a1e;padding:4px 12px;border-radius:4px;font-size:12px;color:#c9a96e'>🎭 {novos_eventos} eventos</span>
            <span style='background:#1a1a1e;padding:4px 12px;border-radius:4px;font-size:12px;color:#aaa'>📰 {novas_noticias} notícias</span>
        </div>
        {html_eventos}
        {html_noticias}
        <hr style='border-color:#222;margin:28px 0'>
        <a href='https://miguelgermano.com' style='color:#c9a96e;font-size:13px;text-decoration:none'>Ver site completo →</a>
    </div>
    """

    try:
        r = requests.post(
            "https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {RESEND_KEY}", "Content-Type": "application/json"},
            json={
                "from": EMAIL_DE,
                "to": EMAIL_PARA,
                "subject": f"📍 Figueira da Foz — {novos_eventos} eventos · {novas_noticias} notícias ({data})",
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
