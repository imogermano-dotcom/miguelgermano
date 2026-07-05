"""
Script: ia_noticias.py
Descrição: Vai buscar posts ao Apify de perfis sobre Inteligência Artificial,
           usa Claude para extrair título e resumo,
           e guarda na tabela 'noticias_ia' no Supabase.
           Envia email diário sempre (com ou sem novidades).

Perfis monitorizados:
    PT: thaismartins_, pplware, exameinformatica, canaltech, tecmundo
    EN: openai, anthropic, googledeepmind, huggingface, techcrunchofficial, theverge

Como usar:
    1. Preenche as configurações abaixo
    2. Corre: python ia_noticias.py
"""

import json
import os
import re
import time
import statistics
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, date, timedelta

# ─── CONFIGURAÇÃO ────────────────────────────────────────────────────────────

from segredos import APIFY_TOKEN, ANTHROPIC_KEY, RESEND_KEY, SUPABASE_URL, SUPABASE_KEY

CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ia_noticias_cache.json")

APIFY_TASK_ID = "kogbQU54eyul3hgdX"
EMAIL_PARA    = ["miguel.germano@gmail.com"]
EMAIL_DE      = "noticias@miguelgermano.com"
DIAS_MAXIMO   = 5   # Janela temporal de posts a considerar (IA: 5 dias)

# Canais YouTube a monitorizar via RSS
CANAIS_YOUTUBE = [
    {"nome": "Onde eu Clico", "channel_id": "UCRVzEriB1B2JKGpsLpL-uNg"},
]

# Nomes legíveis das fontes
NOMES_FONTES = {
    # Português
    "thaismartins_":       "Thais Martin",
    "pplware":             "Pplware",
    "exameinformatica":    "Exame Informática",
    "canaltech":           "Canaltech",
    "tecmundo":            "TecMundo",
    # Inglês
    "openai":              "OpenAI",
    "anthropic":           "Anthropic",
    "googledeepmind":      "Google DeepMind",
    "huggingface":         "Hugging Face",
    "techcrunchofficial":  "TechCrunch",
    "theverge":            "The Verge",
}

# Perfis especializados em IA — todos os posts passam sem filtro de palavras-chave
PERFIS_IA_DIRETOS = {
    "thaismartins_", "openai", "anthropic", "googledeepmind", "huggingface"
}

# Palavras-chave para filtrar posts de media generalista
PALAVRAS_IA = [
    "inteligência artificial", "ia ", " ia,", "machine learning", "deep learning",
    "chatgpt", "gpt", "claude", "gemini", "llm", "modelo de linguagem",
    "openai", "anthropic", "google deepmind", "meta ai", "mistral",
    "hugging face", "transformer", "neural", "chatbot",
    "artificial intelligence", "generative ai", "gen ai", "large language",
    "stable diffusion", "midjourney", "dall-e", "sora", "runway",
    "copilot", "cursor", "github copilot", "ai agent", "agente ia",
    "automação com ia", "IA generativa", "modelo de ia",
    "llama", "grok", "perplexity", "mistral", "qwen",
]


# ─── APIFY ───────────────────────────────────────────────────────────────────

def _apify_ultimo_run_sucedido():
    """Devolve (run_id, dataset_id, horas_atras) do último run SUCCEEDED, ou None."""
    url = (f"https://api.apify.com/v2/actor-tasks/{APIFY_TASK_ID}/runs"
           f"?token={APIFY_TOKEN}&status=SUCCEEDED&limit=1&desc=1")
    try:
        r = requests.get(url, timeout=20)
        if r.status_code != 200:
            return None
        items = r.json().get("data", {}).get("items", [])
        if not items:
            return None
        run = items[0]
        run_id    = run.get("id", "")
        dataset   = run.get("defaultDatasetId", "")
        fim_str   = run.get("finishedAt", "")
        horas     = 999
        if fim_str:
            fim = datetime.fromisoformat(fim_str.replace("Z", "+00:00")).replace(tzinfo=None)
            horas = (datetime.utcnow() - fim).total_seconds() / 3600
        return run_id, dataset, horas
    except Exception as e:
        print(f"   ⚠️  Erro a verificar runs: {e}")
        return None


def _apify_disparar_e_aguardar():
    """Dispara um novo run e aguarda conclusão (máx 15 min). Devolve dataset_id ou None."""
    print("🚀 A disparar Apify task (scrape fresco)...")
    url_run = f"https://api.apify.com/v2/actor-tasks/{APIFY_TASK_ID}/runs?token={APIFY_TOKEN}"
    try:
        r = requests.post(url_run, json={}, timeout=30)
    except Exception as e:
        print(f"   ⚠️  Erro ao disparar: {e}")
        return None
    if r.status_code not in (200, 201):
        print(f"   ⚠️  Não foi possível disparar: {r.status_code} — {r.text[:120]}")
        return None
    run_id   = r.json().get("data", {}).get("id")
    dataset  = r.json().get("data", {}).get("defaultDatasetId")
    if not run_id:
        print("   ⚠️  Run ID não encontrado na resposta.")
        return None
    print(f"   Run iniciado: {run_id}")
    # Polling até 15 minutos (90 × 10s)
    url_status = f"https://api.apify.com/v2/actor-runs/{run_id}?token={APIFY_TOKEN}"
    for i in range(90):
        time.sleep(10)
        try:
            r2 = requests.get(url_status, timeout=20)
            if r2.status_code != 200:
                continue
            dados  = r2.json().get("data", {})
            status = dados.get("status", "")
            dataset = dados.get("defaultDatasetId", dataset)
            if (i + 1) % 6 == 0:   # mostrar progresso a cada 60s
                print(f"   [{(i+1)*10}s] Status: {status}")
            if status in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
                print(f"   ✅ Run terminado: {status}")
                return dataset if status == "SUCCEEDED" else None
        except Exception:
            continue
    print("   ⚠️  Timeout (15 min) — a usar último run bem-sucedido.")
    return None


def _apify_ler_dataset(dataset_id):
    """Lê items de um dataset Apify. Devolve lista de posts válidos."""
    url = f"https://api.apify.com/v2/datasets/{dataset_id}/items?token={APIFY_TOKEN}&clean=true"
    try:
        r = requests.get(url, timeout=60)
        if r.status_code != 200:
            print(f"   ⚠️  Erro ao ler dataset {dataset_id}: {r.status_code}")
            return []
        posts = r.json()
        validos = [p for p in posts if p.get("shortCode") and not p.get("error")]
        print(f"   {len(validos)} posts válidos (de {len(posts)} total no dataset)")
        return validos
    except Exception as e:
        print(f"   ⚠️  Erro ao ler dataset: {e}")
        return []


def obter_posts_apify():
    """Obtém posts do Apify.

    Fluxo:
    1. Verifica se existe run SUCCEEDED com < 23h → usa directamente.
    2. Se não → dispara novo run e aguarda.
    3. Se o novo run falha/timeout → usa o último SUCCEEDED disponível.
    4. Se não há nenhum → devolve [].
    """
    print("📥 A obter posts do Apify (IA)...")

    info_anterior = _apify_ultimo_run_sucedido()

    # Só dispara se o último run tem mais de 23h (ou não existe)
    if info_anterior:
        run_id, dataset_id, horas = info_anterior
        print(f"   Último run bem-sucedido: {int(horas)}h atrás (dataset: {dataset_id})")
        if horas < 23:
            print("   ♻️  Run recente — a reutilizar dados.")
            return _apify_ler_dataset(dataset_id)

    # Precisamos de dados frescos
    novo_dataset = _apify_disparar_e_aguardar()

    if novo_dataset:
        return _apify_ler_dataset(novo_dataset)

    # Fallback: usar o último run SUCCEEDED mesmo que antigo
    if info_anterior:
        _, dataset_id, horas = info_anterior
        print(f"   ⚠️  A usar último run disponível ({int(horas)}h atrás).")
        return _apify_ler_dataset(dataset_id)

    # Último recurso: endpoint runs/last (pode devolver run em curso)
    print("   ⚠️  Sem runs SUCCEEDED — a tentar runs/last...")
    url = (f"https://api.apify.com/v2/actor-tasks/{APIFY_TASK_ID}"
           f"/runs/last/dataset/items?token={APIFY_TOKEN}&status=SUCCEEDED")
    try:
        r = requests.get(url, timeout=60)
        if r.status_code == 404:
            print("   ⚠️  Nenhum run encontrado.")
            return []
        if r.status_code != 200:
            print(f"   ⚠️  Erro Apify: {r.status_code}")
            return []
        posts = r.json()
        validos = [p for p in posts if p.get("shortCode") and not p.get("error")]
        print(f"   {len(validos)} posts (fallback runs/last)")
        return validos
    except Exception as e:
        print(f"   ⚠️  Erro: {e}")
        return []


# ─── FILTRO TEMPORAL ─────────────────────────────────────────────────────────

def filtrar_posts_recentes(posts):
    """Filtra posts dos últimos DIAS_MAXIMO dias."""
    limite = datetime.now() - timedelta(days=DIAS_MAXIMO)
    recentes = []
    for post in posts:
        ts = post.get("timestamp")
        if not ts:
            continue
        try:
            dt = datetime.fromisoformat(ts.replace("Z", ""))
            if dt >= limite:
                recentes.append(post)
        except Exception:
            continue
    print(f"   {len(recentes)} posts dos últimos {DIAS_MAXIMO} dias (de {len(posts)} total)")
    return recentes


# ─── FILTRO IA ────────────────────────────────────────────────────────────────

def e_sobre_ia(caption, username):
    """Verifica se o post é sobre Inteligência Artificial."""
    if username in PERFIS_IA_DIRETOS:
        return True
    if not caption:
        return False
    texto = caption.lower()
    return any(palavra in texto for palavra in PALAVRAS_IA)


# ─── THRESHOLD DE ENGAGEMENT ─────────────────────────────────────────────────

def calcular_thresholds(posts):
    """Calcula threshold de engagement (mediana × 0.5) por perfil."""
    likes_por_perfil = {}
    for post in posts:
        username = post.get("ownerUsername", "")
        likes = max(0, post.get("likesCount") or 0)
        likes_por_perfil.setdefault(username, []).append(likes)

    thresholds = {}
    print("\n📊 Thresholds calculados:")
    for username, likes_list in likes_por_perfil.items():
        mediana = statistics.median(likes_list) if likes_list else 1
        threshold = max(1, int(mediana * 0.5))
        thresholds[username] = threshold
        fonte = NOMES_FONTES.get(username, username)
        print(f"   {fonte}: mediana={int(mediana)} → threshold={threshold} likes")
    return thresholds


# ─── CLAUDE API ──────────────────────────────────────────────────────────────

def extrair_dados_noticia_ia(caption, fonte):
    """Usa Claude para extrair título e resumo de notícia de IA."""
    if not caption or len(caption.strip()) < 20:
        return None

    prompt = f"""Analisa este post do Instagram de "{fonte}" sobre Inteligência Artificial.

Texto:
{caption[:2000]}

Devolve APENAS um JSON válido sem texto extra:
{{
  "titulo": "título claro e informativo da notícia (máx 110 caracteres) ou null",
  "resumo": "desenvolvimento completo da notícia em 4-6 frases (máx 700 caracteres). Explica o que aconteceu, porquê é relevante, o impacto esperado e qualquer detalhe técnico ou contexto importante. Escreve em português europeu, tom jornalístico. ou null",
  "e_relevante": true ou false
}}

e_relevante = true se for sobre IA, modelos de linguagem, ferramentas de IA, lançamentos, investigação ou impacto da IA.
e_relevante = false se for publicidade genérica, conteúdo pessoal sem relação com IA, ou tópico não relacionado."""

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
                "max_tokens": 800,
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


# ─── CACHE LOCAL ─────────────────────────────────────────────────────────────

def carregar_cache():
    """Carrega cache local de notícias já processadas."""
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"short_codes": {}, "titulos": []}


def guardar_cache(cache):
    """Guarda cache local com escrita atómica (evita corrupção)."""
    tmp = CACHE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, CACHE_FILE)


def limpar_cache_antigo(cache):
    """Remove entradas com mais de 30 dias do cache."""
    limite = (date.today() - timedelta(days=30)).isoformat()
    cache["short_codes"] = {
        sc: dt for sc, dt in cache["short_codes"].items()
        if dt and dt >= limite
    }
    # Mantém apenas os últimos 500 títulos
    cache["titulos"] = cache["titulos"][-500:]
    return cache


# ─── SUPABASE ────────────────────────────────────────────────────────────────

def _sb_headers():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=ignore-duplicates,return=minimal"
    }


def supabase_inserir_noticia(noticia):
    """Insere notícia na tabela noticias_ia. Ignora duplicados por short_code."""
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/noticias_ia",
        headers=_sb_headers(),
        json=noticia,
        timeout=20
    )
    return r.status_code in (200, 201)


def supabase_obter_existentes():
    """Devolve set de short_codes já na tabela noticias_ia (últimos 10 dias)."""
    limite = (date.today() - timedelta(days=10)).isoformat()
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/noticias_ia?select=short_code&data_post=gte.{limite}",
        headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"},
        timeout=20
    )
    if r.status_code == 200:
        return {row["short_code"] for row in r.json()}
    return set()


def supabase_obter_ultimas(dias=10):
    """Devolve todas as notícias dos últimos N dias, ordenadas por engagement desc."""
    limite = (date.today() - timedelta(days=dias)).isoformat()
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/noticias_ia"
        f"?select=*&data_post=gte.{limite}&order=engagement.desc",
        headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"},
        timeout=20
    )
    if r.status_code == 200:
        return r.json()
    print(f"   ⚠️  Erro ao obter notícias do Supabase: {r.status_code}")
    return []


def supabase_limpar_antigas(dias=10):
    """Apaga notícias com mais de N dias."""
    limite = (date.today() - timedelta(days=dias)).isoformat()
    r = requests.delete(
        f"{SUPABASE_URL}/rest/v1/noticias_ia?data_post=lt.{limite}",
        headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"},
        timeout=20
    )
    if r.status_code in (200, 204):
        print(f"   🗑️  Notícias com mais de {dias} dias apagadas do Supabase")


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


# ─── YOUTUBE RSS ─────────────────────────────────────────────────────────────

def obter_videos_youtube():
    """Obtém vídeos recentes dos canais YouTube via RSS."""
    limite = datetime.now() - timedelta(days=DIAS_MAXIMO)
    videos = []
    NS = {"atom": "http://www.w3.org/2005/Atom", "media": "http://search.yahoo.com/mrss/"}

    for canal in CANAIS_YOUTUBE:
        try:
            url = f"https://www.youtube.com/feeds/videos.xml?channel_id={canal['channel_id']}"
            r = requests.get(url, timeout=20)
            r.raise_for_status()
            root = ET.fromstring(r.content)

            for entry in root.findall("atom:entry", NS):
                published_str = entry.findtext("atom:published", "", NS)
                try:
                    published = datetime.fromisoformat(published_str.replace("Z", "+00:00")).replace(tzinfo=None)
                except Exception:
                    continue
                if published < limite:
                    continue

                titulo = entry.findtext("atom:title", "", NS)
                link_el = entry.find("atom:link", NS)
                video_url = link_el.get("href") if link_el is not None else ""
                video_id = entry.findtext("yt:videoId", "", {"yt": "http://www.youtube.com/xml/schemas/2015"})
                thumbnail = f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg" if video_id else ""
                descricao = ""
                media_group = entry.find("media:group", NS)
                if media_group is not None:
                    desc_el = media_group.find("media:description", NS)
                    if desc_el is not None and desc_el.text:
                        descricao = desc_el.text[:300]

                videos.append({
                    "short_code": f"yt_{video_id}" if video_id else f"yt_{hash(video_url)}",
                    "titulo": titulo,
                    "resumo": descricao or None,
                    "imagem_url": thumbnail,
                    "instagram_url": video_url,
                    "fonte": canal["nome"],
                    "likes": 0,
                    "comentarios": 0,
                    "visualizacoes": 0,
                    "engagement": 0,
                    "data_post": published.date().isoformat(),
                    "timestamp_post": published_str,
                    "e_youtube": True,
                })
            print(f"   📺 {canal['nome']}: {len([v for v in videos if v['fonte'] == canal['nome']])} vídeos recentes")
        except Exception as e:
            print(f"   ⚠️  Erro YouTube ({canal['nome']}): {e}")

    return videos


# ─── PROGRAMA PRINCIPAL ───────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("🤖 Notícias IA")
    print(f"   Data: {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print("=" * 60)

    # Sem scrape Apify ao fim-de-semana (poupança de recursos)
    dia_semana = datetime.now().weekday()  # 5=Sábado, 6=Domingo
    if dia_semana >= 5:
        print(f"[ia_noticias] Fim-de-semana — scrape Apify ignorado.")
        return

    # YouTube RSS
    print("\n📺 A obter vídeos YouTube...")
    videos_youtube = obter_videos_youtube()

    posts = obter_posts_apify()
    if not posts and not videos_youtube:
        print("❌ Sem posts nem vídeos.")
        todas_noticias = supabase_obter_ultimas(dias=10)
        enviar_email_resumo(0, [], todas_noticias)
        return

    posts = filtrar_posts_recentes(posts)
    thresholds = calcular_thresholds(posts) if posts else {}

    print("\n🔍 A verificar existentes (Supabase + cache)...")
    cache = carregar_cache()
    existentes_cache = set(cache["short_codes"].keys())
    existentes_supabase = supabase_obter_existentes()
    existentes = existentes_cache | existentes_supabase
    titulos = set(cache["titulos"])
    print(f"   {len(existentes_supabase)} no Supabase, {len(existentes_cache)} no cache")

    novas = 0
    filtrados_ia = 0
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

        if short_code in existentes:
            print(f"[{i+1}/{len(posts)}] ⏭️  {short_code} — já existe")
            continue

        fonte = NOMES_FONTES.get(username, username)

        if not e_sobre_ia(caption, username):
            print(f"[{i+1}/{len(posts)}] 🔕 {short_code} ({fonte}) — não é sobre IA")
            filtrados_ia += 1
            continue

        threshold = thresholds.get(username, 1)
        if likes < threshold:
            print(f"[{i+1}/{len(posts)}] 📉 {short_code} ({fonte}) — engagement baixo ({likes} < {threshold})")
            filtrados_engagement += 1
            continue

        print(f"[{i+1}/{len(posts)}] 🤖 {short_code} — {fonte} ({likes} likes)")

        dados = extrair_dados_noticia_ia(caption, fonte)
        if not dados or not dados.get("e_relevante"):
            print(f"   ⏭️  Não é relevante para IA")
            ignorados += 1
            continue

        titulo = dados.get("titulo")

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

        # Guardar no Supabase
        supabase_inserir_noticia(noticia)
        # Actualizar cache local
        cache["short_codes"][short_code] = data_post.isoformat() if data_post else date.today().isoformat()
        if titulo:
            cache["titulos"].append(titulo.lower().strip())
            titulos.add(titulo.lower().strip())
        novas += 1
        lista_novas.append(noticia)
        print(f"   ✅ Guardada (Supabase + cache)")

        time.sleep(0.5)

    print("\n" + "=" * 60)
    print("📊 RESUMO")
    print(f"   🤖 Novas notícias IA:          {novas}")
    print(f"   🔕 Filtradas (não IA):         {filtrados_ia}")
    print(f"   📉 Filtradas (engagement):     {filtrados_engagement}")
    print(f"   ⏭️  Ignoradas (outras):         {ignorados}")
    print(f"   ✅ Concluído!")
    print("=" * 60)

    # Adiciona vídeos YouTube não duplicados
    for video in videos_youtube:
        if video["short_code"] not in existentes:
            noticia_yt = {k: v for k, v in video.items()}
            noticia_yt["e_youtube"] = True
            supabase_inserir_noticia(noticia_yt)
            cache["short_codes"][video["short_code"]] = video.get("data_post") or date.today().isoformat()
            novas += 1
            lista_novas.append(video)
            print(f"   📺 YouTube adicionado: {video['titulo'][:60]}")

    # Limpeza de notícias antigas no Supabase (> 10 dias)
    supabase_limpar_antigas(dias=10)

    cache = limpar_cache_antigo(cache)
    guardar_cache(cache)

    # Obter todas as notícias dos últimos 10 dias para o email
    print("\n📬 A obter arquivo dos últimos 10 dias para o email...")
    todas_noticias = supabase_obter_ultimas(dias=10)
    print(f"   {len(todas_noticias)} notícias no arquivo")

    enviar_email_resumo(novas, lista_novas, todas_noticias)




def _html_noticia(n, destaque=False):
    """Gera o HTML de um cartão de notícia."""
    borda = "border-left:4px solid #b45309;" if destaque else ""
    link_txt = "Ver no YouTube →" if n.get("e_youtube") else "Ver fonte →"
    data_str = ""
    if n.get("data_post"):
        try:
            data_str = datetime.strptime(str(n["data_post"])[:10], "%Y-%m-%d").strftime("%d/%m")
        except Exception:
            data_str = str(n["data_post"])[:10]
    return f"""
        <li style='margin-bottom:20px;background:#fff;border-radius:10px;overflow:hidden;
                   box-shadow:0 1px 4px rgba(0,0,0,0.08);{borda}'>
            {f'<img src="{n.get("imagem_url")}" style="width:100%;height:180px;object-fit:cover;display:block" alt="">' if n.get("imagem_url") else ''}
            <div style='padding:16px 20px'>
                <strong style='font-size:15px;color:#1a1a1a;line-height:1.4;display:block;margin-bottom:6px'>{n.get("titulo") or "Sem título"}</strong>
                <span style='color:#999;font-size:12px'>
                    {n.get("fonte") or ""}&nbsp;·&nbsp;❤️ {n.get("likes") or 0} likes
                    {f"&nbsp;·&nbsp;📅 {data_str}" if data_str else ""}
                </span>
                {f'<p style="color:#444;font-size:14px;line-height:1.7;margin:10px 0 12px">{n.get("resumo","")}</p>' if n.get("resumo") else ''}
                {f'<a href="{n.get("instagram_url","")}" style="color:#b45309;font-size:13px;text-decoration:none;font-weight:500">{link_txt}</a>' if n.get("instagram_url") else ''}
            </div>
        </li>"""


def _html_seccao(titulo, emoji, items, destaque=False):
    """Gera uma secção do email com título e lista de notícias."""
    if not items:
        return ""
    cards = "".join(_html_noticia(n, destaque) for n in items)
    return f"""
    <div style='margin-bottom:32px'>
        <h2 style='color:#1a1a1a;font-size:17px;font-weight:700;margin:0 0 14px;
                   padding-bottom:8px;border-bottom:2px solid #f0f0f0'>
            {emoji} {titulo}
        </h2>
        <ul style='padding-left:0;list-style:none;margin:0'>{cards}</ul>
    </div>"""


def enviar_email_resumo(novas, lista_novas, todas_noticias=None):
    """Envia resumo diário de notícias de IA por email via Resend.

    Estrutura do email:
      1. Novas notícias (desta execução)
      2. Mais engagement (últimos 10 dias, excluindo as novas)
      3. Mais antigas (restantes dos últimos 10 dias)
    """
    data = datetime.now().strftime("%d/%m/%Y")
    todas_noticias = todas_noticias or []

    # Separar notícias por secção
    short_codes_novos = {n.get("short_code") for n in lista_novas}

    # Secção 2: top 5 por engagement, excluindo as novas
    top_engagement = [
        n for n in todas_noticias
        if n.get("short_code") not in short_codes_novos
    ][:5]

    short_codes_top = {n.get("short_code") for n in top_engagement}

    # Secção 3: restantes (excluindo novas e top engagement), ordem cronológica inversa
    restantes = [
        n for n in todas_noticias
        if n.get("short_code") not in short_codes_novos
        and n.get("short_code") not in short_codes_top
    ]
    # ordenar do mais recente para o mais antigo
    restantes.sort(key=lambda n: n.get("data_post") or "", reverse=True)

    # Construir HTML das secções
    if novas == 0 and not todas_noticias:
        html_corpo = """
        <div style='background:#f5f5f5;border-radius:8px;padding:18px 22px'>
            <p style='color:#999;font-size:14px;margin:0'>Sem notícias de IA nos últimos 10 dias.</p>
        </div>"""
    else:
        seccao_novas = _html_seccao(
            f"{novas} nova{'s' if novas != 1 else ''} hoje", "🆕", lista_novas, destaque=True
        ) if novas > 0 else """
        <div style='background:#f5f5f5;border-radius:8px;padding:14px 18px;margin-bottom:24px'>
            <p style='color:#999;font-size:13px;margin:0'>Sem novas notícias hoje.</p>
        </div>"""

        seccao_engagement = _html_seccao("Mais relevantes esta semana", "🔥", top_engagement)
        seccao_antigas    = _html_seccao("Arquivo — últimos 10 dias", "📂", restantes)

        html_corpo = seccao_novas + seccao_engagement + seccao_antigas

    total_arquivo = len(todas_noticias)
    html = f"""<!DOCTYPE html>
<html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'></head>
<body style='margin:0;padding:0;background:#f7f7f7;
             font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif'>
<div style='max-width:640px;margin:32px auto;padding:0 16px'>

    <!-- Cabeçalho -->
    <div style='background:#fff;border-radius:12px;padding:28px 32px 24px;
                margin-bottom:4px;box-shadow:0 1px 4px rgba(0,0,0,0.06)'>
        <p style='color:#b45309;font-size:12px;font-weight:600;letter-spacing:1.5px;
                  text-transform:uppercase;margin:0 0 6px'>Resumo diário · {data}</p>
        <h1 style='color:#1a1a1a;font-size:26px;font-weight:700;margin:0 0 4px'>
            Inteligência Artificial
        </h1>
        <p style='color:#999;font-size:14px;margin:0'>
            {novas} nova{'s' if novas != 1 else ''} hoje
            {"&nbsp;·&nbsp;" + str(total_arquivo) + " no arquivo (10 dias)" if total_arquivo > 0 else ""}
        </p>
    </div>

    <!-- Conteúdo -->
    <div style='background:#f7f7f7;padding:24px 0'>
        {html_corpo}
    </div>

    <!-- Rodapé -->
    <div style='text-align:center;padding:24px 0 40px'>
        <a href='https://miguelgermano.com'
           style='display:inline-block;background:#b45309;color:#fff;font-size:14px;
                  font-weight:500;padding:12px 28px;border-radius:8px;text-decoration:none'>
            Ver site completo
        </a>
        <p style='color:#bbb;font-size:12px;margin:20px 0 0'>miguelgermano.com</p>
    </div>

</div>
</body></html>"""

    assunto = (
        f"🤖 IA — sem novidades hoje · {total_arquivo} em arquivo ({data})"
        if novas == 0
        else f"🤖 IA — {novas} nova{'s' if novas != 1 else ''} · {total_arquivo} em arquivo ({data})"
    )

    try:
        r = requests.post(
            "https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {RESEND_KEY}", "Content-Type": "application/json"},
            json={"from": EMAIL_DE, "to": EMAIL_PARA, "subject": assunto, "html": html},
            timeout=30
        )
        if r.status_code in (200, 201):
            print(f"\n📧 Email enviado: {assunto}")
        else:
            print(f"\n⚠️  Erro ao enviar email: {r.status_code} — {r.text[:100]}")
    except Exception as e:
        print(f"\n⚠️  Erro ao enviar email: {e}")


if __name__ == "__main__":
    main()
    from consolidar_noticias import consolidar_tabela
    consolidar_tabela("noticias_ia", dias=5)
