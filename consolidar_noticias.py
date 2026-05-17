"""
consolidar_noticias.py

Motor de consolidação de notícias duplicadas.
Detecta notícias sobre o mesmo tema via:
  1. Similaridade de título (rápido, sem custo)
  2. Claude Haiku para casos ambíguos (mais preciso)

Quando detecta duplicados:
  - Agrega todos os textos de todas as fontes
  - Pede ao Claude Haiku para gerar um registo consolidado
  - Insere o registo consolidado
  - Apaga os registos parciais
"""

import json
import re
import time
import requests
from datetime import datetime, timezone, timedelta
from difflib import SequenceMatcher

from segredos import ANTHROPIC_KEY, SUPABASE_URL, SUPABASE_KEY

TABELAS = {
    "figueira": "noticias",
    "imoveis":  "noticias_imoveis",
    "ia":       "noticias_ia"
}

# Threshold de similaridade para considerar duplicado sem consultar Claude
THRESHOLD_SIMILARIDADE = 0.70  # 70% de palavras em comum → duplicado directo
THRESHOLD_AMBIGUO      = 0.45  # entre 45-70% → pedir ao Claude para decidir


# ─── SUPABASE REST ────────────────────────────────────────────────────────────

SB_HEADERS = {
    "apikey":        SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type":  "application/json",
}


def sb_select(tabela: str, params: str) -> list[dict]:
    """GET /rest/v1/{tabela}?{params}"""
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/{tabela}?{params}",
        headers=SB_HEADERS,
        timeout=30
    )
    if r.status_code != 200:
        print(f"[supabase] Erro ao ler {tabela}: {r.status_code} {r.text[:120]}")
        return []
    return r.json() or []


def sb_insert(tabela: str, data: dict) -> bool:
    """POST /rest/v1/{tabela}"""
    headers = {**SB_HEADERS, "Prefer": "return=minimal"}
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/{tabela}",
        headers=headers,
        json=data,
        timeout=30
    )
    if r.status_code not in (200, 201):
        print(f"[supabase] Erro ao inserir em {tabela}: {r.status_code} {r.text[:120]}")
        return False
    return True


def sb_update(tabela: str, id_val: str, data: dict) -> bool:
    """PATCH /rest/v1/{tabela}?id=eq.{id}"""
    headers = {**SB_HEADERS, "Prefer": "return=minimal"}
    r = requests.patch(
        f"{SUPABASE_URL}/rest/v1/{tabela}?id=eq.{id_val}",
        headers=headers,
        json=data,
        timeout=30
    )
    if r.status_code not in (200, 204):
        print(f"[supabase] Erro ao actualizar {tabela}/{id_val}: {r.status_code} {r.text[:120]}")
        return False
    return True


def sb_delete_many(tabela: str, ids: list[str]) -> bool:
    """DELETE /rest/v1/{tabela}?id=in.({id1},{id2},...) """
    ids_str = ",".join(ids)
    r = requests.delete(
        f"{SUPABASE_URL}/rest/v1/{tabela}?id=in.({ids_str})",
        headers=SB_HEADERS,
        timeout=30
    )
    if r.status_code not in (200, 204):
        print(f"[supabase] Erro ao apagar de {tabela}: {r.status_code} {r.text[:120]}")
        return False
    return True


# ─── CLAUDE ───────────────────────────────────────────────────────────────────

def claude_call(prompt: str, max_tokens: int = 20) -> str | None:
    """Chama a Claude Haiku e devolve o texto da resposta."""
    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key":         ANTHROPIC_KEY,
                "anthropic-version": "2023-06-01",
                "content-type":      "application/json"
            },
            json={
                "model":      "claude-haiku-4-5-20251001",
                "max_tokens": max_tokens,
                "messages":   [{"role": "user", "content": prompt}]
            },
            timeout=30
        )
        if r.status_code != 200:
            print(f"[claude] Erro {r.status_code}: {r.text[:120]}")
            return None
        return r.json()["content"][0]["text"].strip()
    except Exception as e:
        print(f"[claude] Excepção: {e}")
        return None


# ─── DETECÇÃO ─────────────────────────────────────────────────────────────────

def similaridade_titulos(titulo_a: str, titulo_b: str) -> float:
    """Calcula similaridade entre dois títulos (0.0 a 1.0)."""
    a = titulo_a.lower().strip()
    b = titulo_b.lower().strip()
    return SequenceMatcher(None, a, b).ratio()


def claude_decide_duplicado(titulo_a: str, resumo_a: str,
                             titulo_b: str, resumo_b: str) -> bool:
    """
    Pede ao Claude Haiku para decidir se duas notícias são sobre o mesmo tema.
    Usado apenas para casos ambíguos (similaridade entre 45-70%).
    """
    prompt = f"""Analisa estas duas notícias e responde apenas com SIM ou NÃO.
São sobre o mesmo acontecimento ou tema principal?

Notícia A:
Título: {titulo_a}
Resumo: {(resumo_a or '')[:300]}

Notícia B:
Título: {titulo_b}
Resumo: {(resumo_b or '')[:300]}

Responde apenas: SIM ou NÃO"""

    resposta = claude_call(prompt, max_tokens=10)
    if not resposta:
        return False
    return "SIM" in resposta.upper()


def encontrar_grupos_duplicados(registos: list[dict]) -> list[list[dict]]:
    """
    Agrupa registos que são sobre o mesmo tema.
    Devolve lista de grupos — cada grupo é uma lista de registos duplicados.
    Registos únicos ficam em grupos de tamanho 1.
    """
    n = len(registos)
    visitado = [False] * n
    grupos = []

    for i in range(n):
        if visitado[i]:
            continue
        grupo = [registos[i]]
        visitado[i] = True

        for j in range(i + 1, n):
            if visitado[j]:
                continue

            sim = similaridade_titulos(registos[i]["titulo"], registos[j]["titulo"])

            if sim >= THRESHOLD_SIMILARIDADE:
                # Duplicado directo — sem consultar Claude
                grupo.append(registos[j])
                visitado[j] = True

            elif sim >= THRESHOLD_AMBIGUO:
                # Caso ambíguo — perguntar ao Claude
                time.sleep(0.2)
                if claude_decide_duplicado(
                    registos[i]["titulo"], registos[i].get("resumo", ""),
                    registos[j]["titulo"], registos[j].get("resumo", "")
                ):
                    grupo.append(registos[j])
                    visitado[j] = True

        grupos.append(grupo)

    return grupos


# ─── CONSOLIDAÇÃO ─────────────────────────────────────────────────────────────

def gerar_noticia_consolidada(titulos: list[str], textos: list[str]) -> dict | None:
    """
    Pede ao Claude Haiku para gerar título, resumo e texto_completo consolidados
    a partir de múltiplas fontes sobre o mesmo tema.
    """
    prompt = f"""És um jornalista português. Tens abaixo o mesmo acontecimento coberto
por {len(textos)} fontes diferentes. Consolida toda a informação num único artigo
jornalístico em português europeu.

Responde APENAS com JSON válido neste formato exacto:
{{
  "titulo": "Título claro e informativo (máximo 100 caracteres)",
  "resumo": "Resumo de 2-3 frases que capta o essencial",
  "texto_completo": "Artigo desenvolvido de 4-6 parágrafos que integra toda a informação disponível. Sem repetições. Sem inventar factos."
}}

Títulos das fontes (para referência): {titulos}

Conteúdo das fontes:
{chr(10).join(textos[:5])}"""

    resposta = claude_call(prompt, max_tokens=2000)
    if not resposta:
        return None
    try:
        texto = re.sub(r"```json|```", "", resposta).strip()
        return json.loads(texto)
    except Exception as e:
        print(f"[claude] Erro ao parsear JSON consolidado: {e}")
        return None


def consolidar_grupo(grupo: list[dict], tabela: str) -> dict | None:
    """
    Recebe um grupo de registos duplicados e devolve um único registo consolidado.
    Se o grupo tiver apenas 1 elemento, marca como consolidado e devolve None.
    """
    agora = datetime.now(timezone.utc).isoformat()

    if len(grupo) == 1:
        # Sem duplicados — apenas marcar como consolidado e popular `fontes`
        r = grupo[0]
        fonte_entry = {
            "fonte":          r["fonte"],
            "url":            r.get("instagram_url"),
            "fonte_tipo":     r.get("fonte_tipo", "instagram"),
            "texto_extraido": r.get("texto_completo"),
            "likes":          r.get("likes"),
            "data_post":      str(r.get("data_post"))
        }
        sb_update(tabela, r["id"], {
            "fontes":         [fonte_entry],
            "consolidado":    True,
            "consolidado_em": agora
        })
        return None

    print(f"  Consolidando {len(grupo)} registos: {[r['titulo'][:40] for r in grupo]}")

    # Ordenar: RSS com texto_completo primeiro, depois por likes
    grupo_ordenado = sorted(
        grupo,
        key=lambda r: (
            r.get("fonte_tipo") == "rss" and bool(r.get("texto_completo")),
            bool(r.get("texto_completo")),
            r.get("likes") or 0
        ),
        reverse=True
    )

    base = grupo_ordenado[0]

    # Agregar fontes e textos de todos os registos
    fontes_list = []
    textos_para_claude = []

    for r in grupo_ordenado:
        fonte_entry = {
            "fonte":          r["fonte"],
            "url":            r.get("instagram_url"),
            "fonte_tipo":     r.get("fonte_tipo", "instagram"),
            "texto_extraido": r.get("texto_completo"),
            "likes":          r.get("likes"),
            "data_post":      str(r.get("data_post"))
        }
        fontes_list.append(fonte_entry)

        if r.get("texto_completo"):
            textos_para_claude.append(
                f"--- Fonte: {r['fonte']} ({r.get('fonte_tipo', 'instagram')}) ---\n"
                f"{r['texto_completo'][:3000]}"
            )
        elif r.get("resumo"):
            textos_para_claude.append(
                f"--- Fonte: {r['fonte']} ---\n{r['resumo']}"
            )

    # Gerar registo consolidado com Claude
    conteudo_consolidado = gerar_noticia_consolidada(
        titulos=[r["titulo"] for r in grupo_ordenado],
        textos=textos_para_claude
    )

    if not conteudo_consolidado:
        # Fallback: usar o registo base sem consolidação Claude
        conteudo_consolidado = {
            "titulo":          base["titulo"],
            "resumo":          base.get("resumo", ""),
            "texto_completo":  base.get("texto_completo", "")
        }

    # Escolher melhor imagem disponível
    imagem = next(
        (r["imagem_url"] for r in grupo_ordenado if r.get("imagem_url")),
        None
    )

    # Montar registo final consolidado
    novo_registo = {
        "short_code":      f"cons_{base['short_code']}",
        "titulo":          conteudo_consolidado["titulo"],
        "resumo":          conteudo_consolidado["resumo"],
        "imagem_url":      imagem,
        "instagram_url":   base.get("instagram_url"),
        "fonte":           " + ".join(dict.fromkeys(r["fonte"] for r in grupo_ordenado)),
        "likes":           max((r.get("likes") or 0 for r in grupo), default=None),
        "data_post":       str(base["data_post"]),
        "fonte_tipo":      "consolidado",
        "fontes":          fontes_list,
        "consolidado":     True,
        "consolidado_em":  agora
    }

    # Campos opcionais (presentes nalgumas tabelas mas não em todas)
    if conteudo_consolidado.get("texto_completo"):
        novo_registo["texto_completo"] = conteudo_consolidado["texto_completo"]
    if tabela == "noticias_ia":
        novo_registo["e_youtube"] = any(r.get("e_youtube") for r in grupo)

    return novo_registo


# ─── ORQUESTRADOR ─────────────────────────────────────────────────────────────

def consolidar_tabela(tabela: str, dias: int = 3):
    """
    Processa uma tabela completa:
    1. Busca registos não consolidados dos últimos N dias
    2. Detecta grupos de duplicados
    3. Para cada grupo com duplicados: cria registo consolidado, apaga parciais
    4. Para singletons: marca como consolidado
    """
    data_limite = (datetime.now() - timedelta(days=dias)).strftime("%Y-%m-%d")

    registos = sb_select(
        tabela,
        f"select=*&consolidado=eq.false&data_post=gte.{data_limite}&order=data_post.desc"
    )

    if not registos:
        print(f"[{tabela}] Sem registos novos para consolidar.")
        return

    print(f"[{tabela}] {len(registos)} registos para processar...")

    grupos = encontrar_grupos_duplicados(registos)
    duplicados = [g for g in grupos if len(g) > 1]
    singletons = [g for g in grupos if len(g) == 1]

    print(f"  {len(duplicados)} grupos com duplicados, {len(singletons)} únicos")

    # Processar duplicados
    for grupo in duplicados:
        novo_registo = consolidar_grupo(grupo, tabela)

        if novo_registo:
            if sb_insert(tabela, novo_registo):
                print(f"  ✓ Consolidado: {novo_registo['titulo'][:60]}")
            else:
                continue  # Não apagar parciais se inserção falhou

            # Apagar registos parciais
            ids_para_apagar = [r["id"] for r in grupo]
            if sb_delete_many(tabela, ids_para_apagar):
                print(f"    Apagados {len(ids_para_apagar)} registos parciais")

        time.sleep(0.3)

    # Marcar singletons como consolidados (sem duplicados)
    for grupo in singletons:
        consolidar_grupo(grupo, tabela)

    print(f"[{tabela}] Concluído.")


def consolidar_todos(dias: int = 3):
    """Corre o motor de consolidação em todas as tabelas."""
    print(f"\n=== Motor de Consolidação — últimos {dias} dias ===\n")
    for categoria, tabela in TABELAS.items():
        consolidar_tabela(tabela, dias=dias)
        print()
    print("=== Consolidação concluída ===\n")


if __name__ == "__main__":
    consolidar_todos(dias=3)
