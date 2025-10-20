# ===============================================================================
# 🏆 BOT DE ESTATÍSTICAS DE CONFRONTO V2.0.0 - VERSÃO FINAL E FUNCIONAL
# ===============================================================================

# ===== Importações Essenciais =====
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests
import os 
import tempfile
import asyncio
import logging
from datetime import datetime, timedelta, timezone
import nest_asyncio

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, ContextTypes, JobQueue 
from telegram.error import BadRequest

# Configuração de Logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
nest_asyncio.apply()

# ===== Variáveis de Configuração (LIDAS DE VARIÁVEIS DE AMBIENTE) =====
BOT_TOKEN = os.environ.get("BOT_TOKEN", "SEU_TOKEN_AQUI") 
API_KEY = os.environ.get("API_KEY", "SUA_API_KEY_AQUI")
SHEET_URL = os.environ.get("SHEET_URL", "https://docs.google.com/spreadsheets/d/1ChFFXQxo1qQElNzh2OC8-UPgofRXxyVWN06ExBQ3YqY/edit?usp=drivesdk")

# Mapeamento de Ligas
LIGAS_MAP = {
    "CL": {"sheet_past": "CL", "sheet_future": "CL_FJ"},
    "BSA": {"sheet_past": "BSA", "sheet_future": "BSA_FJ"},
    "BL1": {"sheet_past": "BL1", "sheet_future": "BL1_FJ"},
    "PL": {"sheet_past": "PL", "sheet_future": "PL_FJ"},
    "ELC": {"sheet_past": "ELC", "sheet_future": "ELC_FJ"},
    "DED": {"sheet_past": "DED", "sheet_future": "DED_FJ"},
    "PD": {"sheet_past": "PD", "sheet_future": "PD_FJ"},
    "PPL": {"sheet_past": "PPL", "sheet_future": "PPL_FJ"},
    "SA": {"sheet_past": "SA", "sheet_future": "SA_FJ"},
    "FL1": {"sheet_past": "FL1", "sheet_future": "FL1_FJ"},
}
ABAS_PASSADO = list(LIGAS_MAP.keys())

ULTIMOS = 10
SHEET_CACHE = {}
CACHE_DURATION_SECONDS = 3600 # 1 hora
MAX_GAMES_LISTED = 30

# Filtros reutilizáveis para Estatísticas e Resultados
CONFRONTO_FILTROS = [
    (f"ÚLTIMOS {ULTIMOS} GERAL", ULTIMOS, None, None),
    (f"ÚLTIMOS {ULTIMOS} (M CASA vs V FORA)", ULTIMOS, "casa", "fora")
]

LIVE_STATUSES = ["IN_PLAY", "HALF_TIME", "PAUSED"]

# =================================================================================
# ✅ CONEXÃO GSHEETS VIA VARIÁVEL DE AMBIENTE (Robusta para Railway)
# =================================================================================

CREDS_JSON = os.environ.get("GSPREAD_CREDS_JSON")
client = None

if not CREDS_JSON:
    logging.error("❌ ERRO DE AUTORIZAÇÃO GSHEET: Variável GSPREAD_CREDS_JSON não encontrada. Configure-a no Railway.")
else:
    try:
        # Usa um arquivo temporário para carregar as credenciais
        with tempfile.NamedTemporaryFile(mode="w", delete=False, encoding='utf-8') as tmp_file:
            tmp_file.write(CREDS_JSON)
            tmp_file_path = tmp_file.name
        
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(tmp_file_path, scope)
        client = gspread.authorize(creds)
        logging.info("✅ Conexão GSheets estabelecida via Variável de Ambiente.")
        os.remove(tmp_file_path) # Limpa o arquivo temporário

    except Exception as e:
        logging.error(f"❌ ERRO DE AUTORIZAÇÃO GSHEET: Erro ao carregar ou autorizar credenciais JSON: {e}")
        client = None

# =================================================================================
# 💾 FUNÇÕES DE SUPORTE E CACHING 
# =================================================================================
def safe_int(v):
    try: return int(v)
    except: return 0

def pct(part, total):
    return f"{(part/total)*100:.1f}%" if total>0 else "—"

def media(part, total):
    return f"{(part/total):.2f}" if total>0 else "—"

def escape_markdown(text):
    """FIX CRÍTICO: Escapa caracteres que podem ser interpretados como Markdown (V1) e causavam o erro BadRequest."""
    # Escapa *, _, [ e ] que são os caracteres mais problemáticos
    return str(text).replace('*', '\\*').replace('_', '\\_').replace('[', '\\[') .replace(']', '\\]')

def get_sheet_data(aba_code):
    """Obtém dados da aba de histórico (sheet_past) com cache."""
    global SHEET_CACHE
    agora = datetime.now()

    aba_name = LIGAS_MAP[aba_code]['sheet_past']

    if aba_name in SHEET_CACHE:
        cache_tempo = SHEET_CACHE[aba_name]['timestamp']
        if (agora - cache_tempo).total_seconds() < CACHE_DURATION_SECONDS:
            return SHEET_CACHE[aba_name]['data']

    if not client: raise Exception("Cliente GSheets não autorizado.")
    
    try:
        sh = client.open_by_url(SHEET_URL)
        linhas = sh.worksheet(aba_name).get_all_records()
    except Exception as e:
        if aba_name in SHEET_CACHE: return SHEET_CACHE[aba_name]['data']
        raise e

    SHEET_CACHE[aba_name] = { 'data': linhas, 'timestamp': agora }
    return linhas

def get_sheet_data_future(aba_code):
    """Obtém dados da aba de cache de jogos futuros (sheet_future)."""

    aba_name = LIGAS_MAP[aba_code]['sheet_future']
    if not client: return []

    try:
        sh = client.open_by_url(SHEET_URL)
        linhas_raw = sh.worksheet(aba_name).get_all_values()
    except Exception as e:
        logging.error(f"Erro ao buscar cache de futuros jogos em {aba_name}: {e}")
        return []

    if not linhas_raw or len(linhas_raw) <= 1:
        return []

    data_rows = linhas_raw[1:]

    jogos = []
    for row in data_rows:
        if len(row) >= 4:
            jogos.append({
                "Mandante_Nome": row[0],
                "Visitante_Nome": row[1],
                "Data_Hora": row[2],
                "Matchday": safe_int(row[3])
            })

    return jogos

async def pre_carregar_cache_sheets():
    """Pré-carrega o histórico de todas as ligas (rodado uma vez na inicialização)."""
    if not client:
        logging.warning("Pré-carregamento de cache ignorado: Conexão GSheets falhou.")
        return

    logging.info("Iniciando pré-carregamento de cache...")
    for aba in ABAS_PASSADO:
        try:
            get_sheet_data(aba)
            logging.info(f"Cache de histórico para {aba} pré-carregado.")
        except Exception as e:
            logging.warning(f"Não foi possível pré-carregar cache para {aba}: {e}")
        await asyncio.sleep(1)

# =================================================================================
# 🎯 FUNÇÕES DE API E ATUALIZAÇÃO 
# =================================================================================
def buscar_jogos(league_code, status_filter):
    """Busca jogos na API com filtro de status (usado para FINISHED e ALL)."""
    try:
        url = f"https://api.football-data.org/v4/competitions/{league_code}/matches"

        if status_filter != "ALL":
             url += f"?status={status_filter}"

        r = requests.get(
            url,
            headers={"X-Auth-Token": API_KEY}, timeout=10
        )
        r.raise_for_status()
    except Exception as e:
        logging.error(f"Erro ao buscar jogos {status_filter} para {league_code}: {e}")
        return []

    all_matches = r.json().get("matches", [])

    if status_filter == "ALL":
        # Garante que apenas jogos agendados ou cronometrados (futuros) sejam retornados.
        return [m for m in all_matches if m.get('status') in ['SCHEDULED', 'TIMED']]

    else:
        # Lógica original para jogos FINISHED
        jogos = []
        for m in all_matches:
            if m.get('status') == "FINISHED":
                try:
                    jogo_data = datetime.strptime(m['utcDate'][:10], "%Y-%m-%d")
                    ft = m.get("score", {}).get("fullTime", {})
                    ht = m.get("score", {}).get("halfTime", {})
                    if ft.get("home") is None: continue

                    gm, gv = ft.get("home",0), ft.get("away",0)
                    gm1, gv1 = ht.get("home",0), ht.get("away",0)

                    jogos.append({
                        "Mandante": m.get("homeTeam", {}).get("name", ""),
                        "Visitante": m.get("awayTeam", {}).get("name", ""),
                        "Gols Mandante": gm, "Gols Visitante": gv,
                        "Gols Mandante 1T": gm1, "Gols Visitante 1T": gv1,
                        "Gols Mandante 2T": gm - gm1, "Gols Visitante 2T": gv - gv1,
                        "Data": jogo_data.strftime("%d/%m/%Y")
                    })
                except: continue
        return sorted(jogos, key=lambda x: datetime.strptime(x['Data'], "%d/%m/%Y"))

def buscar_jogos_live(league_code):
    """Busca jogos AO VIVO (IN_PLAY, HALF_TIME, PAUSED) buscando todos os jogos do dia na API."""
    hoje_utc = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    try:
        # Busca todos os jogos da liga que ocorrem na data de hoje
        url = f"https://api.football-data.org/v4/competitions/{league_code}/matches?dateFrom={hoje_utc}&dateTo={hoje_utc}"

        r = requests.get(
            url,
            headers={"X-Auth-Token": API_KEY}, timeout=10
        )
        r.raise_for_status()
    except Exception as e:
        logging.error(f"Erro ao buscar jogos AO VIVO (busca por data) para {league_code}: {e}")
        return []

    all_matches = r.json().get("matches", [])

    jogos = []
    for m in all_matches:
        status_api = m.get('status')
        # Filtra manualmente apenas os status que representam um jogo ativo
        if status_api in LIVE_STATUSES:
            try:
                ft_score = m.get("score", {}).get("fullTime", {})

                gm_atual = ft_score.get("home") if ft_score.get("home") is not None else 0
                gv_atual = ft_score.get("away") if ft_score.get("away") is not None else 0

                minute = m.get("minute", "N/A")

                if status_api in ['PAUSED', 'HALF_TIME']:
                    minute = status_api # Mostra o status exato (e.g. HALF_TIME)
                elif status_api == "IN_PLAY":
                    # Tentativa de obter o minuto, se não vier, infere o tempo
                    if minute == "N/A":
                        if m.get("score", {}).get("duration", "") == "REGULAR":
                            minute = "2ºT"
                        else:
                            minute = "1ºT"

                jogos.append({
                    "Mandante_Nome": m.get("homeTeam", {}).get("name", ""),
                    "Visitante_Nome": m.get("awayTeam", {}).get("name", ""),
                    "Placar_Mandante": gm_atual,
                    "Placar_Visitante": gv_atual,
                    "Tempo_Jogo": minute,
                    "Matchday": safe_int(m.get("matchday", 0))
                })
            except: continue

    return jogos

async def atualizar_planilhas(context: ContextTypes.DEFAULT_TYPE):
    """Atualiza o histórico e o cache de futuros jogos. Função para o JobQueue."""
    global SHEET_CACHE

    if not client:
        logging.error("Atualização de planilhas ignorada: Cliente GSheets não autorizado.")
        return
        
    try: sh = client.open_by_url(SHEET_URL)
    except:
        logging.error("Erro ao abrir planilha para atualização.")
        return

    logging.info("Iniciando a atualização periódica das planilhas...")

    for aba_code, aba_config in LIGAS_MAP.items():
        # 1. ATUALIZAÇÃO DO HISTÓRICO (ABA_PASSADO)
        aba_past = aba_config['sheet_past']
        try: ws_past = sh.worksheet(aba_past)
        except: continue

        jogos_finished = buscar_jogos(aba_code, "FINISHED")
        await asyncio.sleep(10) # Pausa para respeitar limite de rate da API

        if jogos_finished:
            try:
                exist = ws_past.get_all_records()
                keys_exist = {(r['Mandante'], r['Visitante'], r['Data']) for r in exist}

                novas_linhas = []
                for j in jogos_finished:
                    key = (j["Mandante"], j["Visitante"], j["Data"])
                    if key not in keys_exist:
                        novas_linhas.append([
                            j["Mandante"], j["Visitante"], j["Gols Mandante"], j["Gols Visitante"],
                            j["Gols Mandante 1T"], j["Gols Visitante 1T"],
                            j["Gols Mandante 2T"], j["Gols Visitante 2T"], j["Data"]
                        ])

                if novas_linhas:
                    ws_past.append_rows(novas_linhas)
                    logging.info(f"✅ {len(novas_linhas)} jogos adicionados ao histórico de {aba_past}.")

                if aba_past in SHEET_CACHE: del SHEET_CACHE[aba_past]
      
            except Exception as e:
                logging.error(f"Erro ao inserir dados na planilha {aba_past}: {e}")

        # 2. ATUALIZAÇÃO DO CACHE DE FUTUROS JOGOS (ABA_FUTURE)
        aba_future = aba_config['sheet_future']
        try: ws_future = sh.worksheet(aba_future)
        except:
            logging.warning(f"Aba de futuros jogos '{aba_future}' não encontrada.")
            continue

        jogos_future = buscar_jogos(aba_code, "ALL")
        await asyncio.sleep(10) # Pausa para respeitar limite de rate da API

        try:
            ws_future.clear()
            ws_future.update(values=[['Mandante', 'Visitante', 'Data/Hora', 'Matchday']], range_name='A1:D1')

            if jogos_future:
                linhas_future = []

                for m in jogos_future:
                    matchday = m.get("matchday", "")
                    utc_date = m.get('utcDate', '')
    
                    if utc_date:
                        try:
                            data_utc = datetime.strptime(utc_date[:16], '%Y-%m-%dT%H:%M')
                            # Limita a busca a jogos de até 90 dias no futuro
                            if data_utc < datetime.now() + timedelta(days=90):
                                linhas_future.append([
                                    m.get("homeTeam", {}).get("name"),
                                    m.get("awayTeam", {}).get("name"),
                                    utc_date,
                                    matchday
                                ])
                        except:
                            continue

                if linhas_future:
                    ws_future.append_rows(linhas_future, value_input_option='USER_ENTERED')
                    logging.info(f"✅ {len(linhas_future)} jogos futuros atualizados no cache de {aba_future}.")
                else:
                    logging.info(f"⚠️ Nenhuma partida agendada para {aba_code}. Cache {aba_future} limpo.")

        except Exception as e:
            logging.error(f"Erro ao atualizar cache de futuros jogos em {aba_future}: {e}")

        await asyncio.sleep(3) # Pausa entre ligas

# =================================================================================
# 📈 FUNÇÕES DE CÁLCULO E FORMATAÇÃO DE ESTATÍSTICAS
# =================================================================================
def calcular_estatisticas_time(time, aba, ultimos=None, casa_fora=None):
    """Calcula estatísticas detalhadas para um time em uma liga."""

    # Dicionário de resultados (Inicialização completa e detalhada)
    d = {"time":time,"jogos_time":0,"jogos_casa":0,"jogos_fora":0,
         "over15":0,"over15_casa":0,"over15_fora":0, "over25":0,"over25_casa":0,"over25_fora":0,
         "btts":0,"btts_casa":0,"btts_fora":0, "over05_1T":0,"over05_1T_casa":0,"over05_1T_fora":0,
         "over05_2T":0,"over05_2T_casa":0,"over05_2T_fora":0, "over15_2T":0,"over15_2T_casa":0,"over15_2T_fora":0,
         "gols_marcados":0,"gols_sofridos":0, "gols_marcados_casa":0,"gols_sofridos_casa":0,
         "gols_marcados_fora":0,"gols_sofridos_fora":0, "total_gols":0,"total_gols_casa":0,"total_gols_fora":0,
         "gols_marcados_1T":0,"gols_sofridos_1T":0, "gols_marcados_2T":0,"gols_sofridos_2T":0,
         "gols_marcados_1T_casa":0,"gols_sofridos_1T_casa":0, "gols_marcados_1T_fora":0,"gols_sofridos_1T_fora":0,
         "gols_marcados_2T_casa":0,"gols_sofridos_2T_casa":0, "gols_marcados_2T_fora":0,"gols_sofridos_2T_fora":0}

    try:
        linhas = get_sheet_data(aba)
    except:
        return {"time":time, "jogos_time": 0}

    # Aplica filtro casa/fora
    if casa_fora=="casa":
        linhas = [l for l in linhas if l['Mandante']==time]
    elif casa_fora=="fora":
        linhas = [l for l in linhas if l['Visitante']==time]
    else:
        linhas = [l for l in linhas if l['Mandante']==time or l['Visitante']==time]

    # Ordena e filtra os N últimos jogos
    try:
        linhas.sort(key=lambda x: datetime.strptime(x['Data'], "%d/%m/%Y"), reverse=False)
    except: pass

    if ultimos:
        linhas = linhas[-ultimos:]

    for linha in linhas:
        em_casa = (time == linha['Mandante'])
        gm, gv = safe_int(linha['Gols Mandante']), safe_int(linha['Gols Visitante'])
        gm1, gv1 = safe_int(linha['Gols Mandante 1T']), safe_int(linha['Gols Visitante 1T'])
        gm2, gv2 = gm-gm1, gv-gv1

        total, total1, total2 = gm+gv, gm1+gv1, gm2+gv2
        d["jogos_time"] += 1

        if em_casa:
            marcados, sofridos = gm, gv
            d["jogos_casa"] += 1
            d["gols_marcados_1T_casa"] += gm1
            d["gols_sofridos_1T_casa"] += gv1
            d["gols_marcados_2T_casa"] += gm2
            d["gols_sofridos_2T_casa"] += gv2
        else:
            marcados, sofridos = gv, gm
            d["jogos_fora"] += 1
            d["gols_marcados_1T_fora"] += gv1
            d["gols_sofridos_1T_fora"] += gm1
            d["gols_marcados_2T_fora"] += gv2
            d["gols_sofridos_2T_fora"] += gm2

        d["gols_marcados"] += marcados
        d["gols_sofridos"] += sofridos
        if em_casa:
            d["gols_marcados_casa"] += marcados
            d["gols_sofridos_casa"] += sofridos
        else:
            d["gols_marcados_fora"] += marcados
            d["gols_sofridos_fora"] += sofridos

        d["total_gols"] += total
        if em_casa: d["total_gols_casa"] += total
        else: d["total_gols_fora"] += total

        if total>1.5: d["over15"] += 1
        if total>2.5: d["over25"] += 1
        if gm>0 and gv>0: d["btts"] += 1
        if total1>0.5: d["over05_1T"] += 1
        if total2>0.5: d["over05_2T"] += 1
        if total2>1.5: d["over15_2T"] += 1

        # Estatísticas por condição (casa/fora)
        d["over15_casa" if em_casa else "over15_fora"] += (1 if total > 1.5 else 0)
        d["over25_casa" if em_casa else "over25_fora"] += (1 if total > 2.5 else 0)
        d["btts_casa" if em_casa else "btts_fora"] += (1 if gm > 0 and gv > 0 else 0)
        d["over05_1T_casa" if em_casa else "over05_1T_fora"] += (1 if total1 > 0.5 else 0)
        d["over05_2T_casa" if em_casa else "over05_2T_fora"] += (1 if total2 > 0.5 else 0)
        d["over15_2T_casa" if em_casa else "over15_2T_fora"] += (1 if total2 > 1.5 else 0)

        d["gols_marcados_1T"] += gm1 if em_casa else gv1
        d["gols_sofridos_1T"] += gv1 if em_casa else gm1
        d["gols_marcados_2T"] += gm2 if em_casa else gv2
        d["gols_sofridos_2T"] += gv2 if em_casa else gm2

    return d

def formatar_estatisticas(d):
    """Formata o dicionário de estatísticas para a mensagem do Telegram."""
    jt, jc, jf = d["jogos_time"], d.get("jogos_casa", 0), d.get("jogos_fora", 0)

    if jt == 0: return f"⚠️ **Nenhum jogo encontrado** para **{escape_markdown(d['time'])}** com o filtro selecionado."
    
    return (f"📊 **Estatísticas - {escape_markdown(d['time'])}**\n"
            f"📅 Jogos: {jt} (Casa: {jc} | Fora: {jf})\n\n"
            f"⚽ Over 1.5: **{pct(d['over15'], jt)}** (C: {pct(d.get('over15_casa',0), jc)} | F: {pct(d.get('over15_fora',0), jf)})\n"
            f"⚽ Over 2.5: **{pct(d['over25'], jt)}** (C: {pct(d.get('over25_casa',0), jc)} | F: {pct(d.get('over25_fora',0), jf)})\n"
            f"🔁 BTTS: **{pct(d['btts'], jt)}** (C: {pct(d.get('btts_casa',0), jc)} | F: {pct(d.get('btts_fora',0), jf)})\n\n"
            
            f"⏱️ 1ºT Over 0.5: {pct(d['over05_1T'], jt)} (C: {pct(d['over05_1T_casa'], jc)} | F: {pct(d['over05_1T_fora'], jf)})\n"
            f"⏱️ 2ºT Over 0.5: {pct(d['over05_2T'], jt)} (C: {pct(d['over05_2T_casa'], jc)} | F: {pct(d['over05_2T_fora'], jf)})\n"
            f"⏱️ 2ºT Over 1.5: {pct(d['over15_2T'], jt)} (C: {pct(d['over15_2T_casa'], jc)} | F: {pct(d['over15_2T_fora'], jf)})\n\n"
            
            f"➕ **Média gols marcados:** {media(d['gols_marcados'], jt)} (C: {media(d.get('gols_marcados_casa',0), jc)} | F: {media(d.get('gols_marcados_fora',0), jf)})\n"
            f"➖ **Média gols sofridos:** {media(d['gols_sofridos'], jt)} (C: {media(d.get('gols_sofridos_casa',0), jc)} | F: {media(d.get('gols_sofridos_fora',0), jf)})\n\n"

            f"⏱️ Média gols 1ºT (GP/GC): {media(d['gols_marcados_1T'], jt)} / {media(d['gols_sofridos_1T'], jt)}\n"
            f"⏱️ Média gols 2ºT (GP/GC): {media(d['gols_marcados_2T'], jt)} / {media(d['gols_sofridos_2T'], jt)}\n\n"
            
            f"🔢 **Média total de gols:** {media(d['total_gols'], jt)} (C: {media(d.get('total_gols_casa',0), jc)} | F: {media(d.get('total_gols_fora',0), jf)})"
    )

def listar_ultimos_jogos(time, aba, ultimos=None, casa_fora=None):
    """Lista os últimos N jogos de um time com filtros."""
    try: linhas = get_sheet_data(aba)
    except: return f"⚠️ Erro ao ler dados da planilha para {escape_markdown(time)}."

    if casa_fora == "casa":
        linhas = [l for l in linhas if l['Mandante'] == time]
    elif casa_fora == "fora":
        linhas = [l for l in linhas if l['Visitante'] == time]
    else:
        linhas = [l for l in linhas if l['Mandante'] == time or l['Visitante'] == time]

    try: linhas.sort(key=lambda x: datetime.strptime(x['Data'], "%d/%m/%Y"), reverse=False)
    except: pass

    if ultimos:
        linhas = linhas[-ultimos:]

    if not linhas: return f"Nenhum jogo encontrado para **{escape_markdown(time)}** com o filtro selecionado."

    texto_jogos = ""
    for l in linhas:
        data = l['Data']
        gm, gv = safe_int(l['Gols Mandante']), safe_int(l['Gols Visitante'])

        if l['Mandante'] == time:
            oponente = escape_markdown(l['Visitante'])
            condicao = "(CASA)"
            m_cor = "🟢" if gm > gv else ("🟡" if gm == gv else "🔴")
            texto_jogos += f"{m_cor} {data} {condicao}: **{escape_markdown(time)}** {gm} x {gv} {oponente}\n"
        else:
            oponente = escape_markdown(l['Mandante'])
            condicao = "(FORA)"
            m_cor = "🟢" if gv > gm else ("🟡" if gv == gm else "🔴")
            texto_jogos += f"{m_cor} {data} {condicao}: {oponente} {gm} x {gv} **{escape_markdown(time)}**\n"

    return texto_jogos

# =================================================================================
# 🤖 FUNÇÕES DO BOT: HANDLERS E FLUXOS
# =================================================================================
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "👋 Bem-vindo ao **Bot de Estatísticas de Confronto**!\n\n"
        "Selecione um comando para começar:\n"
        "• **/stats** 📊: Inicia a análise estatística de um confronto futuro ou ao vivo."
    )
    await update.message.reply_text(text, parse_mode='Markdown')

async def listar_competicoes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Primeira tela: Lista todas as competições."""
    title = "📊 **Estatísticas de Confronto:** Escolha a Competição:"

    keyboard = []
    abas_list = list(LIGAS_MAP.keys())
    for i in range(0, len(abas_list), 3):
        row = []
        for aba in abas_list[i:i + 3]:
            # Callback: c:LIGA
            row.append(InlineKeyboardButton(aba, callback_data=f"c:{aba}"))
        keyboard.append(row)

    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.message:
        await update.message.reply_text(title, reply_markup=reply_markup, parse_mode='Markdown')
    else:
        # Se for um callback, edita a mensagem anterior
        try:
            await update.callback_query.edit_message_text(title, reply_markup=reply_markup, parse_mode='Markdown')
        except BadRequest:
             # Fallback: Se a edição falhar, envia nova mensagem
             await update.callback_query.message.reply_text(title, reply_markup=reply_markup, parse_mode='Markdown')


async def mostrar_menu_status_jogo(update: Update, context: ContextTypes.DEFAULT_TYPE, aba_code: str):
    """
    Segundo menu: Escolhe entre Jogos AO VIVO e Próximos Jogos (Future).
    FIX CRÍTICO: Adicionado bloco try/except para garantir que a navegação não pare ao falhar na edição.
    """

    title = f"**{aba_code}** - Escolha o Tipo de Partida:"

    keyboard = [
        # Callback: STATUS|LIVE|aba_code
        [InlineKeyboardButton("🔴 AO VIVO (API)", callback_data=f"STATUS|LIVE|{aba_code}")],
        # Callback: STATUS|FUTURE|aba_code
        [InlineKeyboardButton("📅 PRÓXIMOS JOGOS (Planilha)", callback_data=f"STATUS|FUTURE|{aba_code}")],
        [InlineKeyboardButton("⬅️ Voltar para Ligas", callback_data="VOLTAR_LIGA")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    try:
        # Tenta editar a mensagem existente (o comportamento padrão de um callback)
        await update.callback_query.edit_message_text(title, reply_markup=reply_markup, parse_mode='Markdown')
    except Exception as e:
        logging.error(f"ERRO ao editar mensagem em mostrar_menu_status_jogo (c:{aba_code}): {e}")
        # Se a edição falhar (e.g., mensagem muito antiga, BadRequest), envia uma nova mensagem como fallback
        await update.callback_query.message.reply_text(
            f"**{aba_code}** - Escolha o Tipo de Partida:",
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )

async def listar_jogos(update: Update, context: ContextTypes.DEFAULT_TYPE, aba_code: str, status: str):
    """Terceira tela: Lista jogos futuros (GSheets) ou ao vivo (API)."""

    jogos_a_listar = []

    if status == "FUTURE":
        try:
            await update.callback_query.edit_message_text(
                f"⏳ Buscando os próximos **{MAX_GAMES_LISTED}** jogos em **{aba_code}** (Planilha)...",
                parse_mode='Markdown'
            )
        except Exception as e:
            logging.error(f"Erro ao editar mensagem de loading FUTURE: {e}")
            pass # Continua o fluxo

        jogos_agendados = get_sheet_data_future(aba_code)

        # Filtra jogos cuja data/hora já passou
        jogos_futuros_filtrados = []
        agora_utc = datetime.now(timezone.utc).replace(tzinfo=None)
        for jogo in jogos_agendados:
            try:
                data_utc = datetime.strptime(jogo['Data_Hora'][:16], '%Y-%m-%dT%H:%M')
                if data_utc > agora_utc:
                    jogos_futuros_filtrados.append(jogo)
            except Exception as e:
                logging.warning(f"Erro ao parsear data de jogo futuro: {e}")
                continue
        jogos_agendados = jogos_futuros_filtrados

        if not jogos_agendados:
            await update.callback_query.edit_message_text(
                f"⚠️ **Nenhum jogo agendado futuro** encontrado em **{aba_code}**.\n"
                f"O Bot de atualização roda a cada 1 hora.",
                parse_mode='Markdown'
            )
            keyboard = [[InlineKeyboardButton("⬅️ Voltar para Status", callback_data=f"VOLTAR_LIGA_STATUS|{aba_code}")]]
            await update.effective_message.reply_text("Opções:", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
            return

        jogos_a_listar = jogos_agendados[:MAX_GAMES_LISTED]
        total_jogos_encontrados = len(jogos_agendados)
        matchday_label = f"Próximos {len(jogos_a_listar)} jogos (de {total_jogos_encontrados} no cache)"

        # CRIAÇÃO DOS BOTÕES PARA FUTURE
        keyboard = []
        for jogo in jogos_a_listar:
            try:
                M_full = jogo['Mandante_Nome']
                V_full = jogo['Visitante_Nome']
                data_str = jogo['Data_Hora']
                
                try:
                    data_utc = datetime.strptime(data_str[:16], '%Y-%m-%dT%H:%M')
                    matchday_num = jogo.get('Matchday', "N/A")
                    # Converção para fuso horário local (-3h)
                    data_local = data_utc - timedelta(hours=3) 
                    data_label = data_local.strftime('%d/%m %H:%M')
                except ValueError:
                    data_label = data_str 
                    matchday_num = "N/A"

                M_safe = escape_markdown(M_full)
                V_safe = escape_markdown(V_full)
                
                label = f"({matchday_num}) {data_label} | {M_safe} x {V_safe}"
                callback_data = f"JOGO|{aba_code}|{M_safe}|{V_safe}"
                
                # Trata limite de 64 bytes para callback_data
                if len(callback_data.encode('utf-8')) > 64: 
                    M_safe_short = M_full.split(' ')[0][:8]
                    V_safe_short = V_full.split(' ')[0][:8]
                    callback_data = f"JOGO|{aba_code}|{M_safe_short}|{V_safe_short}"
                    label = f"({matchday_num}) {data_label} | {M_safe_short} x {V_safe_short}"
                    
                keyboard.append([InlineKeyboardButton(label, callback_data=callback_data)])
            except Exception as e:
                logging.error(f"Erro ao processar jogo FUTURE: {e}")
                continue

    elif status == "LIVE":
        try:
            await update.callback_query.edit_message_text(
                f"⏳ Buscando jogos **AO VIVO** (IN_PLAY, INTERVALO) em **{aba_code}** (API)...",
                parse_mode='Markdown'
            )
        except Exception as e:
            logging.error(f"Erro ao editar mensagem de loading LIVE: {e}")
            pass # Continua o fluxo

        jogos_a_listar = buscar_jogos_live(aba_code)

        if not jogos_a_listar:
            await update.callback_query.edit_message_text(
                f"⚠️ **Nenhum jogo AO VIVO** encontrado em **{aba_code}** no momento.",
                parse_mode='Markdown'
            )
            # Voltar para o menu de Status (LIVE/FUTURE)
            keyboard = [[InlineKeyboardButton("⬅️ Voltar para Status", callback_data=f"VOLTAR_LIGA_STATUS|{aba_code}")]]
            await update.effective_message.reply_text("Opções:", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
            return

        matchday_label = f"{len(jogos_a_listar)} jogos AO VIVO"

        # CRIAÇÃO DOS BOTÕES PARA LIVE
        keyboard = []
        for jogo in jogos_a_listar:
            M_full = jogo['Mandante_Nome']
            V_full = jogo['Visitante_Nome']
            placar_m = jogo['Placar_Mandante']
            placar_v = jogo['Placar_Visitante']
            tempo = jogo['Tempo_Jogo']

            M_safe = escape_markdown(M_full)
            V_safe = escape_markdown(V_full)
            
            label = f"🔴 {tempo} | {M_safe} {placar_m} x {placar_v} {V_safe}"
            callback_data = f"JOGO|{aba_code}|{M_safe}|{V_safe}"
            
            # Trata limite de 64 bytes para callback_data
            if len(callback_data.encode('utf-8')) > 64:
                M_safe_short = M_full.split(' ')[0][:8]
                V_safe_short = V_full.split(' ')[0][:8]
                callback_data = f"JOGO|{aba_code}|{M_safe_short}|{V_safe_short}"
                label = f"🔴 {tempo} | {M_safe_short} {placar_m} x {placar_v} {V_safe_short}"

            keyboard.append([InlineKeyboardButton(label, callback_data=callback_data)])
    
    # Botão de voltar ajustado para voltar ao Menu de Status
    keyboard.append([InlineKeyboardButton("⬅️ Voltar para Status", callback_data=f"VOLTAR_LIGA_STATUS|{aba_code}")])

    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(
        f"**SELECIONE A PARTIDA** ({aba_code} - **{matchday_label}**):",
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )

async def mostrar_menu_acoes(update: Update, context: ContextTypes.DEFAULT_TYPE, aba_code: str, mandante: str, visitante: str):
    """Quarta tela: Menu para escolher entre Estatísticas e Últimos Jogos."""
    m_sanitized = escape_markdown(mandante)
    v_sanitized = escape_markdown(visitante)
    
    # Salva o jogo para facilitar o acesso nos próximos filtros
    context.user_data["current_match_info"] = {"aba": aba_code, "mandante": mandante, "visitante": visitante}

    keyboard = [
        # Callback: AÇÃO|STATS|LIGA|MANDANTE|VISITANTE
        [InlineKeyboardButton("📊 Estatísticas Detalhadas", callback_data=f"AÇÃO|STATS|{aba_code}|{m_sanitized}|{v_sanitized}")],
        # Callback: AÇÃO|RESULT|LIGA|MANDANTE|VISITANTE (usa filtro 0 default)
        [InlineKeyboardButton("📜 Últimos Resultados", callback_data=f"AÇÃO|RESULT|{aba_code}|{m_sanitized}|{v_sanitized}")],
        # Voltar para a lista de jogos (dependendo do status)
        [InlineKeyboardButton("⬅️ Voltar para Jogos da Liga", callback_data=f"VOLTAR_LIGA_STATUS|{aba_code}")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.callback_query.edit_message_text(
        f"Jogo selecionado: **{m_sanitized} x {v_sanitized}**\n\nEscolha a **ação**:",
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )

async def mostrar_menu_filtros_confronto(update: Update, context: ContextTypes.DEFAULT_TYPE, tipo_acao: str, aba_code: str, mandante: str, visitante: str):
    """Quinta tela: Menu de filtros (Geral ou Casa vs Fora) para STATS/RESULT."""
    m_sanitized = escape_markdown(mandante)
    v_sanitized = escape_markdown(visitante)

    keyboard = []
    for idx, (nome, _, _, _) in enumerate(CONFRONTO_FILTROS):
        # Callback: FILTRO|ACAO|LIGA|MANDANTE|VISITANTE|INDEX_FILTRO
        callback_data = f"FILTRO|{tipo_acao}|{aba_code}|{m_sanitized}|{v_sanitized}|{idx}"
        keyboard.append([InlineKeyboardButton(f"📊 {nome}", callback_data=callback_data)])
        
    # Voltar para o Menu de Ações (Estatísticas/Resultados)
    keyboard.append([InlineKeyboardButton("⬅️ Voltar para Ações", callback_data=f"AÇÃO_MENU|{aba_code}|{m_sanitized}|{v_sanitized}")])
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.callback_query.edit_message_text(
        f"Escolha o **Filtro de Confronto** para **{m_sanitized} x {v_sanitized}**:",
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )

async def exibir_estatisticas(update: Update, context: ContextTypes.DEFAULT_TYPE, mandante: str, visitante: str, aba_code: str, filtro_idx: int):
    """Exibe estatísticas detalhadas para Mandante e Visitante."""
    if not (0 <= filtro_idx < len(CONFRONTO_FILTROS)): return

    nome_filtro, ultimos, condicao_m, condicao_v = CONFRONTO_FILTROS[filtro_idx]

    m_sanitized = escape_markdown(mandante)
    v_sanitized = escape_markdown(visitante)

    await update.callback_query.edit_message_text(
        f"⏳ Buscando estatísticas para **{m_sanitized} x {v_sanitized}** (Filtro: **{nome_filtro}**)...",
        parse_mode='Markdown'
    )

    # 1. Estatísticas do Mandante
    estat_m = calcular_estatisticas_time(mandante, aba_code, ultimos, condicao_m)
    texto_m = formatar_estatisticas(estat_m)

    # 2. Estatísticas do Visitante
    estat_v = calcular_estatisticas_time(visitante, aba_code, ultimos, condicao_v)
    texto_v = formatar_estatisticas(estat_v)

    mensagem_final = (
        f"**CONFRONTO: {m_sanitized} x {v_sanitized}**\n"
        f"Filtro: **{nome_filtro}**\n\n"
        f"--- {m_sanitized} (Mandante) ---\n{texto_m}\n\n"
        f"--- {v_sanitized} (Visitante) ---\n{texto_v}"
    )

    # Cria o menu de filtros de volta
    keyboard = []
    for idx, (nome, _, _, _) in enumerate(CONFRONTO_FILTROS):
        callback_data = f"FILTRO|STATS|{aba_code}|{m_sanitized}|{v_sanitized}|{idx}"
        keyboard.append([InlineKeyboardButton(f"📊 {nome}", callback_data=callback_data)])
        
    keyboard.append([InlineKeyboardButton("⬅️ Voltar para Ações", callback_data=f"AÇÃO_MENU|{aba_code}|{m_sanitized}|{v_sanitized}")])
    reply_markup = InlineKeyboardMarkup(keyboard)

    # Edita a mensagem com o resultado final
    await update.callback_query.edit_message_text(
        mensagem_final,
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )

async def exibir_ultimos_resultados(update: Update, context: ContextTypes.DEFAULT_TYPE, mandante: str, visitante: str, aba_code: str, filtro_idx: int):
    """Exibe os últimos resultados do Mandante e Visitante."""
    if not (0 <= filtro_idx < len(CONFRONTO_FILTROS)): return

    nome_filtro, ultimos, condicao_m, condicao_v = CONFRONTO_FILTROS[filtro_idx]

    m_sanitized = escape_markdown(mandante)
    v_sanitized = escape_markdown(visitante)

    await update.callback_query.edit_message_text(
        f"⏳ Buscando últimos resultados para **{m_sanitized} x {v_sanitized}** (Filtro: **{nome_filtro}**)...",
        parse_mode='Markdown'
    )

    # 1. Últimos jogos do Mandante
    texto_m = listar_ultimos_jogos(mandante, aba_code, ultimos, condicao_m)

    # 2. Últimos jogos do Visitante
    texto_v = listar_ultimos_jogos(visitante, aba_code, ultimos, condicao_v)

    mensagem_final = (
        f"**ÚLTIMOS RESULTADOS: {m_sanitized} x {v_sanitized}**\n"
        f"Filtro: **{nome_filtro}**\n\n"
        f"--- {m_sanitized} (Mandante) ---\n{texto_m}\n\n"
        f"--- {v_sanitized} (Visitante) ---\n{texto_v}"
    )

    # Cria o menu de filtros de volta
    keyboard = []
    for idx, (nome, _, _, _) in enumerate(CONFRONTO_FILTROS):
        callback_data = f"FILTRO|RESULT|{aba_code}|{m_sanitized}|{v_sanitized}|{idx}"
        keyboard.append([InlineKeyboardButton(f"📜 {nome}", callback_data=callback_data)])
        
    keyboard.append([InlineKeyboardButton("⬅️ Voltar para Ações", callback_data=f"AÇÃO_MENU|{aba_code}|{m_sanitized}|{v_sanitized}")])
    reply_markup = InlineKeyboardMarkup(keyboard)

    # Edita a mensagem com o resultado final
    await update.callback_query.edit_message_text(
        mensagem_final,
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )

async def callback_query_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Função central de roteamento de callbacks."""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    
    try:
        # Usa o primeiro segmento como tipo de comando
        tipo_comando, *args = data.split("|")
        
        # --- FLUXO DE NAVEGAÇÃO ---
        if tipo_comando == "c": # Ex: c:BSA
            liga = args[0]
            await mostrar_menu_status_jogo(update, context, liga)
            
        elif tipo_comando == "STATUS": # Ex: STATUS|LIVE|BSA
            status_tipo, liga = args
            await listar_jogos(update, context, liga, status_tipo)
            
        elif tipo_comando == "JOGO": # Ex: JOGO|BSA|Time Mandante|Time Visitante
            liga, mandante, visitante = args
            await mostrar_menu_acoes(update, context, liga, mandante, visitante)
        
        elif tipo_comando == "AÇÃO": # Ex: AÇÃO|STATS|BSA|M|V
            acao_tipo, liga, mandante, visitante = args
            
            if acao_tipo == "STATS":
                await mostrar_menu_filtros_confronto(update, context, "STATS", liga, mandante, visitante)
            elif acao_tipo == "RESULT":
                # Para Últimos Resultados, pula o menu de filtros e usa o filtro 0 (Geral)
                await exibir_ultimos_resultados(update, context, mandante, visitante, liga, 0)

        elif tipo_comando == "FILTRO": # Ex: FILTRO|STATS|BSA|M|V|0
            acao_tipo, liga, mandante, visitante, filtro_idx_str = args
            filtro_idx = safe_int(filtro_idx_str)
            
            if acao_tipo == "STATS":
                await exibir_estatisticas(update, context, mandante, visitante, liga, filtro_idx)
            elif acao_tipo == "RESULT":
                await exibir_ultimos_resultados(update, context, mandante, visitante, liga, filtro_idx)

        # --- FLUXOS DE VOLTAR ---
        elif tipo_comando == "VOLTAR_LIGA":
            await listar_competicoes(update, context) 

        elif tipo_comando == "VOLTAR_LIGA_STATUS": # Ex: VOLTAR_LIGA_STATUS|BSA
            liga = args[0]
            await mostrar_menu_status_jogo(update, context, liga)

        elif tipo_comando == "AÇÃO_MENU": # Ex: AÇÃO_MENU|BSA|M|V
             liga, mandante, visitante = args
             await mostrar_menu_acoes(update, context, liga, mandante, visitante)

        else:
            await query.edit_message_text(f"Comando desconhecido: {data}")
            
    except Exception as e:
        logging.error(f"Erro no processamento do callback ({data}): {e}")
        # Envia uma mensagem de erro ao usuário se a exceção for crítica
        await query.edit_message_text(f"Ocorreu um erro ao processar sua solicitação: {e}. Verifique os logs do Railway.")


# =================================================================================
# ⚙️ FUNÇÃO PRINCIPAL (Polling)
# =================================================================================
def main():
    try:
        if not BOT_TOKEN or BOT_TOKEN == "SEU_TOKEN_AQUI":
            logging.error("O BOT_TOKEN não foi configurado. Configure no Railway.")
            return

        # 1. Cria e inicia o JobQueue
        job_queue = JobQueue() 
        application = ApplicationBuilder().token(BOT_TOKEN).job_queue(job_queue).build()
        job_queue.start() 

        # Handlers
        application.add_handler(CommandHandler("start", start_command))
        application.add_handler(CommandHandler("stats", listar_competicoes))
        application.add_handler(CallbackQueryHandler(callback_query_handler))

        # Pré-carregar cache (síncrono, essencial para o primeiro uso)
        asyncio.run(pre_carregar_cache_sheets())
        
        # 4. Agenda a tarefa de atualização periódica (a cada CACHE_DURATION_SECONDS)
        application.job_queue.run_repeating(
            atualizar_planilhas,
            interval=CACHE_DURATION_SECONDS,
            first=10 # Inicia 10 segundos após o start para dar tempo do pré-cache
        )
        
        logging.info("Bot iniciado em modo Polling no Railway...")
        application.run_polling()
        
    except Exception as e:
        logging.error(f"Erro na execução principal: {e}")

if __name__ == '__main__':
    main()
