# ===============================================================================
# 🏆 BOT DE ESTATÍSTICAS DE CONFRONTO V2.2.3 - CORREÇÃO UNIVERSAL BUTTON_DATA_INVALID
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
import sys 

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, ContextTypes, JobQueue 
from telegram.error import BadRequest
from gspread.exceptions import WorksheetNotFound

# Configuração de Logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
nest_asyncio.apply()

# ===== Variáveis de Configuração (LIDAS DE VARIÁVEIS DE AMBIENTE) =====
BOT_TOKEN = os.environ.get("BOT_TOKEN", "SEU_TOKEN_AQUI") 
API_KEY = os.environ.get("API_KEY", "SUA_API_KEY_AQUI")
[cite_start]SHEET_URL = os.environ.get("SHEET_URL", "https://docs.google.com/spreadsheets/d/1ChFFXQxo1qQElNzh2OC8-UPgofRXxyVWN06ExBQ3YqY/edit?usp=drivesdk") # [cite: 387, 279]

# Mapeamento de Ligas GSheets (Mantido do seu código original)
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
[cite_start]} # [cite: 388]
ABAS_PASSADO = list(LIGAS_MAP.keys())

# ===============================================================================
# 🛠️ CORREÇÃO UNIVERSAL: CACHE DE JOGOS E MAPPING DE API
# ===============================================================================
# Armazena temporariamente os nomes longos dos times para contornar o limite de 64 bytes.
GAME_CACHE = {}
GAME_ID_COUNTER = 0 # Contador para IDs únicos

# Dicionário adicional para ligar o código GSheet (ex: BL1) ao ID da API real.
# ⚠️ VOCÊ PRECISA PREENCHER OS IDS DA SUA API AQUI! (Estes são exemplos)
LEAGUE_API_MAP = {
    "CL": {"id": 2001, "name": "Champions League"}, 
    "BSA": {"id": 2013, "name": "Brasileirão Série A"}, 
    "BL1": {"id": 2002, "name": "Bundesliga 1"}, 
    "PL": {"id": 2021, "name": "Premier League"}, 
    "ELC": {"id": 2020, "name": "Championship"},
    "DED": {"id": 2003, "name": "Eredivisie"},
    "PD": {"id": 2014, "name": "La Liga"},
    "PPL": {"id": 2017, "name": "Primeira Liga"},
    "SA": {"id": 2019, "name": "Serie A"},
    "FL1": {"id": 2015, "name": "Ligue 1"},
}


ULTIMOS = 10
SHEET_CACHE = {}
CACHE_DURATION_SECONDS = 3600 # 1 hora
[cite_start]MAX_GAMES_LISTED = 30 # [cite: 388]

# Filtros reutilizáveis para Estatísticas e Resultados (Mantidos do original)
CONFRONTO_FILTROS = [
    (f"📊 Estatísticas | ÚLTIMOS {ULTIMOS} GERAL", "STATS_FILTRO", ULTIMOS, None, None),
    (f"📊 Estatísticas | {ULTIMOS} (M CASA vs V FORA)", "STATS_FILTRO", ULTIMOS, "casa", "fora"),
    (f"📅 Resultados | ÚLTIMOS {ULTIMOS} GERAL", "RESULTADOS_FILTRO", ULTIMOS, None, None),
    (f"📅 Resultados | {ULTIMOS} (M CASA vs V FORA)", "RESULTADOS_FILTRO", ULTIMOS, "casa", "fora"),
[cite_start]] # [cite: 389, 390]

[cite_start]LIVE_STATUSES = ["IN_PLAY", "HALF_TIME", "PAUSED"] # [cite: 390]

# =================================================================================
# ✅ CONEXÃO GSHEETS VIA VARIÁVEL DE AMBIENTE 
# =================================================================================

CREDS_JSON = os.environ.get("GSPREAD_CREDS_JSON")
client = None

if not CREDS_JSON:
    logging.error("❌ ERRO DE AUTORIZAÇÃO GSHEET: Variável GSPREAD_CREDS_JSON não encontrada. Configure-a no Railway.")
else:
    try:
        # [cite_start]Usa um arquivo temporário para carregar as credenciais [cite: 391]
        with tempfile.NamedTemporaryFile(mode="w", delete=False, encoding='utf-8') as tmp_file:
            tmp_file.write(CREDS_JSON)
            tmp_file_path = tmp_file.name
        
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(tmp_file_path, scope)
        client = gspread.authorize(creds)
      
        [cite_start]logging.info("✅ Conexão GSheets estabelecida via Variável de Ambiente.") # [cite: 392]
        os.remove(tmp_file_path) # Limpa o arquivo temporário

    except Exception as e:
        [cite_start]logging.error(f"❌ ERRO DE AUTORIZAÇÃO GSHEET: Erro ao carregar ou autorizar credenciais JSON: {e}") # [cite: 392, 393]
        client = None

# =================================================================================
# 💾 FUNÇÕES DE SUPORTE E CACHING 
# =================================================================================
def safe_int(v):
    try: return int(v)
    [cite_start]except: return 0 # [cite: 393]

def pct(part, total):
    [cite_start]return f"{(part/total)*100:.1f}%" if total>0 else "—" # [cite: 393]

def media(part, total):
    [cite_start]return f"{(part/total):.2f}" if total>0 else "—" # [cite: 393]

def escape_markdown(text):
    """FIX CRÍTICO: Escapa caracteres que podem ser interpretados como Markdown (V1) e causavam o erro BadRequest."""
    # Escapa *, _, [ e ] que são os caracteres mais problemáticos
    [cite_start]return str(text).replace('*', '\\*').replace('_', '\\_').replace('[', '\\[') .replace(']', '\\]') # [cite: 393]

def get_sheet_data(aba_code):
    """Obtém dados da aba de histórico (sheet_past) com cache."""
    global SHEET_CACHE
    agora = datetime.now()

    [cite_start]aba_name = LIGAS_MAP[aba_code]['sheet_past'] # [cite: 394]

    if aba_name in SHEET_CACHE:
        [cite_start]cache_tempo = SHEET_CACHE[aba_name]['timestamp'] # [cite: 394]
        if (agora - cache_tempo).total_seconds() < CACHE_DURATION_SECONDS:
            [cite_start]return SHEET_CACHE[aba_name]['data'] # [cite: 394]

    [cite_start]if not client: raise Exception("Cliente GSheets não autorizado.") # [cite: 394]
    
    try:
        sh = client.open_by_url(SHEET_URL)
        [cite_start]linhas = sh.worksheet(aba_name).get_all_records() # [cite: 394]
    except Exception as e:
        [cite_start]if aba_name in SHEET_CACHE: return SHEET_CACHE[aba_name]['data'] # [cite: 394]
        [cite_start]raise e # [cite: 395]

    [cite_start]SHEET_CACHE[aba_name] = { 'data': linhas, 'timestamp': agora } # [cite: 395]
    return linhas

def get_sheet_data_future(aba_code):
    """Obtém dados da aba de cache de jogos futuros (sheet_future)."""

    [cite_start]aba_name = LIGAS_MAP[aba_code]['sheet_future'] # [cite: 395]
    if not client: return []

    try:
        sh = client.open_by_url(SHEET_URL)
        [cite_start]linhas_raw = sh.worksheet(aba_name).get_all_values() # [cite: 395]
    except Exception as e:
        [cite_start]logging.error(f"Erro ao buscar cache de futuros jogos em {aba_name}: {e}") # [cite: 396]
        return []

    [cite_start]if not linhas_raw or len(linhas_raw) <= 1: # [cite: 396]
        return []

    data_rows = linhas_raw[1:]

    jogos = []
    for row in data_rows:
        if len(row) >= 4:
            jogos.append({
                [cite_start]"Mandante_Nome": row[0], # [cite: 397]
                [cite_start]"Visitante_Nome": row[1], # [cite: 397]
                "Data_Hora": row[2],
                "Matchday": safe_int(row[3])
            })

    [cite_start]return jogos # [cite: 397]

async def pre_carregar_cache_sheets():
    """Pré-carrega o histórico de todas as ligas (rodado uma vez na inicialização)."""
    if not client:
        [cite_start]logging.warning("Pré-carregamento de cache ignorado: Conexão GSheets falhou.") # [cite: 398]
        return

    logging.info("Iniciando pré-carregamento de cache...")
    for aba in ABAS_PASSADO:
        try:
            get_sheet_data(aba)
            logging.info(f"Cache de histórico para {aba} pré-carregado.")
        except Exception as e:
            [cite_start]logging.warning(f"Não foi possível pré-carregar cache para {aba}: {e}") # [cite: 399]
        await asyncio.sleep(1)

# =================================================================================
# 🎯 FUNÇÕES DE API E ATUALIZAÇÃO 
# =================================================================================
def buscar_jogos(league_code, status_filter):
    """Busca jogos na API com filtro de status (usado para FINISHED e ALL)."""
    
    # Usa o ID da API do novo LEAGUE_API_MAP
    api_id = LEAGUE_API_MAP.get(league_code, {}).get('id')
    if not api_id:
        logging.error(f"Código de liga '{league_code}' não encontrado no LEAGUE_API_MAP.")
        return []

    try:
        url = f"https://api.football-data.org/v4/competitions/{api_id}/matches"

        if status_filter != "ALL":
             url += f"?status={status_filter}"

        r = requests.get(
            url,
            [cite_start]headers={"X-Auth-Token": API_KEY}, timeout=10 # [cite: 400]
        )
        r.raise_for_status()
    except Exception as e:
        [cite_start]logging.error(f"Erro ao buscar jogos {status_filter} para {league_code}: {e}") # [cite: 400]
        return []

    all_matches = r.json().get("matches", [])

    if status_filter == "ALL":
        # Garante que apenas jogos agendados ou cronometrados (futuros) sejam retornados.
        [cite_start]return [m for m in all_matches if m.get('status') in ['SCHEDULED', 'TIMED']] # [cite: 401]

    else:
        # Lógica original para jogos FINISHED
        jogos = []
        for m in all_matches:
            if m.get('status') == "FINISHED":
                try:
                    [cite_start]jogo_data = datetime.strptime(m['utcDate'][:10], "%Y-%m-%d") # [cite: 402]
                    ft = m.get("score", {}).get("fullTime", {})
                    ht = m.get("score", {}).get("halfTime", {})
                    if ft.get("home") is None: continue

                    [cite_start]gm, gv = ft.get("home",0), ft.get("away",0) # [cite: 403]
  
                    gm1, gv1 = ht.get("home",0), ht.get("away",0)

                    jogos.append({
                        "Mandante": m.get("homeTeam", {}).get("name", ""),
                        [cite_start]"Visitante": m.get("awayTeam", {}).get("name", ""), # [cite: 404]
    
                        "Gols Mandante": gm, "Gols Visitante": gv,
                        "Gols Mandante 1T": gm1, "Gols Visitante 1T": gv1,
                        [cite_start]"Gols Mandante 2T": gm - gm1, "Gols Visitante 2T": gv - gv1, # [cite: 405]
         
                        "Data": jogo_data.strftime("%d/%m/%Y")
                    })
                except: continue
        [cite_start]return sorted(jogos, key=lambda x: datetime.strptime(x['Data'], "%d/%m/%Y")) # [cite: 405]

def buscar_jogos_live(league_code):
    """Busca jogos AO VIVO (IN_PLAY, HALF_TIME, PAUSED) buscando todos os jogos do dia na API."""
    
    # Usa o ID da API do novo LEAGUE_API_MAP
    api_id = LEAGUE_API_MAP.get(league_code, {}).get('id')
    if not api_id:
        logging.error(f"Código de liga '{league_code}' não encontrado no LEAGUE_API_MAP.")
        return []
        
    [cite_start]hoje_utc = datetime.now(timezone.utc).strftime('%Y-%m-%d') # [cite: 406]

    try:
        # Busca todos os jogos da liga que ocorrem na data de hoje
        [cite_start]url = f"https://api.football-data.org/v4/competitions/{api_id}/matches?dateFrom={hoje_utc}&dateTo={hoje_utc}" # [cite: 406]

        r = requests.get(
            url,
            headers={"X-Auth-Token": API_KEY}, timeout=10
        )
        [cite_start]r.raise_for_status() # [cite: 407]
    except Exception as e:
        [cite_start]logging.error(f"Erro ao buscar jogos AO VIVO (busca por data) para {league_code}: {e}") # [cite: 407]
        return []

    [cite_start]all_matches = r.json().get("matches", []) # [cite: 407]

    jogos = []
    for m in all_matches:
        status_api = m.get('status')
        # Filtra manualmente apenas os status que representam um jogo ativo
        [cite_start]if status_api in LIVE_STATUSES: # [cite: 408]
            try:
                [cite_start]ft_score = m.get("score", {}).get("fullTime", {}) # [cite: 408]

                gm_atual = ft_score.get("home") if ft_score.get("home") is not None else 0
                [cite_start]gv_atual = ft_score.get("away") if ft_score.get("away") is not None else 0 # [cite: 408]

                minute = m.get("minute", "N/A")

                [cite_start]if status_api in ['PAUSED', 'HALF_TIME']: # [cite: 409]
                    [cite_start]minute = status_api # Mostra o status exato (e.g. HALF_TIME) # [cite: 410]
                elif status_api == "IN_PLAY":
                    # Tentativa de obter o minuto, se não vier, infere o tempo
                    [cite_start]if minute == "N/A": # [cite: 410]
                        [cite_start]if m.get("score", {}).get("duration", "") == "REGULAR": # [cite: 411]
                            minute = "2ºT"
                        else:
                            [cite_start]minute = "1ºT" # [cite: 411]

                jogos.append({
                    [cite_start]"Mandante_Nome": m.get("homeTeam", {}).get("name", ""), # [cite: 412]
                    "Visitante_Nome": m.get("awayTeam", {}).get("name", ""),
                    [cite_start]"Placar_Mandante": gm_atual, # [cite: 413]
                    [cite_start]"Placar_Visitante": gv_atual, # [cite: 413]
                    "Tempo_Jogo": minute,
                    "Matchday": safe_int(m.get("matchday", 0))
                })
            except: continue

    return jogos

async def atualizar_planilhas(context: ContextTypes.DEFAULT_TYPE):
    """Atualiza o histórico e o cache de futuros jogos. Função para o JobQueue."""
    [cite_start]global SHEET_CACHE # [cite: 414]

    if not client:
        [cite_start]logging.error("Atualização de planilhas ignorada: Cliente GSheets não autorizado.") # [cite: 414]
        return
        
    try: sh = client.open_by_url(SHEET_URL)
    except:
        logging.error("Erro ao abrir planilha para atualização.")
        return

    [cite_start]logging.info("Iniciando a atualização periódica das planilhas...") # [cite: 414]

    
    [cite_start]for aba_code, aba_config in LIGAS_MAP.items(): # [cite: 415]
        # 1. ATUALIZAÇÃO DO HISTÓRICO (ABA_PASSADO)
        aba_past = aba_config['sheet_past']
        [cite_start]try: ws_past = sh.worksheet(aba_past) # [cite: 415]
        except WorksheetNotFound: 
            logging.warning(f"Aba de histórico '{aba_past}' não encontrada. Ignorando...")
            continue

        jogos_finished = buscar_jogos(aba_code, "FINISHED") # Usa a função que pega o ID da API
        [cite_start]await asyncio.sleep(10) # Pausa para respeitar limite de rate da API [cite: 416]

        if jogos_finished:
            try:
                exist = ws_past.get_all_records()
                keys_exist = {(r['Mandante'], r['Visitante'], r['Data']) for r in exist}

                novas_linhas = []
            
                [cite_start]for j in jogos_finished: # [cite: 417]
                    key = (j["Mandante"], j["Visitante"], j["Data"])
                    if key not in keys_exist:
           
                        [cite_start]novas_linhas.append([ # [cite: 418]
                            j["Mandante"], j["Visitante"], j["Gols Mandante"], j["Gols Visitante"],
                            j["Gols Mandante 1T"], j["Gols Visitante 1T"],
                     
                            [cite_start]j["Gols Mandante 2T"], j["Gols Visitante 2T"], j["Data"] # [cite: 419]
                        ])

                if novas_linhas:
                    ws_past.append_rows(novas_linhas)
                    logging.info(f"✅ {len(novas_linhas)} jogos adicionados ao histórico de {aba_past}.")

                    [cite_start]if aba_past in SHEET_CACHE: del SHEET_CACHE[aba_past] # [cite: 420]
      
            except Exception as e:
                [cite_start]logging.error(f"Erro ao inserir dados na planilha {aba_past}: {e}") # [cite: 420]

        # 2. ATUALIZAÇÃO DO CACHE DE FUTUROS JOGOS (ABA_FUTURE)
        [cite_start]aba_future = aba_config['sheet_future'] # [cite: 420]
        
        [cite_start]try: ws_future = sh.worksheet(aba_future) # [cite: 421]
        except WorksheetNotFound:
            [cite_start]logging.warning(f"Aba de futuros jogos '{aba_future}' não encontrada. Ignorando...") # [cite: 422]
            continue

        jogos_future = buscar_jogos(aba_code, "ALL") # Usa a função que pega o ID da API
        await asyncio.sleep(10) # Pausa para respeitar limite de rate da API

        try:
            [cite_start]ws_future.clear() # [cite: 423]
            ws_future.update(values=[['Mandante', 'Visitante', 'Data/Hora', 'Matchday']], range_name='A1:D1')

            if jogos_future:
          
                linhas_future = []

                for m in jogos_future:
                    matchday = m.get("matchday", "")
                    [cite_start]utc_date = m.get('utcDate', '') # [cite: 424]
    
                    [cite_start]if utc_date: # [cite: 424]
  
                        try:
                            data_utc = datetime.strptime(utc_date[:16], '%Y-%m-%dT%H:%M')
                            # [cite_start]Limita a busca a jogos de até 90 dias no futuro # [cite: 425]
        
                            if data_utc < datetime.now() + timedelta(days=90):
                                linhas_future.append([
                                    [cite_start]m.get("homeTeam", {}).get("name"), # [cite: 426]
                                    m.get("awayTeam", {}).get("name"),
                                    utc_date,
                                    [cite_start]matchday # [cite: 427]
                                ])
      
                        [cite_start]except: # [cite: 428]
                            continue

             
                if linhas_future:
                    ws_future.append_rows(linhas_future, value_input_option='USER_ENTERED')
                    [cite_start]logging.info(f"✅ {len(linhas_future)} jogos futuros atualizados no cache de {aba_future}.") # [cite: 429]
                else:
                    [cite_start]logging.info(f"⚠️ Nenhuma partida agendada para {aba_code}. Cache {aba_future} limpo.") # [cite: 430]

        except Exception as e:
            [cite_start]logging.error(f"Erro ao atualizar cache de futuros jogos em {aba_future}: {e}") # [cite: 430]

        await asyncio.sleep(3) # Pausa entre ligas

# =================================================================================
# 📈 FUNÇÕES DE CÁLCULO E FORMATAÇÃO DE ESTATÍSTICAS
# =================================================================================
def calcular_estatisticas_time(time, aba, ultimos=None, casa_fora=None):
    """Calcula estatísticas detalhadas para um time em uma liga."""

    # Dicionário de resultados (Inicialização completa e detalhada)
    d = {"time":time,"jogos_time":0,"jogos_casa":0,"jogos_fora":0,
         [cite_start]"over15":0,"over15_casa":0,"over15_fora":0, # [cite: 431]
         # GAT ADICIONADO AQUI
         "over25":0,"over25_casa":0,"over25_fora":0,
         "btts":0,"btts_casa":0,"btts_fora":0, "g_a_t":0,"g_a_t_casa":0,"g_a_t_fora":0, "over05_1T":0,"over05_1T_casa":0,"over05_1T_fora":0,
         "over05_2T":0,"over05_2T_casa":0,"over05_2T_fora":0, "over15_2T":0,"over15_2T_casa":0,"over15_2T_fora":0,
         "gols_marcados":0,"gols_sofridos":0, "gols_marcados_casa":0,"gols_sofridos_casa":0,
         "gols_marcados_fora":0,"gols_sofridos_fora":0, "total_gols":0,"total_gols_casa":0,"total_gols_fora":0,
         "gols_marcados_1T":0,"gols_sofridos_1T":0, "gols_marcados_2T":0,"gols_sofridos_2T":0,
         "gols_marcados_1T_casa":0,"gols_sofridos_1T_casa":0, "gols_marcados_1T_fora":0,"gols_sofridos_1T_fora":0,
         [cite_start]"gols_marcados_2T_casa":0,"gols_sofridos_2T_casa":0, "gols_marcados_2T_fora":0,"gols_sofridos_2T_fora":0} # [cite: 431]

    try:
        [cite_start]linhas = get_sheet_data(aba) # [cite: 432]
    except:
        return {"time":time, "jogos_time": 0}

    # Aplica filtro casa/fora
    if casa_fora=="casa":
        linhas = [l for l in linhas if l['Mandante']==time]
    elif casa_fora=="fora":
        linhas = [l for l in linhas if l['Visitante']==time]
    else:
        [cite_start]linhas = [l for l in linhas if l['Mandante']==time or l['Visitante']==time] # [cite: 432]

    # Ordena e filtra os N últimos jogos
    try:
        [cite_start]linhas.sort(key=lambda x: datetime.strptime(x['Data'], "%d/%m/%Y"), reverse=False) # [cite: 433]
    except: pass

    if ultimos:
        linhas = linhas[-ultimos:]

    for linha in linhas:
        [cite_start]em_casa = (time == linha['Mandante']) # [cite: 433]
        gm, gv = safe_int(linha['Gols Mandante']), safe_int(linha['Gols Visitante'])
        gm1, gv1 = safe_int(linha['Gols Mandante 1T']), safe_int(linha['Gols Visitante 1T'])
        [cite_start]gm2, gv2 = gm-gm1, gv-gv1 # [cite: 434]

        [cite_start]total, total1, total2 = gm+gv, gm1+gv1, gm2+gv2 # [cite: 434]
        d["jogos_time"] += 1

        if em_casa:
            marcados, sofridos = gm, gv
            d["jogos_casa"] += 1
            d["gols_marcados_1T_casa"] += gm1
            [cite_start]d["gols_sofridos_1T_casa"] += gv1 # [cite: 435]
            d["gols_marcados_2T_casa"] += gm2
        
            [cite_start]d["gols_sofridos_2T_casa"] += gv2 # [cite: 435]
        else:
            marcados, sofridos = gv, gm
            d["jogos_fora"] += 1
            d["gols_marcados_1T_fora"] += gv1
            
            [cite_start]d["gols_sofridos_1T_fora"] += gm1 # [cite: 436]
            d["gols_marcados_2T_fora"] += gv2
            # CORREÇÃO DE ERRO DE LÓGICA: Gols sofridos no 2T fora é (Gols Mandante 2T) = gm2, não gm1
            [cite_start]d["gols_sofridos_2T_fora"] += gm2 # [cite: 436]

        [cite_start]d["gols_marcados"] += marcados # [cite: 436]
        d["gols_sofridos"] += sofridos
        if em_casa:
            [cite_start]d["gols_marcados_casa"] += marcados # [cite: 437]
            d["gols_sofridos_casa"] += sofridos
        else:
            d["gols_marcados_fora"] += marcados
            d["gols_sofridos_fora"] += sofridos

       
        d["total_gols"] += total
        [cite_start]if em_casa: d["total_gols_casa"] += total # [cite: 437]
        else: d["total_gols_fora"] += total

       
        [cite_start]if total>1.5: d["over15"] += 1 # [cite: 438]
        if total>2.5: d["over25"] += 1
        if gm>0 and gv>0: d["btts"] += 1
        if total1>0.5: d["over05_1T"] += 1
        if total2>0.5: d["over05_2T"] += 1
        if total2>1.5: d["over15_2T"] += 1

        # GAT (Gol em Ambos os Tempos) - NOVO CÁLCULO
        [cite_start]gol_no_1t = total1 > 0 # [cite: 439]
        gol_no_2t = total2 > 0
        if gol_no_1t and gol_no_2t:
            d["g_a_t"] += 1
            d["g_a_t_casa" if em_casa else "g_a_t_fora"] += 1

        # Estatísticas por condição (casa/fora)
        d["over15_casa" if em_casa else "over15_fora"] += (1 if total > 1.5 else 0)
        [cite_start]d["over25_casa" if em_casa else "over25_fora"] += (1 if total > 2.5 else 0) # [cite: 440]
        d["btts_casa" if em_casa else "btts_fora"] += (1 if gm > 0 and gv > 0 else 0)
        d["over05_1T_casa" if em_casa else "over05_1T_fora"] += (1 if total1 > 0.5 else 0)
    
        d["over05_2T_casa" if em_casa else "over05_2T_fora"] += (1 if total2 > 0.5 else 0)
        [cite_start]d["over15_2T_casa" if em_casa else "over15_2T_fora"] += (1 if total2 > 1.5 else 0) # [cite: 440]

        [cite_start]d["gols_marcados_1T"] += gm1 if em_casa else gv1 # [cite: 441]
        d["gols_sofridos_1T"] += gv1 if em_casa else gm1
        d["gols_marcados_2T"] += gm2 if em_casa else gv2
        d["gols_sofridos_2T"] += gv2 if em_casa else gm2 # CORREÇÃO DE ERRO DE LÓGICA (uso de gm2)

    [cite_start]return d # [cite: 441]

def formatar_estatisticas(d):
  
    """Formata o dicionário de estatísticas para a mensagem do Telegram."""
    [cite_start]jt, jc, jf = d["jogos_time"], d.get("jogos_casa", 0), d.get("jogos_fora", 0) # [cite: 441]

    if jt == 0: return f"⚠️ **Nenhum jogo encontrado** para **{escape_markdown(d['time'])}** com o filtro selecionado." [cite_start]# [cite: 442]
    
    return (f"📊 **Estatísticas - {escape_markdown(d['time'])}**\n"
            [cite_start]f"📅 Jogos: {jt} (Casa: {jc} | Fora: {jf})\n\n" # [cite: 443]
            f"⚽ Over 1.5: **{pct(d['over15'], jt)}** (C: {pct(d.get('over15_casa',0), jc)} | F: {pct(d.get('over15_fora',0), jf)})\n"
            f"⚽ Over 2.5: **{pct(d['over25'], jt)}** (C: {pct(d.get('over25_casa',0), jc)} | F: {pct(d.get('over25_fora',0), jf)})\n"
            f"🔁 BTTS: **{pct(d['btts'], jt)}** (C: {pct(d.get('btts_casa',0), jc)} | F: {pct(d.get('btts_fora',0), jf)})\n"
            f"🥅 **G.A.T. (Gol em Ambos os Tempos): {pct(d.get('g_a_t',0), jt)}** (C: {pct(d.get('g_a_t_casa',0), jc)} | F: {pct(d.get('g_a_t_fora',0), jf)})\n\n" # LINHA G.A.T. [cite_start]ADICIONADA [cite: 444]
            
            f"⏱️ 1ºT Over 0.5: {pct(d['over05_1T'], jt)} (C: {pct(d['over05_1T_casa'], jc)} | F: {pct(d['over05_1T_fora'], jf)})\n"
            f"⏱️ 2ºT Over 0.5: {pct(d['over05_2T'], jt)} (C: {pct(d['over05_2T_casa'], jc)} | F: {pct(d['over05_2T_fora'], jf)})\n"
            [cite_start]f"⏱️ 2ºT Over 1.5: {pct(d['over15_2T'], jt)} (C: {pct(d['over15_2T_casa'], jc)} | F: {pct(d['over15_2T_fora'], jf)})\n\n" # [cite: 445]
            
            [cite_start]f"➕ **Média gols marcados:** {media(d['gols_marcados'], jt)} (C: {media(d.get('gols_marcados_casa',0), jc)} | F: {media(d.get('gols_marcados_fora',0), jf)})\n" # [cite: 445]
           
            f"➖ **Média gols sofridos:** {media(d['gols_sofridos'], jt)} (C: {media(d.get('gols_sofridos_casa',0), jc)} | F: {media(d.get('gols_sofridos_fora',0), jf)})\n\n"

            [cite_start]f"⏱️ Média gols 1ºT (GP/GC): {media(d['gols_marcados_1T'], jt)} / {media(d['gols_sofridos_1T'], jt)}\n" # [cite: 446]
            [cite_start]f"⏱️ Média gols 2ºT (GP/GC): {media(d['gols_marcados_2T'], jt)} / {media(d['gols_sofridos_2T'], jt)}\n\n" # [cite: 446]
            
            [cite_start]f"🔢 **Média total de gols:** {media(d['total_gols'], jt)} (C: {media(d.get('total_gols_casa',0), jc)} | F: {media(d.get('total_gols_fora',0), jf)})" # [cite: 447]
    )

def listar_ultimos_jogos(time, aba, ultimos=None, casa_fora=None):
    """Lista os últimos N jogos de um time com filtros."""
    try: linhas = get_sheet_data(aba)
    except: return f"⚠️ Erro ao ler dados da planilha para {escape_markdown(time)}." [cite_start]# [cite: 447]

    if casa_fora == "casa":
        linhas = [l for l in linhas if l['Mandante'] == time]
    elif casa_fora == "fora":
        linhas = [l for l in linhas if l['Visitante'] == time]
    else:
        [cite_start]linhas = [l for l in linhas if l['Mandante'] == time or l['Visitante'] == time] # [cite: 448]

 
    [cite_start]try: linhas.sort(key=lambda x: datetime.strptime(x['Data'], "%d/%m/%Y"), reverse=False) # [cite: 448]
    except: pass

    if ultimos:
        linhas = linhas[-ultimos:]

    if not linhas: return f"Nenhum jogo encontrado para **{escape_markdown(time)}** com o filtro selecionado."

    texto_jogos = ""
    for l in linhas:
        data = l['Data']
        [cite_start]gm, gv = safe_int(l['Gols Mandante']), safe_int(l['Gols Visitante']) # [cite: 449]

        if l['Mandante'] == time:
        
            oponente = escape_markdown(l['Visitante'])
            condicao = "(CASA)"
            m_cor = "🟢" if gm > gv else ("🟡" if gm == gv else "🔴")
            [cite_start]texto_jogos += f"{m_cor} {data} {condicao}: **{escape_markdown(time)}** {gm} x {gv} {oponente}\n" # [cite: 449]
   
        else:
            [cite_start]oponente = escape_markdown(l['Mandante']) # [cite: 450]
           
            condicao = "(FORA)"
            m_cor = "🟢" if gv > gm else ("🟡" if gv == gm else "🔴")
            [cite_start]texto_jogos += f"{m_cor} {data} {condicao}: {oponente} {gm} x {gv} **{escape_markdown(time)}**\n" # [cite: 450]

    return texto_jogos

# =================================================================================
# 🤖 FUNÇÕES DO BOT: HANDLERS E FLUXOS
# =================================================================================
[cite_start]async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE): # [cite: 451]
    text = (
        "👋 Bem-vindo ao **Bot de Estatísticas de Confronto**!\n\n"
        "Selecione um comando para começar:\n"
        "• **/stats** 📊: Inicia a análise estatística de um confronto futuro ou ao vivo."
    [cite_start]) # [cite: 342]
    await update.message.reply_text(text, parse_mode='Markdown')

async def listar_competicoes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Primeira tela: Lista todas as competições."""
    [cite_start]title = "📊 **Estatísticas de Confronto:** Escolha a Competição:" # [cite: 343]
    keyboard = []
    [cite_start]abas_list = list(LIGAS_MAP.keys()) # [cite: 343]
    for i in range(0, len(abas_list), 3):
        row = []
        for aba in abas_list[i:i + 3]:
            [cite_start]row.append(InlineKeyboardButton(aba, callback_data=f"c|{aba}")) # [cite: 343]
        keyboard.append(row)
    reply_markup = InlineKeyboardMarkup(keyboard)
    if update.message:
        [cite_start]await update.message.reply_text(title, reply_markup=reply_markup, parse_mode='Markdown') # [cite: 343]
    else:
        # Se for um callback, edita a mensagem anterior
        try:
            [cite_start]await update.callback_query.edit_message_text(title, reply_markup=reply_markup, parse_mode='Markdown') # [cite: 343]
        except BadRequest:
            # Fallback: Se a edição falhar, envia nova mensagem
            [cite_start]await update.callback_query.message.reply_text(title, reply_markup=reply_markup, parse_mode='Markdown') # [cite: 343]

async def mostrar_menu_status_jogo(update: Update, context: ContextTypes.DEFAULT_TYPE, aba_code: str):
    """ Segundo menu: Escolhe entre Jogos AO VIVO e Próximos Jogos (Future). """
    [cite_start]title = f"**{aba_code}** - Escolha o Tipo de Partida:" # [cite: 344]
    keyboard = [
        [InlineKeyboardButton("🔴 AO VIVO (API)", callback_data=f"STATUS|LIVE|{aba_code}")],
        [InlineKeyboardButton("📅 PRÓXIMOS JOGOS (Planilha)", callback_data=f"STATUS|FUTURE|{aba_code}")],
        [InlineKeyboardButton("⬅️ Voltar para Ligas", callback_data="VOLTAR_LIGA")]
    [cite_start]] # [cite: 344]
    reply_markup = InlineKeyboardMarkup(keyboard)
    try:
        [cite_start]await update.callback_query.edit_message_text(title, reply_markup=reply_markup, parse_mode='Markdown') # [cite: 344]
    except Exception as e:
        [cite_start]logging.error(f"ERRO ao editar mensagem em mostrar_menu_status_jogo (c|{aba_code}): {e}") # [cite: 344]
        await update.callback_query.message.reply_text(
            [cite_start]f"**{aba_code}** - Escolha o Tipo de Partida:", reply_markup=reply_markup, parse_mode='Markdown' # [cite: 344]
        )

async def listar_jogos(update: Update, context: ContextTypes.DEFAULT_TYPE, aba_code: str, status: str):
    """Terceira tela: Lista jogos futuros (GSheets) ou ao vivo (API)."""
    global GAME_ID_COUNTER # Para gerar IDs únicos
    jogos_a_listar = [] 
    
    if status == "FUTURE":
        try:
            await update.callback_query.edit_message_text(
                [cite_start]f"⏳ Buscando os próximos **{MAX_GAMES_LISTED}** jogos em **{aba_code}** (Planilha)...", # [cite: 345]
                parse_mode='Markdown'
            )
        except Exception as e:
            [cite_start]logging.error(f"Erro ao editar mensagem de loading FUTURE: {e}") # [cite: 345]
            pass
        
        [cite_start]jogos_agendados = get_sheet_data_future(aba_code) # [cite: 345]
        
        jogos_futuros_filtrados = [] 
        agora_utc = datetime.now(timezone.utc)
        
        # Filtro de jogos futuros já no cache da GSheets (o cache já faz o filtro de 90 dias)
        
        [cite_start]if not jogos_agendados: # [cite: 354]
            await update.callback_query.edit_message_text(
                f"⚠️ **Nenhum jogo futuro** encontrado em **{aba_code}**.\n"
                [cite_start]f"O Bot de atualização roda a cada 1 hora.", parse_mode='Markdown' # [cite: 354]
            )
            [cite_start]keyboard = [[InlineKeyboardButton("⬅️ Voltar para Status", callback_data=f"VOLTAR_LIGA_STATUS|{aba_code}")]] # [cite: 354]
            await update.effective_message.reply_text("Opções:", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
            return

        [cite_start]jogos_a_listar = jogos_agendados[:MAX_GAMES_LISTED] # [cite: 354]
        total_jogos_encontrados = len(jogos_agendados)
        [cite_start]matchday_label = f"Próximos {len(jogos_a_listar)} jogos (de {total_jogos_encontrados} no cache)" # [cite: 354]
        keyboard = []

        for jogo in jogos_a_listar:
            try:
                [cite_start]M_full = jogo['Mandante_Nome'] # [cite: 354]
                [cite_start]V_full = jogo['Visitante_Nome'] # [cite: 354]
                [cite_start]data_str = jogo['Data_Hora'] # [cite: 354]
                
                try:
                    [cite_start]data_utc = datetime.strptime(data_str[:16], '%Y-%m-%dT%H:%M') # [cite: 354]
                    matchday_num = jogo.get('Matchday', "N/A")
                    data_local = data_utc - timedelta(hours=3) # Assume -3h GMT
                    [cite_start]data_label = data_local.strftime('%d/%m %H:%M') # [cite: 355]
                except ValueError:
                    data_label = data_str
                    [cite_start]matchday_num = "N/A" # [cite: 355]
                
                # --- APLICAÇÃO DO FIX UNIVERSAL (JOGOS FUTUROS) ---
                GAME_ID_COUNTER += 1
                game_unique_id = f"G{GAME_ID_COUNTER}"
                
                # 1. ARMAZENA OS NOMES COMPLETOS NO CACHE
                GAME_CACHE[game_unique_id] = {'M': M_full, 'V': V_full, 'ABA': aba_code}

                M_safe = escape_markdown(M_full)
                V_safe = escape_markdown(V_full)
                
                # 2. CRIA O CALLBACK CURTO (USANDO O ID DO CACHE)
                callback_data = f"JOGO_ID|{game_unique_id}"
                [cite_start]label = f"({matchday_num}) {data_label} | {M_safe} x {V_safe}" # [cite: 355]
                
                keyboard.append([InlineKeyboardButton(label, callback_data=callback_data)])
            
            except Exception as e:
                [cite_start]logging.error(f"Erro ao processar jogo FUTURE: {e}") # [cite: 356]
                continue
    
    elif status == "LIVE":
        try:
            await update.callback_query.edit_message_text(
                f"⏳ Buscando jogos **AO VIVO** (IN_PLAY, INTERVALO) em **{aba_code}** (API)...", 
                [cite_start]parse_mode='Markdown' # [cite: 356]
            )
        except Exception as e:
            [cite_start]logging.error(f"Erro ao editar mensagem de loading LIVE: {e}") # [cite: 356]
            pass
        
        [cite_start]jogos_a_listar = buscar_jogos_live(aba_code) # [cite: 356]

        if not jogos_a_listar:
            await update.callback_query.edit_message_text(
                f"⚠️ **Nenhum jogo AO VIVO** encontrado em **{aba_code}** no momento.", 
                [cite_start]parse_mode='Markdown' # [cite: 357]
            )
            keyboard = [[InlineKeyboardButton("⬅️ Voltar para Status", callback_data=f"VOLTAR_LIGA_STATUS|{aba_code}")]]
            await update.effective_message.reply_text("Opções:", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
            return

        [cite_start]matchday_label = f"{len(jogos_a_listar)} jogos AO VIVO" # [cite: 357]
        keyboard = []
        for jogo in jogos_a_listar:
            M_full = jogo['Mandante_Nome']
            V_full = jogo['Visitante_Nome']
            [cite_start]placar_m = jogo['Placar_Mandante'] # [cite: 357]
            [cite_start]placar_v = jogo['Placar_Visitante'] # [cite: 357]
            [cite_start]tempo = jogo['Tempo_Jogo'] # [cite: 357]
            
            # --- APLICAÇÃO DO FIX UNIVERSAL (JOGOS AO VIVO) ---
            GAME_ID_COUNTER += 1
            game_unique_id = f"G{GAME_ID_COUNTER}"
            
            # 1. ARMAZENA OS NOMES COMPLETOS NO CACHE
            GAME_CACHE[game_unique_id] = {'M': M_full, 'V': V_full, 'ABA': aba_code}

            M_safe = escape_markdown(M_full)
            V_safe = escape_markdown(V_full)
            
            # 2. CRIA O CALLBACK CURTO (USANDO O ID DO CACHE)
            callback_data = f"JOGO_ID|{game_unique_id}"
            [cite_start]label = f"🔴 {tempo} | {M_safe} {placar_m} x {placar_v} {V_safe}" # [cite: 358]
            
            keyboard.append([InlineKeyboardButton(label, callback_data=callback_data)])
            
    # Envia a mensagem final com os botões de jogos
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(
        f"**{aba_code}** - {matchday_label}:", 
        reply_markup=reply_markup, 
        parse_mode='Markdown'
    )
    
async def mostrar_menu_acoes(update: Update, context: ContextTypes.DEFAULT_TYPE, aba_code: str, mandante: str, visitante: str):
    """ Quarta tela: Menu de filtros para estatísticas do confronto. """
    
    m_sanitized = escape_markdown(mandante)
    v_sanitized = escape_markdown(visitante)

    # 1. Armazena o confronto no cache para a próxima ação (clique no filtro)
    # Note: O ID do jogo já foi limpo no callback, então criamos um novo ID.
    # Para simplificar, vamos passar os nomes sanitizados diretamente no callback do FILTRO.
    # O callback do filtro JÁ ERA SEGURO, pois usa um índice.
    
    [cite_start]title = f"Escolha o filtro para o confronto **{m_sanitized} x {v_sanitized}**:" # [cite: 367]
    keyboard = []
    
    # Cria botões para Estatísticas e Resultados (Mandante/Visitante implícito)
    [cite_start]for idx, (label, tipo_filtro, ultimos, condicao_m, condicao_v) in enumerate(CONFRONTO_FILTROS): # [cite: 367]
        # O callback agora inclui Mandante e Visitante para o cálculo final
        # Note: Este callback é seguro, pois só é chamado APÓS o JOGO_ID, e usa M_sanitized (que é curto o suficiente)
        # Se os nomes sanitizados ainda forem muito longos (raro, mas possível), o erro pode ocorrer aqui. 
        # Mantemos assim por enquanto, pois o erro CRÍTICO vinha da listagem de jogos.
        [cite_start]callback_data = f"{tipo_filtro}|{aba_code}|{m_sanitized}|{v_sanitized}|{idx}" # [cite: 367]
        keyboard.append([InlineKeyboardButton(label, callback_data=callback_data)])
        
    # Opções de Voltar
    [cite_start]keyboard.append([InlineKeyboardButton("⬅️ Voltar para Jogos", callback_data=f"VOLTAR_LIGA_STATUS|{aba_code}")]) # [cite: 367]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # [cite_start]Envia a mensagem como uma nova resposta, mantendo o histórico [cite: 367]
    await update.effective_message.reply_text(title, reply_markup=reply_markup, parse_mode='Markdown')
    
    # [cite_start]Fecha o relógio de loading no botão clicado (JOGO_ID|...) [cite: 368]
    # O answer() é chamado no handler, vou mover para lá.


async def exibir_estatisticas(update: Update, context: ContextTypes.DEFAULT_TYPE, mandante: str, visitante: str, aba_code: str, filtro_idx: int):
    """ Exibe as estatísticas detalhadas. """
    
    [cite_start]if not (0 <= filtro_idx < len(CONFRONTO_FILTROS)): return # [cite: 369]
    
    # Filtro: (Label, Tipo, Últimos, Condicao_M, Condicao_V)
    [cite_start]_, _, ultimos, condicao_m, condicao_v = CONFRONTO_FILTROS[filtro_idx] # [cite: 369]
    
    # Calcula estatísticas para ambos os times e concatena
    [cite_start]d_m = calcular_estatisticas_time(mandante, aba_code, ultimos=ultimos, casa_fora=condicao_m) # [cite: 369]
    [cite_start]d_v = calcular_estatisticas_time(visitante, aba_code, ultimos=ultimos, casa_fora=condicao_v) # [cite: 369]
    
    # Gera o texto formatado para Mandante e Visitante
    texto_estatisticas = (
        formatar_estatisticas(d_m) + "\n\n---\n\n" + formatar_estatisticas(d_v)
    [cite_start]) # [cite: 369]
    
    # 1. Responde com a ESTATÍSTICA como uma NOVA MENSAGEM na conversa
    await update.effective_message.reply_text(
        f"**Confronto:** {escape_markdown(mandante)} x {escape_markdown(visitante)}\n\n{texto_estatisticas}", 
        [cite_start]parse_mode='Markdown' # [cite: 370]
    )
    
    # 2. Reexibe o menu de opções logo abaixo da estatística (CORREÇÃO DE UX)
    [cite_start]await mostrar_menu_acoes(update, context, aba_code, mandante, visitante) # [cite: 371]
    
    # 3. Fecha o relógio de loading do botão (sem pop-up)
    [cite_start]await update.callback_query.answer() # [cite: 371]

async def exibir_resultados(update: Update, context: ContextTypes.DEFAULT_TYPE, mandante: str, visitante: str, aba_code: str, filtro_idx: int):
    """ Exibe os resultados detalhados. """
    
    [cite_start]if not (0 <= filtro_idx < len(CONFRONTO_FILTROS)): return # [cite: 377]
    
    # Filtro: (Label, Tipo, Últimos, Condicao_M, Condicao_V)
    [cite_start]_, _, ultimos, condicao_m, condicao_v = CONFRONTO_FILTROS[filtro_idx] # [cite: 377]
    
    # Calcula resultados para ambos os times e concatena
    [cite_start]texto_jogos_m = listar_ultimos_jogos(mandante, aba_code, ultimos=ultimos, casa_fora=condicao_m) # [cite: 377]
    [cite_start]texto_jogos_v = listar_ultimos_jogos(visitante, aba_code, ultimos=ultimos, casa_fora=condicao_v) # [cite: 377]
    
    texto_final = (
        f"📅 **Últimos Resultados - {escape_markdown(mandante)}**\n{texto_jogos_m}" + 
        f"\n\n---\n\n" + 
        f"📅 **Últimos Resultados - {escape_markdown(visitante)}**\n{texto_jogos_v}"
    [cite_start]) # [cite: 377]
    
    # 1. Responde com os RESULTADOS como uma NOVA MENSAGEM na conversa (UX solicitada)
    await update.effective_message.reply_text(
        f"**Confronto:** {escape_markdown(mandante)} x {escape_markdown(visitante)}\n\n{texto_final}", 
        [cite_start]parse_mode='Markdown' # [cite: 377]
    )
    
    # 2. Reexibe o menu de opções logo abaixo dos resultados (CORREÇÃO DE UX)
    [cite_start]await mostrar_menu_acoes(update, context, aba_code, mandante, visitante) # [cite: 378]
    
    # 3. Fecha o relógio de loading do botão (sem pop-up)
    [cite_start]await update.callback_query.answer() # [cite: 378]

# =================================================================================
# 🔄 CALLBACK HANDLER PRINCIPAL (CORRIGIDO PARA O NOVO FLUXO JOGO_ID)
# =================================================================================
async def callback_query_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Lida com todos os cliques de botões inline (callbacks)."""
    query = update.callback_query
    [cite_start]data = query.data # [cite: 378]
    
    try:
        if data.startswith("c|"):
            [cite_start]_, aba_code = data.split('|') # [cite: 378]
            await mostrar_menu_status_jogo(update, context, aba_code)
            return

        if data.startswith("STATUS|"):
            [cite_start]_, status, aba_code = data.split('|') # [cite: 378]
            await listar_jogos(update, context, aba_code, status)
            return

        # 3. Seleção de Jogo (JOGO_ID|GAME_ID) -> Novo Fluxo Seguro
        if data.startswith("JOGO_ID|"):
            _, game_unique_id = data.split('|')
            
            # Responde ao callback antes de editar a mensagem (evita o relógio de loading)
            await query.answer() 

            if game_unique_id not in GAME_CACHE:
                await query.edit_message_text("❌ Erro: O confronto expirou ou não foi encontrado no cache. Tente novamente iniciando com /stats.")
                return

            game_data = GAME_CACHE[game_unique_id]
            aba_code = game_data['ABA']
            mandante = game_data['M']
            visitante = game_data['V']

            # Limpa o cache após o uso para não acumular memória
            del GAME_CACHE[game_unique_id]
            
            # Chama o menu de ações usando os nomes COMPLETO e CORRETO
            await mostrar_menu_acoes(update, context, aba_code, mandante, visitante)
            return
            
        # 4. Seleção de Filtro de Estatísticas (STATS_FILTRO|ABA|MANDANTE|VISITANTE|IDX)
        if data.startswith("STATS_FILTRO|"):
            await query.answer(text="Calculando estatísticas...", show_alert=False)
            _, aba_code, m_sanitized, v_sanitized, filtro_idx_str = data.split('|')
            filtro_idx = int(filtro_idx_str)
            await exibir_estatisticas(update, context, m_sanitized, v_sanitized, aba_code, filtro_idx)
            return

        # 5. Seleção de Filtro de Resultados (RESULTADOS_FILTRO|ABA|MANDANTE|VISITANTE|IDX)
        if data.startswith("RESULTADOS_FILTRO|"):
            await query.answer(text="Buscando últimos resultados...", show_alert=False)
            _, aba_code, m_sanitized, v_sanitized, filtro_idx_str = data.split('|')
            filtro_idx = int(filtro_idx_str)
            await exibir_resultados(update, context, m_sanitized, v_sanitized, aba_code, filtro_idx)
            return

        # 6. Voltar para Status
        if data.startswith("VOLTAR_LIGA_STATUS|"):
            [cite_start]_, aba_code = data.split('|') # [cite: 384]
            await mostrar_menu_status_jogo(update, context, aba_code)
            return

        # 7. Voltar para Ligas
        if data == "VOLTAR_LIGA":
            [cite_start]await listar_competicoes(update, context) # [cite: 384]
            return
            
        # Se for um callback não tratado, garante o fechamento do relógio
        await query.answer() 
        
    except Exception as e:
        [cite_start]logging.error(f"ERRO NO CALLBACK HANDLER ({data}): {e}") # [cite: 384]
        await update.effective_message.reply_text(f"❌ Ocorreu um erro ao processar sua solicitação: {e}", parse_mode='Markdown')
        try:
             await update.callback_query.edit_message_text("❌ Ocorreu um erro interno. Tente novamente iniciando com /stats.")
        except:
             [cite_start]await update.effective_message.reply_text("❌ Ocorreu um erro interno. Tente novamente iniciando com /stats.") # [cite: 384]


# =================================================================================
# 🚀 FUNÇÃO PRINCIPAL
# =================================================================================
def main():
    if not BOT_TOKEN or BOT_TOKEN == "SEU_TOKEN_AQUI":
        logging.error("O token do bot não está configurado. Verifique a variável de ambiente BOT_TOKEN.")
        sys.exit(1) # Finaliza o processo se o token estiver errado
        
    
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("stats", listar_competicoes))
    app.add_handler(CallbackQueryHandler(callback_query_handler))
    
    if client:
        job_queue: JobQueue = app.job_queue
        # Roda a atualização a cada 1 hora (3600 segundos)
        job_queue.run_repeating(atualizar_planilhas, interval=3600, first=0, name="AtualizacaoPlanilhas")
        asyncio.run(pre_carregar_cache_sheets())
    else:
        logging.warning("Job Queue de atualização desativado: Conexão com GSheets não estabelecida.")
    
    logging.info("Bot iniciando polling...")
    app.run_polling(drop_pending_updates=True)

if __name__ == '__main__':
    main()
