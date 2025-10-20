# ===============================================================================
# 🏆 BOT DE ESTATÍSTICAS DE CONFRONTO V2.0.0 - VERSÃO FINAL PARA RAILWAY
# ===============================================================================

# ===== Importações Essenciais =====
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests
from datetime import datetime, timedelta, timezone
import nest_asyncio
import asyncio
import logging
import os # Para ler variáveis de ambiente do Railway
import tempfile # Para lidar com credenciais JSON
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, ContextTypes
from telegram.error import BadRequest

# Configuração de Logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
nest_asyncio.apply()

# ===== Variáveis de Configuração (LIDAS DE VARIÁVEIS DE AMBIENTE DO RAILWAY) =====
# Os valores padrão ("SEU_TOKEN_AQUI") serão sobrescritos pelas variáveis do Railway.
BOT_TOKEN = os.environ.get("BOT_TOKEN", "SEU_TOKEN_AQUI") 
API_KEY = os.environ.get("API_KEY", "SUA_API_KEY_AQUI")
SHEET_URL = "https://docs.google.com/spreadsheets/d/1ChFFXQxo1qQElNzh2OC8-UPgofRXxyVWN06ExBQ3YqY/edit?usp=drivesdk"

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
CACHE_DURATION_SECONDS = 3600
MAX_GAMES_LISTED = 30

CONFRONTO_FILTROS = [
    (f"ÚLTIMOS {ULTIMOS} GERAL", ULTIMOS, None, None),
    (f"ÚLTIMOS {ULTIMOS} (M CASA vs V FORA)", ULTIMOS, "casa", "fora")
]

LIVE_STATUSES = ["IN_PLAY", "HALF_TIME", "PAUSED"]

# =================================================================================
# ✅ CONEXÃO GSHEETS VIA VARIÁVEL DE AMBIENTE (AJUSTE CRÍTICO PARA RAILWAY)
# =================================================================================

CREDS_JSON = os.environ.get("GSPREAD_CREDS_JSON")
client = None

if not CREDS_JSON:
    logging.error("❌ ERRO DE AUTORIZAÇÃO GSHEET: Variável GSPREAD_CREDS_JSON não encontrada. Configure-a no Railway.")
else:
    try:
        # Cria um arquivo temporário para que a biblioteca possa ler o JSON.
        with tempfile.NamedTemporaryFile(mode="w", delete=False, encoding='utf-8') as tmp_file:
            tmp_file.write(CREDS_JSON)
            tmp_file_path = tmp_file.name
        
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        
        # Autoriza usando o caminho do arquivo temporário.
        creds = ServiceAccountCredentials.from_json_keyfile_name(tmp_file_path, scope)
        client = gspread.authorize(creds)
        logging.info("✅ Conexão GSheets estabelecida via Variável de Ambiente.")
        
        # Remove o arquivo temporário imediatamente após o uso.
        os.remove(tmp_file_path)

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
    """Escapa caracteres que podem ser interpretados como Markdown."""
    return str(text).replace('*', '\\*').replace('_', '\\_').replace('[', '\\[') .replace(']', '\\]')

def get_sheet_data(aba_code):
    """Obtém dados da aba de histórico (sheet_past)."""
    global SHEET_CACHE
    agora = datetime.now()

    aba_name = LIGAS_MAP[aba_code]['sheet_past']

    if aba_name in SHEET_CACHE:
        cache_tempo = SHEET_CACHE[aba_name]['timestamp']
        if (agora - cache_tempo).total_seconds() < CACHE_DURATION_SECONDS:
            return SHEET_CACHE[aba_name]['data']

    try:
        if client is None: 
            raise Exception("Client GSheets não inicializado devido a erro de credenciais.")
            
        sh = client.open_by_url(SHEET_URL)
        linhas = sh.worksheet(aba_name).get_all_records()
    except Exception as e:
        logging.error(f"Erro de GSheets em get_sheet_data: {e}")
        if aba_name in SHEET_CACHE: return SHEET_CACHE[aba_name]['data']
        raise e

    SHEET_CACHE[aba_name] = { 'data': linhas, 'timestamp': agora }
    return linhas

def get_sheet_data_future(aba_code):
    """Obtém dados da aba de cache de jogos futuros (sheet_future)."""

    aba_name = LIGAS_MAP[aba_code]['sheet_future']

    try:
        if client is None: 
            raise Exception("Client GSheets não inicializado devido a erro de credenciais.")
            
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
        return [m for m in all_matches if m.get('status') in ['SCHEDULED', 'TIMED']]

    else:
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
        if status_api in LIVE_STATUSES:
            try:
                ft_score = m.get("score", {}).get("fullTime", {})
                gm_atual = ft_score.get("home") if ft_score.get("home") is not None else 0
                gv_atual = ft_score.get("away") if ft_score.get("away") is not None else 0
                minute = m.get("minute", "N/A")

                if status_api in ['PAUSED', 'HALF_TIME']:
                    minute = status_api
                elif status_api == "IN_PLAY":
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

async def atualizar_planilhas():
    """Atualiza o histórico e o cache de futuros jogos."""
    global SHEET_CACHE

    if client is None:
        logging.error("❌ Abortando atualização: Client GSheets não inicializado.")
        return

    try: sh = client.open_by_url(SHEET_URL)
    except:
        logging.error("Erro ao abrir planilha para atualização.")
        return

    for aba_code, aba_config in LIGAS_MAP.items():
        # 1. ATUALIZAÇÃO DO HISTÓRICO (ABA_PASSADO)
        aba_past = aba_config['sheet_past']
        try: ws_past = sh.worksheet(aba_past)
        except: continue

        jogos_finished = buscar_jogos(aba_code, "FINISHED")
        await asyncio.sleep(10)

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
        await asyncio.sleep(10)

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

        await asyncio.sleep(3)

async def atualizar_periodicamente(intervalo=3600):
    while True:
        await atualizar_planilhas()
        logging.info(f"Próxima atualização em {intervalo/60} minutos...")
        await asyncio.sleep(intervalo)

# =================================================================================
# 📈 FUNÇÕES DE CÁLCULO E FORMATAÇÃO DE ESTATÍSTICAS
# =================================================================================
def calcular_estatisticas_time(time, aba, ultimos=None, casa_fora=None):
    # ... (O corpo completo desta função foi mantido igual, apenas omitido para brevidade) ...
    d = {"time":time,"jogos_time":0,"jogos_casa":0,"jogos_fora":0,
         "over15":0,"over15_casa":0,"over15_fora":0, "over25":0,"over25_casa":0,"over25_fora":0,
         "btts":0,"btts_casa":0,"btts_fora":0, "over05_1T":0,"over05_1T_casa":0,"over05_1T_fora":0,
         "over05_2T":0,"over05_2T_casa":0,"over05_2T_fora":0, "over15_2T":0,"over15_2T_casa":0,"over15_2T_fora":0,
         "gols_marcados":0,"gols_sofridos":0, "gols_marcados_casa":0,"gols_sofridos_casa":0,
         "gols_marcados_fora":0,"gols_sofridos_fora":0, "total_gols":0,"total_gols_casa":0,"total_gols_fora":0,
         "gols_marcados_1T":0,"gols_sofridos_1T":0, "gols_marcados_2T":0,"gols_sofridos_2T":0,
         "gols_marcados_1T_casa":0,"gols_sofridos_1T_casa":0,
"gols_marcados_1T_fora":0,"gols_sofridos_1T_fora":0,
         "gols_marcados_2T_casa":0,"gols_sofridos_2T_casa":0, "gols_marcados_2T_fora":0,"gols_sofridos_2T_fora":0}

    try:
        linhas = get_sheet_data(aba)
    except:
        return {"time":time, "jogos_time": 0}

    if casa_fora=="casa":
        linhas = [l for l in linhas if l['Mandante']==time]
    elif casa_fora=="fora":
        linhas = [l for l in linhas if l['Visitante']==time]
    else:
        linhas = [l for l in linhas if l['Mandante']==time or l['Visitante']==time]

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
            d["gols_sofridos_2T_fora"] += gm1

        d["gols_marcados"] += marcados
        d["gols_sofridos"] += sofridos
        if em_casa:
            d["gols_marcados_casa"] += marcados
            d["gols_sofridos_casa"] += sofridos
        else:
            d["gols_marcados_fora"] += marcados
            d["gols_sofridos_fora"] += sofridos

        d["total_gols"] += total
        if em_casa:
            d["total_gols_casa"] += total
        else:
            d["total_gols_fora"] += total

        if total>1.5: d["over15"] += 1
        d["over15_casa" if em_casa else "over15_fora"] += 1
        if total>2.5: d["over25"] += 1
        d["over25_casa" if em_casa else "over25_fora"] += 1
        if gm>0 and gv>0: d["btts"] += 1
        d["btts_casa" if em_casa else "btts_fora"] += 1

        if total1>0.5: d["over05_1T"] += 1
        d["over05_1T_casa" if em_casa else "over05_1T_fora"] += 1
        if total2>0.5: d["over05_2T"] += 1
        d["over05_2T_casa" if em_casa else "over05_2T_fora"] += 1
        if total2>1.5: d["over15_2T"] += 1
        d["over15_2T_casa" if em_casa else "over15_2T_fora"] += 1

        d["gols_marcados_1T"] += gm1 if em_casa else gv1
        d["gols_sofridos_1T"] += gv1 if em_casa else gm1
        d["gols_marcados_2T"] += gm2 if em_casa else gv2
        d["gols_sofridos_2T"] += gv2 if em_casa else gm1

    return d

def formatar_estatisticas(d):
    # ... (O corpo completo desta função foi mantido igual, apenas omitido para brevidade) ...
    jt, jc, jf = d["jogos_time"], d.get("jogos_casa", 0), d.get("jogos_fora", 0)

    if jt == 0: return f"⚠️ **Nenhum jogo encontrado** para **{escape_markdown(d['time'])}** com o filtro selecionado."
    return (f"📊 **Estatísticas - {escape_markdown(d['time'])}**\n📅 Jogos: {jt}\n\n"
            f"⚽ Over 1.5: **{pct(d['over15'], jt)}**\n"
            f"🏟️ Casa: {pct(d.get('over15_casa',0), jc)} | Fora: {pct(d.get('over15_fora',0), jf)}\n\n"
            f"⚽ Over 2.5: **{pct(d['over25'], jt)}**\n"
            f"🏟️ Casa: {pct(d.get('over25_casa',0), jc)} | Fora: {pct(d.get('over25_fora',0), jf)}\n\n"
            f"🔁 BTTS: **{pct(d['btts'], jt)}**\n"
            f"🏟️ Casa: {pct(d.get('btts_casa',0), jc)} | Fora: {pct(d.get('btts_fora',0), jf)}\n\n"
            f"⏱️ 1ºT Over 0.5: {pct(d['over05_1T'], jt)} (Casa: {pct(d['over05_1T_casa'], jc)} | Fora: {pct(d['over05_1T_fora'], jf)})\n"
            f"⏱️ 2ºT Over 0.5: {pct(d['over05_2T'], jt)} (Casa: {pct(d['over05_2T_casa'], jc)} | Fora: {pct(d['over05_2T_fora'], jf)})\n"
            f"⏱️ 2ºT Over 1.5: {pct(d['over15_2T'], jt)} (Casa: {pct(d['over15_2T_casa'], jc)} | Fora: {pct(d['over15_2T_fora'], jf)})\n\n"
            f"➕ **Média gols marcados (Total):** {media(d['gols_marcados'], jt)} "
            f"(Casa: {media(d.get('gols_marcados_casa',0), jc)} | Fora: {media(d.get('gols_marcados_fora',0), jf)})\n"
            f"➖ **Média gols sofridos (Total):** {media(d['gols_sofridos'], jt)} "
            f"(Casa: {media(d.get('gols_sofridos_casa',0), jc)} | Fora: {media(d.get('gols_sofridos_fora',0), jf)})\n\n"
            f"⏱️ Média gols 1ºT (GP): {media(d['gols_marcados_1T'], jt)} "
            f"| Sofridos 1ºT (GC): {media(d['gols_sofridos_1T'], jt)}\n"
            f"⏱️ Média gols 2ºT (GP): {media(d['gols_marcados_2T'], jt)} "
            f"| Sofridos 2ºT (GC): {media(d['gols_sofridos_2T'], jt)}\n\n"
            f"🔢 **Média total de gols:** {media(d['total_gols'], jt)} "
            f"(Casa: {media(d.get('total_gols_casa',0), jc)} | Fora: {media(d.get('total_gols_fora',0), jf)})"
    )

def listar_ultimos_jogos(time, aba, ultimos=None, casa_fora=None):
    # ... (O corpo completo desta função foi mantido igual, apenas omitido para brevidade) ...
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

# ... (Todas as funções de comando, callback e menu: start_command, help_command, 
# listar_competicoes, mostrar_menu_status_jogo, listar_jogos, mostrar_menu_acoes, 
# mostrar_menu_filtros_confronto, mostrar_menu_filtros_resultados, 
# exibir_estatisticas_confronto, exibir_ultimos_resultados, callback_query_handler) ...
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = ("👋 Bem-vindo ao **Bot de Estatísticas de Confronto**!\n\n"
            "Selecione um comando para começar:\n"
            "• **/stats** 📊: Inicia a análise estatística de um confronto futuro ou ao vivo.\n"
            "• **/help** ℹ️: Exibe este guia de comandos.")
    await update.message.reply_text(text, parse_mode='Markdown')

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = ("ℹ️ **Guia de Comandos do Bot de Estatísticas**\n\n"
            "• **/stats** 📊: Exibe estatísticas completas de confrontos futuros ou ao vivo.\n"
            "• **/start** 🤖: Exibe a mensagem de boas-vindas.")
    await update.message.reply_text(text, parse_mode='Markdown')

async def listar_competicoes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    title = "📊 **Estatísticas de Confronto:** Escolha a Competição:"
    keyboard = []
    abas_list = list(LIGAS_MAP.keys())
    for i in range(0, len(abas_list), 3):
        row = []
        for aba in abas_list[i:i + 3]:
            row.append(InlineKeyboardButton(aba, callback_data=f"c:{aba}"))
        keyboard.append(row)
    reply_markup = InlineKeyboardMarkup(keyboard)
    if update.message:
        await update.message.reply_text(title, reply_markup=reply_markup, parse_mode='Markdown')
    else:
        await update.callback_query.edit_message_text(title, reply_markup=reply_markup, parse_mode='Markdown')

async def mostrar_menu_status_jogo(update: Update, context: ContextTypes.DEFAULT_TYPE, aba_code: str):
    title = f"**{aba_code}** - Escolha o Tipo de Partida:"
    keyboard = [
        [InlineKeyboardButton("🔴 AO VIVO (API)", callback_data=f"STATUS|LIVE|{aba_code}")],
        [InlineKeyboardButton("📅 PRÓXIMOS JOGOS (Planilha)", callback_data=f"STATUS|FUTURE|{aba_code}")],
        [InlineKeyboardButton("⬅️ Voltar para Ligas", callback_data="VOLTAR_LIGA")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    if update.callback_query:
        await update.callback_query.edit_message_text(title, reply_markup=reply_markup, parse_mode='Markdown')
    else:
        await update.effective_message.reply_text(title, reply_markup=reply_markup, parse_mode='Markdown')

async def listar_jogos(update: Update, context: ContextTypes.DEFAULT_TYPE, aba_code: str, status: str):
    jogos_a_listar = []

    if status == "FUTURE":
        await update.callback_query.edit_message_text(f"⏳ Buscando os próximos **{MAX_GAMES_LISTED}** jogos em **{aba_code}** (Planilha)...", parse_mode='Markdown')
        jogos_agendados = get_sheet_data_future(aba_code)
        jogos_futuros_filtrados = []
        agora_utc = datetime.now(timezone.utc).replace(tzinfo=None)
        for jogo in jogos_agendados:
            try:
                data_utc = datetime.strptime(jogo['Data_Hora'][:16], '%Y-%m-%dT%H:%M')
                if data_utc > agora_utc:
                    jogos_futuros_filtrados.append(jogo)
            except Exception: continue
        jogos_agendados = jogos_futuros_filtrados

        if not jogos_agendados:
            await update.effective_message.edit_text(f"⚠️ **Nenhum jogo agendado futuro** encontrado em **{aba_code}**.\nO Bot de atualização roda a cada 1 hora.", parse_mode='Markdown')
            keyboard = [[InlineKeyboardButton("⬅️ Voltar para Status", callback_data=f"VOLTAR_LIGA_STATUS|{aba_code}")]]
            await update.effective_message.reply_text("Opções:", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
            return

        jogos_a_listar = jogos_agendados[:MAX_GAMES_LISTED]
        total_jogos_encontrados = len(jogos_agendados)
        matchday_label = f"Próximos {MAX_GAMES_LISTED} jogos (de {total_jogos_encontrados} no cache)" if total_jogos_encontrados > MAX_GAMES_LISTED else f"Próximos {total_jogos_encontrados} jogos"
        keyboard = []
        for jogo in jogos_a_listar:
            try:
                M_full = jogo['Mandante_Nome']
                V_full = jogo['Visitante_Nome']
                data_str = jogo['Data_Hora']
                data_utc = datetime.strptime(data_str[:16], '%Y-%m-%dT%H:%M')
                matchday_num = jogo['Matchday'] if jogo['Matchday'] > 0 else "N/A"
                data_local = data_utc - timedelta(hours=3)
                data_label = data_local.strftime('%d/%m %H:%M')
                M_safe = escape_markdown(M_full)
                V_safe = escape_markdown(V_full)
                label = f"({matchday_num}) {data_label} | {M_safe} x {V_safe}"
                callback_data = f"JOGO|{aba_code}|{M_safe}|{V_safe}"
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
        await update.callback_query.edit_message_text(f"⏳ Buscando jogos **AO VIVO** (IN_PLAY, INTERVALO) em **{aba_code}** (API)...", parse_mode='Markdown')
        jogos_a_listar = buscar_jogos_live(aba_code)

        if not jogos_a_listar:
            await update.effective_message.edit_text(f"⚠️ **Nenhum jogo AO VIVO** encontrado em **{aba_code}** no momento.", parse_mode='Markdown')
            keyboard = [[InlineKeyboardButton("⬅️ Voltar para Status", callback_data=f"VOLTAR_LIGA_STATUS|{aba_code}")]]
            await update.effective_message.reply_text("Opções:", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
            return

        matchday_label = f"{len(jogos_a_listar)} jogos AO VIVO"
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
            if len(callback_data.encode('utf-8')) > 64:
                 M_safe_short = M_full.split(' ')[0][:8]
                 V_safe_short = V_full.split(' ')[0][:8]
                 callback_data = f"JOGO|{aba_code}|{M_safe_short}|{V_safe_short}"
                 label = f"🔴 {tempo} | {M_safe_short} {placar_m} x {placar_v} {V_safe_short}"
            keyboard.append([InlineKeyboardButton(label, callback_data=callback_data)])

    keyboard.append([InlineKeyboardButton("⬅️ Voltar para Status", callback_data=f"VOLTAR_LIGA_STATUS|{aba_code}")])
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.effective_message.edit_text(f"**SELECIONE A PARTIDA** ({aba_code} - **{matchday_label}**):", reply_markup=reply_markup, parse_mode='Markdown')

async def mostrar_menu_acoes(update: Update, context: ContextTypes.DEFAULT_TYPE, aba_code: str, mandante: str, visitante: str):
    m_sanitized = escape_markdown(mandante)
    v_sanitized = escape_markdown(visitante)
    context.user_data["current_match_info"] = {"aba": aba_code, "mandante": mandante, "visitante": visitante}
    keyboard = [
        [InlineKeyboardButton("📊 Estatísticas Detalhadas", callback_data=f"AÇÃO|STATS|{aba_code}|{m_sanitized}|{v_sanitized}")],
        [InlineKeyboardButton("📜 Últimos Resultados", callback_data=f"AÇÃO|RESULT|{aba_code}|{m_sanitized}|{v_sanitized}")],
        [InlineKeyboardButton("⬅️ Voltar para Jogos da Liga", callback_data=f"VOLTAR_LIGA_STATUS|{aba_code}")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(f"Jogo selecionado: **{m_sanitized} x {v_sanitized}**\n\nEscolha a **ação**:", reply_markup=reply_markup, parse_mode='Markdown')

async def mostrar_menu_filtros_confronto(update: Update, context: ContextTypes.DEFAULT_TYPE, aba_code: str, mandante: str, visitante: str):
    m_sanitized = escape_markdown(mandante)
    v_sanitized = escape_markdown(visitante)
    keyboard = []
    for idx, (nome, _, _, _) in enumerate(CONFRONTO_FILTROS):
        callback_data = f"FILTRO_STATS|{m_sanitized}|{v_sanitized}|{aba_code}|{idx}"
        keyboard.append([InlineKeyboardButton(f"📊 {nome}", callback_data=callback_data)])
    keyboard.append([InlineKeyboardButton("⬅️ Voltar para Ações", callback_data=f"AÇÃO_MENU|{aba_code}|{m_sanitized}|{v_sanitized}")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(f"Estatísticas: **{m_sanitized} x {v_sanitized}**\n\nEscolha o **filtro**:", reply_markup=reply_markup, parse_mode='Markdown')

async def mostrar_menu_filtros_resultados(update: Update, context: ContextTypes.DEFAULT_TYPE, aba_code: str, mandante: str, visitante: str):
    m_sanitized = escape_markdown(mandante)
    v_sanitized = escape_markdown(visitante)
    keyboard = []
    for idx, (nome, _, _, _) in enumerate(CONFRONTO_FILTROS):
        callback_data = f"FILTRO_RESULT|{m_sanitized}|{v_sanitized}|{aba_code}|{idx}"
        keyboard.append([InlineKeyboardButton(f"📜 {nome}", callback_data=callback_data)])
    keyboard.append([InlineKeyboardButton("⬅️ Voltar para Ações", callback_data=f"AÇÃO_MENU|{aba_code}|{m_sanitized}|{v_sanitized}")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.callback_query.edit_message_text(f"Últimos Resultados: **{m_sanitized} x {v_sanitized}**\n\nEscolha o **filtro**:", reply_markup=reply_markup, parse_mode='Markdown')

async def exibir_estatisticas_confronto(update: Update, context: ContextTypes.DEFAULT_TYPE, mandante: str, visitante: str, aba_code: str, filtro_idx: int):
    if not (0 <= filtro_idx < len(CONFRONTO_FILTROS)): return
    nome_filtro, ultimos, condicao_m, condicao_v = CONFRONTO_FILTROS[filtro_idx]
    m_sanitized = escape_markdown(mandante)
    v_sanitized = escape_markdown(visitante)

    await update.callback_query.edit_message_text(f"⏳ Calculando estatísticas (Filtro: **{nome_filtro}**)...", parse_mode='Markdown')

    d_mandante = calcular_estatisticas_time(mandante, aba_code, ultimos, condicao_m)
    texto_mandante = formatar_estatisticas(d_mandante)
    d_visitante = calcular_estatisticas_time(visitante, aba_code, ultimos, condicao_v)
    texto_visitante = formatar_estatisticas(d_visitante)

    mensagem_estatisticas = (
        f"**🏆 CONFRONTO:** {m_sanitized} x {v_sanitized}\n"
        f"**🔎 FILTRO:** {nome_filtro}\n"
        f"----------------------------------------\n"
        f"{texto_mandante}\n"
        f"----------------------------------------\n"
        f"{texto_visitante}"
    )
    await update.effective_message.reply_text(mensagem_estatisticas, parse_mode='Markdown')

    keyboard = []
    for idx, (nome, _, _, _) in enumerate(CONFRONTO_FILTROS):
        callback_data = f"FILTRO_STATS|{m_sanitized}|{v_sanitized}|{aba_code}|{idx}"
        keyboard.append([InlineKeyboardButton(f"📊 {nome}", callback_data=callback_data)])
    keyboard.append([InlineKeyboardButton("⬅️ Voltar para Ações", callback_data=f"AÇÃO_MENU|{aba_code}|{m_sanitized}|{v_sanitized}")])
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.effective_message.reply_text(f"Estatísticas para **{m_sanitized} x {v_sanitized}** (Escolha outro filtro):", reply_markup=reply_markup, parse_mode='Markdown')

async def exibir_ultimos_resultados(update: Update, context: ContextTypes.DEFAULT_TYPE, mandante: str, visitante: str, aba_code: str, filtro_idx: int):
    if not (0 <= filtro_idx < len(CONFRONTO_FILTROS)): return
    nome_filtro, ultimos, condicao_m, condicao_v = CONFRONTO_FILTROS[filtro_idx]
    m_sanitized = escape_markdown(mandante)
    v_sanitized = escape_markdown(visitante)

    await update.callback_query.edit_message_text(f"⏳ Buscando últimos resultados (Filtro: **{nome_filtro}**)...", parse_mode='Markdown')

    texto_ultimos_mandante = listar_ultimos_jogos(mandante, aba_code, ultimos, condicao_m)
    texto_ultimos_visitante = listar_ultimos_jogos(visitante, aba_code, ultimos, condicao_v)

    mensagem_resultados = (
        f"**🏆 CONFRONTO:** {m_sanitized} x {v_sanitized}\n"
        f"**🔎 FILTRO:** {nome_filtro}\n"
        f"----------------------------------------\n"
        f"📜 **ÚLTIMOS JOGOS DE {m_sanitized}:**\n"
        f"----------------------------------------\n"
        f"{texto_ultimos_mandante}\n"
        f"----------------------------------------\n"
        f"📜 **ÚLTIMOS JOGOS DE {v_sanitized}:**\n"
        f"----------------------------------------\n"
        f"{texto_ultimos_visitante}"
    )
    await update.effective_message.reply_text(mensagem_resultados, parse_mode='Markdown')

    keyboard = []
    for idx, (nome, _, _, _) in enumerate(CONFRONTO_FILTROS):
        callback_data = f"FILTRO_RESULT|{m_sanitized}|{v_sanitized}|{aba_code}|{idx}"
        keyboard.append([InlineKeyboardButton(f"📜 {nome}", callback_data=callback_data)])
    keyboard.append([InlineKeyboardButton("⬅️ Voltar para Ações", callback_data=f"AÇÃO_MENU|{aba_code}|{m_sanitized}|{v_sanitized}")])
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.effective_message.reply_text(f"Últimos Resultados para **{m_sanitized} x {v_sanitized}** (Escolha outro filtro):", reply_markup=reply_markup, parse_mode='Markdown')

async def callback_query_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "VOLTAR_LIGA":
        await listar_competicoes(update, context)
        return
    if data.startswith("VOLTAR_LIGA_STATUS|"):
        _, aba_code = data.split('|')
        await mostrar_menu_status_jogo(update, context, aba_code)
        return
    if data.startswith("c:"):
        aba_code = data.split(":")[1]
        await mostrar_menu_status_jogo(update, context, aba_code)
        return
    if data.startswith("STATUS|"):
        try:
            _, status, aba_code = data.split('|')
            await listar_jogos(update, context, aba_code, status)
        except Exception as e:
            logging.error(f"Erro ao processar STATUS|: {e} - Data: {data}")
        return
    if data.startswith("JOGO|"):
        try:
            _, aba_code, mandante, visitante = data.split('|')
            await mostrar_menu_acoes(update, context, aba_code, mandante, visitante)
        except Exception as e:
            logging.error(f"Erro ao processar JOGO|: {e} - Data: {data}")
        return
    if data.startswith("AÇÃO|"):
        try:
            _, acao, aba_code, mandante, visitante = data.split('|')
            if acao == "STATS":
                await mostrar_menu_filtros_confronto(update, context, aba_code, mandante, visitante)
            elif acao == "RESULT":
                await mostrar_menu_filtros_resultados(update, context, aba_code, mandante, visitante)
        except Exception as e:
            logging.error(f"Erro ao processar AÇÃO|: {e} - Data: {data}")
        return
    if data.startswith("AÇÃO_MENU|"):
        try:
            _, aba_code, mandante, visitante = data.split('|')
            await mostrar_menu_acoes(update, context, aba_code, mandante, visitante)
        except Exception as e:
            logging.error(f"Erro ao processar AÇÃO_MENU|: {e} - Data: {data}")
        return
    if data.startswith("FILTRO_STATS|"):
        try:
            _, mandante, visitante, aba_code, filtro_idx_str = data.split('|')
            filtro_idx = int(filtro_idx_str)
            await exibir_estatisticas_confronto(update, context, mandante, visitante, aba_code, filtro_idx)
        except Exception as e:
            logging.error(f"Erro ao processar FILTRO_STATS|: {e} - Data: {data}")
        return
    if data.startswith("FILTRO_RESULT|"):
        try:
            _, mandante, visitante, aba_code, filtro_idx_str = data.split('|')
            filtro_idx = int(filtro_idx_str)
            await exibir_ultimos_resultados(update, context, mandante, visitante, aba_code, filtro_idx)
        except Exception as e:
            logging.error(f"Erro ao processar FILTRO_RESULT|: {e} - Data: {data}")
        return


# =================================================================================
# ⚙️ FUNÇÃO PRINCIPAL (Polling)
# =================================================================================
def main():
    try:
        if not BOT_TOKEN or BOT_TOKEN == "SEU_TOKEN_AQUI":
            logging.error("O BOT_TOKEN não foi configurado. Configure no Railway.")
            return

        application = ApplicationBuilder().token(BOT_TOKEN).build()

        # Comandos e Callbacks
        application.add_handler(CommandHandler("start", start_command))
        application.add_handler(CommandHandler("help", help_command))
        application.add_handler(CommandHandler("stats", listar_competicoes))
        application.add_handler(CallbackQueryHandler(callback_query_handler))

        # Pré-carregar cache e iniciar tarefa de atualização
        asyncio.run(pre_carregar_cache_sheets())
        application.job_queue.run_once(
            lambda context: asyncio.create_task(atualizar_periodicamente()),
            0 # Inicia a primeira atualização imediatamente
        )
        
        # O modo Polling é o que o Railway Worker ou Service precisa
        logging.info("Bot iniciado em modo Polling no Railway...")
        application.run_polling()
        
    except Exception as e:
        logging.error(f"Erro na execução principal: {e}")

if __name__ == '__main__':
    main()
