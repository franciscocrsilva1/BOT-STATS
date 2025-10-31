# ===============================================================================
# 🏆 BOT DE ESTATÍSTICAS DE CONFRONTO V2.2.2 - UX FIX: MENU NOVO E RECORRENTE
# ===============================================================================

# ===== Importações Essenciais =====
[cite_start]import gspread [cite: 216]
[cite_start]from oauth2client.service_account import ServiceAccountCredentials [cite: 216]
[cite_start]import requests [cite: 216]
[cite_start]import os [cite: 216]
[cite_start]import tempfile [cite: 216]
[cite_start]import asyncio [cite: 216]
[cite_start]import logging [cite: 216]
[cite_start]from datetime import datetime, timedelta, timezone [cite: 216]
[cite_start]import nest_asyncio [cite: 216]
[cite_start]import sys # Necessário para o sys.exit [cite: 216]

[cite_start]from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update [cite: 216]
[cite_start]from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, ContextTypes, JobQueue [cite: 216]
[cite_start]from telegram.error import BadRequest [cite: 216]
[cite_start]from gspread.exceptions import WorksheetNotFound [cite: 216]

# Configuração de Logging
logging.basicConfig(
    [cite_start]format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO [cite: 216]
)
[cite_start]nest_asyncio.apply() [cite: 216]

# ===== Variáveis de Configuração (LIDAS DE VARIÁVEIS DE AMBIENTE) =====
[cite_start]BOT_TOKEN = os.environ.get("BOT_TOKEN", "SEU_TOKEN_AQUI") [cite: 216]
[cite_start]API_KEY = os.environ.get("API_KEY", "SUA_API_KEY_AQUI") [cite: 216]
[cite_start]SHEET_URL = os.environ.get("SHEET_URL", "https://docs.google.com/spreadsheets/d/1ChFFXQxo1qQElNzh2OC8-UPgofRXxyVWN06ExBQ3YqY/edit?usp=drivesdk") [cite: 216]

# Mapeamento de Ligas
LIGAS_MAP = {
    [cite_start]"CL": {"sheet_past": "CL", "sheet_future": "CL_FJ"}, [cite: 217]
    [cite_start]"BSA": {"sheet_past": "BSA", "sheet_future": "BSA_FJ"}, [cite: 217]
    [cite_start]"BL1": {"sheet_past": "BL1", "sheet_future": "BL1_FJ"}, [cite: 217]
    [cite_start]"PL": {"sheet_past": "PL", "sheet_future": "PL_FJ"}, [cite: 217]
    [cite_start]"ELC": {"sheet_past": "ELC", "sheet_future": "ELC_FJ"}, [cite: 217]
    [cite_start]"DED": {"sheet_past": "DED", "sheet_future": "DED_FJ"}, [cite: 217]
    [cite_start]"PD": {"sheet_past": "PD", "sheet_future": "PD_FJ"}, [cite: 217]
    [cite_start]"PPL": {"sheet_past": "PPL", "sheet_future": "PPL_FJ"}, [cite: 217]
    [cite_start]"SA": {"sheet_past": "SA", "sheet_future": "SA_FJ"}, [cite: 217]
    [cite_start]"FL1": {"sheet_past": "FL1", "sheet_future": "FL1_FJ"}, [cite: 217]
}
[cite_start]ABAS_PASSADO = list(LIGAS_MAP.keys()) [cite: 217]

[cite_start]ULTIMOS = 10 [cite: 217]
[cite_start]SHEET_CACHE = {} [cite: 217]
[cite_start]CACHE_DURATION_SECONDS = 3600 # 1 hora [cite: 217]
[cite_start]MAX_GAMES_LISTED = 30 [cite: 217]

# Filtros reutilizáveis para Estatísticas e Resultados
CONFRONTO_FILTROS = [
    # Label | Tipo no callback | Últimos | [cite_start]Condição Mandante | [cite: 218]
    # [cite_start]Condição Visitante [cite: 219]
    (f"📊 Estatísticas | ÚLTIMOS {ULTIMOS} GERAL", "STATS_FILTRO", ULTIMOS, None, None)[cite_start], [cite: 219]
    (f"📊 Estatísticas | {ULTIMOS} (M CASA vs V FORA)[cite_start]", "STATS_FILTRO", ULTIMOS, "casa", "fora"), [cite: 219]
    (f"📅 Resultados | ÚLTIMOS {ULTIMOS} GERAL", "RESULTADOS_FILTRO", ULTIMOS, None, None)[cite_start], [cite: 219]
    (f"📅 Resultados | {ULTIMOS} (M CASA vs V FORA)[cite_start]", "RESULTADOS_FILTRO", ULTIMOS, "casa", "fora"), [cite: 219]
]

[cite_start]LIVE_STATUSES = ["IN_PLAY", "HALF_TIME", "PAUSED"] [cite: 219]

# =================================================================================
# ✅ CONEXÃO GSHEETS VIA VARIÁVEL DE AMBIENTE 
# =================================================================================

[cite_start]CREDS_JSON = os.environ.get("GSPREAD_CREDS_JSON") [cite: 219]
[cite_start]client = None [cite: 219]

[cite_start]if not CREDS_JSON: [cite: 219]
    [cite_start]logging.error("❌ ERRO DE AUTORIZAÇÃO GSHEET: Variável GSPREAD_CREDS_JSON não encontrada. Configure-a no Railway.") [cite: 219]
[cite_start]else: [cite: 219]
    try:
        # Usa um arquivo temporário para carregar as credenciais
        [cite_start]with tempfile.NamedTemporaryFile(mode="w", delete=False, encoding='utf-8') as tmp_file: [cite: 220]
            [cite_start]tmp_file.write(CREDS_JSON) [cite: 220]
            [cite_start]tmp_file_path = tmp_file.name [cite: 220]
        
        [cite_start]scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"] [cite: 220]
        [cite_start]creds = ServiceAccountCredentials.from_json_keyfile_name(tmp_file_path, scope) [cite: 220]
        [cite_start]client = gspread.authorize(creds) [cite: 220]
      
        [cite_start]logging.info("✅ Conexão GSheets estabelecida via Variável de Ambiente.") [cite: 221]
        [cite_start]os.remove(tmp_file_path) # Limpa o arquivo temporário [cite: 221]

    [cite_start]except Exception as e: [cite: 221]
        [cite_start]logging.error(f"❌ ERRO DE AUTORIZAÇÃO GSHEET: Erro ao carregar ou autorizar credenciais JSON: {e}") [cite: 221]
        [cite_start]client = None [cite: 221]

# =================================================================================
# 💾 FUNÇÕES DE SUPORTE E CACHING 
# =================================================================================
[cite_start]def safe_int(v): [cite: 221]
    [cite_start]try: return int(v) [cite: 221]
    [cite_start]except: return 0 [cite: 221]

[cite_start]def pct(part, total): [cite: 221]
    [cite_start]return f"{(part/total)*100:.1f}%" if total>0 else "—" [cite: 221]

[cite_start]def media(part, total): [cite: 222]
    [cite_start]return f"{(part/total):.2f}" if total>0 else "—" [cite: 222]

[cite_start]def escape_markdown(text): [cite: 222]
    [cite_start]"""FIX CRÍTICO: Escapa caracteres que podem ser interpretados como Markdown (V1) e causavam o erro BadRequest.""" [cite: 222]
    # [cite_start]Escapa *, _, [ e ] que são os caracteres mais problemáticos [cite: 222]
    [cite_start]return str(text).replace('*', '\\*').replace('_', '\\_').replace('[', '\\[') .replace(']', '\\]') [cite: 222]

[cite_start]def get_sheet_data(aba_code): [cite: 222]
    [cite_start]"""Obtém dados da aba de histórico (sheet_past) com cache.""" [cite: 222]
    [cite_start]global SHEET_CACHE [cite: 222]
    [cite_start]agora = datetime.now() [cite: 222]

    [cite_start]aba_name = LIGAS_MAP[aba_code]['sheet_past'] [cite: 222]

    [cite_start]if aba_name in SHEET_CACHE: [cite: 222]
        [cite_start]cache_tempo = SHEET_CACHE[aba_name]['timestamp'] [cite: 223]
     
        [cite_start]if (agora - cache_tempo).total_seconds() < CACHE_DURATION_SECONDS: [cite: 223]
            [cite_start]return SHEET_CACHE[aba_name]['data'] [cite: 223]

    [cite_start]if not client: raise Exception("Cliente GSheets não autorizado.") [cite: 223]
    
    [cite_start]try: [cite: 223]
        [cite_start]sh = client.open_by_url(SHEET_URL) [cite: 223]
        [cite_start]linhas = sh.worksheet(aba_name).get_all_records() [cite: 223]
    [cite_start]except Exception as e: [cite: 223]
        [cite_start]if aba_name in SHEET_CACHE: return SHEET_CACHE[aba_name]['data'] [cite: 223]
        [cite_start]raise e [cite: 224]

    [cite_start]SHEET_CACHE[aba_name] = { 'data': linhas, 'timestamp': agora [cite: 224]
[cite_start]} [cite: 224]
    [cite_start]return linhas [cite: 224]

[cite_start]def get_sheet_data_future(aba_code): [cite: 224]
    [cite_start]"""Obtém dados da aba de cache de jogos futuros (sheet_future).""" [cite: 224]

    [cite_start]aba_name = LIGAS_MAP[aba_code]['sheet_future'] [cite: 224]
    [cite_start]if not client: return [] [cite: 224]

    [cite_start]try: [cite: 224]
        [cite_start]sh = client.open_by_url(SHEET_URL) [cite: 224]
        [cite_start]linhas_raw = sh.worksheet(aba_name).get_all_values() [cite: 224]
    [cite_start]except Exception as e: [cite: 224]
        [cite_start]logging.error(f"Erro ao buscar cache de futuros jogos em {aba_name}: {e}") [cite: 224]
        [cite_start]return [] [cite: 225]

    # [cite_start]CORREÇÃO DO ERRO DE SINTAXE NA LINHA 149 [cite: 225]
    [cite_start]if not linhas_raw or len(linhas_raw) <= 1: [cite: 225]
        [cite_start]return [] [cite: 225]

    [cite_start]data_rows = linhas_raw[1:] [cite: 225]

    [cite_start]jogos = [] [cite: 225]
    [cite_start]for row in data_rows: [cite: 225]
        [cite_start]if len(row) >= 4: [cite: 225]
            [cite_start]jogos.append({ [cite: 225]
                [cite_start]"Mandante_Nome": row[0], [cite: 226]
                [cite_start]"Visitante_Nome": row[1], [cite: 226]
               
                [cite_start]"Data_Hora": row[2], [cite: 226]
                [cite_start]"Matchday": safe_int(row[3]) [cite: 226]
            [cite_start]}) [cite: 226]

    [cite_start]return jogos [cite: 226]

[cite_start]async def pre_carregar_cache_sheets(): [cite: 226]
    [cite_start]"""Pré-carrega o histórico de todas as ligas (rodado uma vez na inicialização).""" [cite: 226]
    [cite_start]if not client: [cite: 226]
        [cite_start]logging.warning("Pré-carregamento de cache ignorado: Conexão GSheets falhou.") [cite: 227]
        [cite_start]return [cite: 227]

    [cite_start]logging.info("Iniciando pré-carregamento de cache...") [cite: 227]
    [cite_start]for aba in ABAS_PASSADO: [cite: 227]
     
        [cite_start]try: [cite: 227]
            [cite_start]get_sheet_data(aba) [cite: 227]
            [cite_start]logging.info(f"Cache de histórico para {aba} pré-carregado.") [cite: 227]
        [cite_start]except Exception as e: [cite: 227]
            
            [cite_start]logging.warning(f"Não foi possível pré-carregar cache para {aba}: {e}") [cite: 228]
        [cite_start]await asyncio.sleep(1) [cite: 228]

# =================================================================================
# 🎯 FUNÇÕES DE API E ATUALIZAÇÃO 
# =================================================================================
[cite_start]def buscar_jogos(league_code, status_filter): [cite: 228]
    [cite_start]"""Busca jogos na API com filtro de status (usado para FINISHED e ALL).""" [cite: 228]
  
    [cite_start]try: [cite: 228]
        [cite_start]url = f"https://api.football-data.org/v4/competitions/{league_code}/matches" [cite: 228]

        [cite_start]if status_filter != "ALL": [cite: 228]
             [cite_start]url += f"?status={status_filter}" [cite: 228]

        [cite_start]r = requests.get( [cite: 228]
            [cite_start]url, [cite: 229]
            [cite_start]headers={"X-Auth-Token": API_KEY}, timeout=10 [cite: 229]
        [cite_start]) [cite: 229]
        [cite_start]r.raise_for_status() [cite: 229]
    [cite_start]except Exception as e: [cite: 229]
   
        [cite_start]logging.error(f"Erro ao buscar jogos {status_filter} para {league_code}: {e}") [cite: 229]
        [cite_start]return [] [cite: 229]

    [cite_start]all_matches = r.json().get("matches", []) [cite: 229]

    [cite_start]if status_filter == "ALL": [cite: 229]
        # [cite_start]Garante que apenas jogos agendados ou [cite: 230] cronometrados (futuros) sejam retornados.
        [cite_start]return [m for m in all_matches if m.get('status') in ['SCHEDULED', 'TIMED']] [cite: 230]

    [cite_start]else: [cite: 230]
        # [cite_start]Lógica original para jogos FINISHED [cite: 230]
        [cite_start]jogos = [] [cite: 230]
        [cite_start]for m in all_matches: [cite: 230]
            [cite_start]if m.get('status') == "FINISHED": [cite: 230]
                [cite_start]try: [cite: 230]
                 
                    [cite_start]jogo_data = datetime.strptime(m['utcDate'][:10], "%Y-%m-%d") [cite: 231]
                    [cite_start]ft = m.get("score", {}).get("fullTime", {}) [cite: 231]
                    [cite_start]ht = m.get("score", {}).get("halfTime", {}) [cite: 231]
                    [cite_start]if ft.get("home") is None: continue [cite: 231]

                    [cite_start]gm, gv = ft.get("home",0), ft.get("away",0) [cite: 232]
  
                    [cite_start]gm1, gv1 = ht.get("home",0), ht.get("away",0) [cite: 232]

                    [cite_start]jogos.append({ [cite: 232]
                        [cite_start]"Mandante": m.get("homeTeam", {}).get("name", ""), [cite: 232]
                        [cite_start]"Visitante": m.get("awayTeam", {}).get("name", ""), [cite: 233]
    
                        [cite_start]"Gols Mandante": gm, "Gols Visitante": gv, [cite: 233]
                        [cite_start]"Gols Mandante 1T": gm1, "Gols Visitante 1T": gv1, [cite: 233]
                        [cite_start]"Gols Mandante 2T": gm - gm1, "Gols Visitante 2T": gv - gv1, [cite: 234]
         
                        [cite_start]"Data": jogo_data.strftime("%d/%m/%Y") [cite: 234]
                    [cite_start]}) [cite: 234]
                [cite_start]except: continue [cite: 234]
        [cite_start]return sorted(jogos, key=lambda x: datetime.strptime(x['Data'], "%d/%m/%Y")) [cite: 234]

[cite_start]def buscar_jogos_live(league_code): [cite: 234]
    [cite_start]"""Busca jogos AO VIVO (IN_PLAY, HALF_TIME, PAUSED) buscando todos os [cite: 235] jogos do dia na API."""
    [cite_start]hoje_utc = datetime.now(timezone.utc).strftime('%Y-%m-%d') [cite: 235]

    [cite_start]try: [cite: 235]
     
        # [cite_start]Busca todos os jogos da liga que ocorrem na data de hoje [cite: 235]
        [cite_start]url = f"https://api.football-data.org/v4/competitions/{league_code}/matches?dateFrom={hoje_utc}&dateTo={hoje_utc}" [cite: 235]

        [cite_start]r = requests.get( [cite: 235]
            [cite_start]url, [cite: 235]
            [cite_start]headers={"X-Auth-Token": API_KEY}, timeout=10 [cite: 236]
        [cite_start]) [cite: 236]
        [cite_start]r.raise_for_status() [cite: 236]
    [cite_start]except Exception as e: [cite: 236]
        [cite_start]logging.error(f"Erro ao buscar jogos AO VIVO (busca por data) para {league_code}: {e}") [cite: 236]
        [cite_start]return [] [cite: 236]

    [cite_start]all_matches = r.json().get("matches", []) [cite: 236]

    [cite_start]jogos = [] [cite: 236]
    [cite_start]for m in all_matches: [cite: 236]
        [cite_start]status_api = m.get('status') [cite: 236]
        # [cite_start]Filtra manualmente apenas os status que representam um jogo ativo [cite: 236]
        [cite_start]if status_api in LIVE_STATUSES: [cite: 237]
            [cite_start]try: [cite: 237]
                [cite_start]ft_score = m.get("score", {}).get("fullTime", {}) [cite: 237]

                [cite_start]gm_atual = ft_score.get("home") if ft_score.get("home") is not None else 0 [cite: 237]
                [cite_start]gv_atual = ft_score.get("away") if ft_score.get("away") is not None else 0 [cite: 237]

                [cite_start]minute = m.get("minute", "N/A") [cite: 237]

    
                [cite_start]if status_api in ['PAUSED', 'HALF_TIME']: [cite: 238]
        
                    [cite_start]minute = status_api # Mostra o status exato (e.g. [cite: 239] HALF_TIME)
                [cite_start]elif status_api == "IN_PLAY": [cite: 239]
                    # [cite_start]Tentativa de obter o minuto, se não vier, infere o tempo [cite: 239]
                    [cite_start]if minute == "N/A": [cite: 239]
                        [cite_start]if m.get("score", {}).get("duration", [cite: 240] "") == "REGULAR":
                            [cite_start]minute = "2ºT" [cite: 240]
                        [cite_start]else: [cite: 240]
                            [cite_start]minute = "1ºT" [cite: 240]

              
                
                [cite_start]jogos.append({ [cite: 241]
                    [cite_start]"Mandante_Nome": m.get("homeTeam", {}).get("name", ""), [cite: 241]
                    [cite_start]"Visitante_Nome": m.get("awayTeam", {}).get("name", ""), [cite: 241]
                    [cite_start]"Placar_Mandante": gm_atual, [cite: 241]
                 
                    [cite_start]"Placar_Visitante": gv_atual, [cite: 242]
          
                    [cite_start]"Tempo_Jogo": minute, [cite: 242]
                    [cite_start]"Matchday": safe_int(m.get("matchday", 0)) [cite: 242]
                [cite_start]}) [cite: 242]
            [cite_start]except: continue [cite: 242]

    [cite_start]return jogos [cite: 242]

[cite_start]async def atualizar_planilhas(context: ContextTypes.DEFAULT_TYPE): [cite: 242]
    [cite_start]"""Atualiza o [cite: 243] histórico e o cache de futuros jogos. Função para o JobQueue."""
    [cite_start]global SHEET_CACHE [cite: 243]

    [cite_start]if not client: [cite: 243]
   
        [cite_start]logging.error("Atualização de planilhas ignorada: Cliente GSheets não autorizado.") [cite: 244]
        [cite_start]return [cite: 244]
        
    [cite_start]try: sh = client.open_by_url(SHEET_URL) [cite: 244]
    [cite_start]except: [cite: 244]
        [cite_start]logging.error("Erro ao abrir planilha para atualização.") [cite: 244]
        [cite_start]return [cite: 244]

    [cite_start]logging.info("Iniciando a atualização periódica das planilhas...") [cite: 244]

    
    [cite_start]for aba_code, aba_config in LIGAS_MAP.items(): [cite: 244]
        # [cite_start]1. ATUALIZAÇÃO DO HISTÓRICO (ABA_PASSADO) [cite: 244]
        [cite_start]aba_past = aba_config['sheet_past'] [cite: 244]
        [cite_start]try: ws_past = sh.worksheet(aba_past) [cite: 244]
        [cite_start]except WorksheetNotFound: [cite: 244] 
            [cite_start]logging.warning(f"Aba de histórico '{aba_past}' não encontrada. Ignorando...") [cite: 244]
            [cite_start]continue [cite: 244]

        [cite_start]jogos_finished = buscar_jogos(aba_code, "FINISHED") [cite: 244]
        [cite_start]await asyncio.sleep(10) # [cite: 245] Pausa para respeitar limite de rate da API

        [cite_start]if jogos_finished: [cite: 245]
            [cite_start]try: [cite: 245]
                [cite_start]exist = ws_past.get_all_records() [cite: 245]
                [cite_start]keys_exist = {(r['Mandante'], r['Visitante'], r['Data']) for r in exist} [cite: 245]

                [cite_start]novas_linhas = [] [cite: 246]
            
                [cite_start]for j in jogos_finished: [cite: 246]
                    [cite_start]key = (j["Mandante"], j["Visitante"], j["Data"]) [cite: 246]
                    [cite_start]if key not in keys_exist: [cite: 246]
           
                        [cite_start]novas_linhas.append([ [cite: 246]
          
                            [cite_start]j["Mandante"], j["Visitante"], j["Gols Mandante"], j["Gols Visitante"], [cite: 247]
                            [cite_start]j["Gols Mandante 1T"], j["Gols Visitante 1T"], [cite: 247]
                     
                       
                            [cite_start]j["Gols Mandante 2T"], j["Gols Visitante 2T"], j["Data"] [cite: 248]
                        [cite_start]]) [cite: 248]

                [cite_start]if novas_linhas: [cite: 248]
                    [cite_start]ws_past.append_rows(novas_linhas) [cite: 248]
                    [cite_start]logging.info(f"✅ {len(novas_linhas)} jogos adicionados ao histórico de {aba_past}.") [cite: 248]

 
                [cite_start]if aba_past in SHEET_CACHE: del SHEET_CACHE[aba_past] [cite: 249]
      
            [cite_start]except Exception as e: [cite: 249]
                [cite_start]logging.error(f"Erro ao inserir dados na planilha {aba_past}: {e}") [cite: 249]

        # [cite_start]2. ATUALIZAÇÃO DO CACHE DE FUTUROS JOGOS (ABA_FUTURE) [cite: 249]
        [cite_start]aba_future = aba_config['sheet_future'] [cite: 249]
        
  
        [cite_start]try: ws_future = sh.worksheet(aba_future) [cite: 250]
        [cite_start]except WorksheetNotFound: [cite: 250]
            [cite_start]logging.warning(f"Aba de futuros jogos '{aba_future}' não encontrada. [cite: 251] Ignorando...")
            [cite_start]continue [cite: 251]

        [cite_start]jogos_future = buscar_jogos(aba_code, "ALL") [cite: 251]
        [cite_start]await asyncio.sleep(10) # Pausa para respeitar limite de rate da API [cite: 251]

        [cite_start]try: [cite: 251]
            [cite_start]ws_future.clear() [cite: 251]
            [cite_start]ws_future.update(values=[['Mandante', 'Visitante', 'Data/Hora', 'Matchday']], range_name='A1:D1') [cite: 251]

            [cite_start]if jogos_future: [cite: 251]
          
                [cite_start]linhas_future = [] [cite: 252]

                [cite_start]for m in jogos_future: [cite: 252]
                    [cite_start]matchday = m.get("matchday", "") [cite: 252]
                    [cite_start]utc_date = m.get('utcDate', '') [cite: 252]
    
             
                    [cite_start]if utc_date: [cite: 253]
  
                        [cite_start]try: [cite: 253]
                            [cite_start]data_utc = datetime.strptime(utc_date[:16], '%Y-%m-%dT%H:%M') [cite: 253]
                            # [cite_start]Limita a busca a jogos de [cite: 254] até 90 dias no futuro
        
                            [cite_start]if data_utc < datetime.now() + timedelta(days=90): [cite: 254]
                                [cite_start]linhas_future.append([ [cite: 254]
                       
                                    [cite_start]m.get("homeTeam", {}).get("name"), [cite: 255]
      
                                    [cite_start]m.get("awayTeam", {}).get("name"), [cite: 255]
                                    [cite_start]utc_date, [cite: 256]
       
                                    [cite_start]matchday [cite: 256]
                                [cite_start]]) [cite: 256]
      
                            [cite_start]except: [cite: 257]
                                [cite_start]continue [cite: 257]

             
                [cite_start]if linhas_future: [cite: 257]
                    [cite_start]ws_future.append_rows(linhas_future, value_input_option='USER_ENTERED') [cite: 257]
   
                    [cite_start]logging.info(f"✅ {len(linhas_future)} jogos futuros atualizados no cache de {aba_future}.") [cite: 258]
                [cite_start]else: [cite: 258]
                    [cite_start]logging.info(f"⚠️ Nenhuma partida agendada para {aba_code}. [cite: 259] Cache {aba_future} limpo.")

        [cite_start]except Exception as e: [cite: 259]
            [cite_start]logging.error(f"Erro ao atualizar cache de futuros jogos em {aba_future}: {e}") [cite: 259]

        [cite_start]await asyncio.sleep(3) # Pausa entre ligas [cite: 259]

# =================================================================================
# 📈 FUNÇÕES DE CÁLCULO E FORMATAÇÃO DE ESTATÍSTICAS
# =================================================================================
[cite_start]def calcular_estatisticas_time(time, aba, ultimos=None, casa_fora=None): [cite: 259]
    [cite_start]"""Calcula estatísticas detalhadas para um time em uma liga.""" [cite: 259]

    # [cite_start]Dicionário de resultados (Inicialização completa e detalhada) [cite: 259]
    [cite_start]d = {"time":time,"jogos_time":0,"jogos_casa":0,"jogos_fora":0, [cite: 259]
         [cite_start]"over15":0,"over15_casa":0,"over15_fora":0, [cite: 260] 
         # [cite_start]GAT ADICIONADO AQUI [cite: 260]
         [cite_start]"over25":0,"over25_casa":0,"over25_fora":0, [cite: 260]
         [cite_start]"btts":0,"btts_casa":0,"btts_fora":0, "g_a_t":0,"g_a_t_casa":0,"g_a_t_fora":0, "over05_1T":0,"over05_1T_casa":0,"over05_1T_fora":0, [cite: 260]
         [cite_start]"over05_2T":0,"over05_2T_casa":0,"over05_2T_fora":0, "over15_2T":0,"over15_2T_casa":0,"over15_2T_fora":0, [cite: 260]
         [cite_start]"gols_marcados":0,"gols_sofridos":0, "gols_marcados_casa":0,"gols_sofridos_casa":0, [cite: 260]
         [cite_start]"gols_marcados_fora":0,"gols_sofridos_fora":0, "total_gols":0,"total_gols_casa":0,"total_gols_fora":0, [cite: 260]
         [cite_start]"gols_marcados_1T":0,"gols_sofridos_1T":0, "gols_marcados_2T":0,"gols_sofridos_2T":0, [cite: 260]
         [cite_start]"gols_marcados_1T_casa":0,"gols_sofridos_1T_casa":0, "gols_marcados_1T_fora":0,"gols_sofridos_1T_fora":0, [cite: 260]
         [cite_start]"gols_marcados_2T_casa":0,"gols_sofridos_2T_casa":0, "gols_marcados_2T_fora":0,"gols_sofridos_2T_fora":0} [cite: 260]

    [cite_start]try: [cite: 260]
    
        [cite_start]linhas = get_sheet_data(aba) [cite: 261]
    [cite_start]except: [cite: 261]
   
        [cite_start]return {"time":time, "jogos_time": 0} [cite: 261]

    # [cite_start]Aplica filtro casa/fora [cite: 261]
    [cite_start]if casa_fora=="casa": [cite: 261]
        [cite_start]linhas = [l for l in linhas if l['Mandante']==time] [cite: 261]
    [cite_start]elif casa_fora=="fora": [cite: 261]
        [cite_start]linhas = [l for l in linhas if l['Visitante']==time] [cite: 261]
    [cite_start]else: [cite: 261]
        [cite_start]linhas = [l for l in linhas if l['Mandante']==time or l['Visitante']==time] [cite: 261]

    [cite_start]# [cite: 262] Ordena e filtra os N últimos jogos
    [cite_start]try: [cite: 262]
      
        [cite_start]linhas.sort(key=lambda x: datetime.strptime(x['Data'], "%d/%m/%Y"), reverse=False) [cite: 262]
    [cite_start]except: pass [cite: 262]

    [cite_start]if ultimos: [cite: 262]
        [cite_start]linhas = linhas[-ultimos:] [cite: 262]

    [cite_start]for linha in linhas: [cite: 262]
        [cite_start]em_casa = (time == linha['Mandante']) [cite: 262]
        [cite_start]gm, gv = safe_int(linha['Gols Mandante']), safe_int(linha['Gols Visitante']) [cite: 262]
        [cite_start]gm1, gv1 = safe_int(linha['Gols Mandante 1T']), safe_int(linha['Gols Visitante 1T']) [cite: 263]
        [cite_start]gm2, gv2 = gm-gm1, gv-gv1 [cite: 263]

        [cite_start]total, total1, total2 = gm+gv, gm1+gv1, gm2+gv2 [cite: 263]
        [cite_start]d["jogos_time"] += 1 [cite: 263]

        [cite_start]if em_casa: [cite: 263]
            [cite_start]marcados, sofridos = gm, gv [cite: 263]
            [cite_start]d["jogos_casa"] += 1 [cite: 263]
            [cite_start]d["gols_marcados_1T_casa"] += gm1 [cite: 263]
            [cite_start]d["gols_sofridos_1T_casa"] += gv1 [cite: 264]
            [cite_start]d["gols_marcados_2T_casa"] += gm2 [cite: 264]
        
            [cite_start]d["gols_sofridos_2T_casa"] += gv2 [cite: 264]
        [cite_start]else: [cite: 264]
            [cite_start]marcados, sofridos = gv, gm [cite: 264]
            [cite_start]d["jogos_fora"] += 1 [cite: 264]
            [cite_start]d["gols_marcados_1T_fora"] += gv1 [cite: 264]
            
            [cite_start]d["gols_sofridos_1T_fora"] += gm1 [cite: 265]
            [cite_start]d["gols_marcados_2T_fora"] += gv2 [cite: 265]
            # [cite_start]CORREÇÃO DE ERRO DE LÓGICA: Gols sofridos no 2T fora é (Gols Mandante 2T) = gm2, não gm1 [cite: 265]
            [cite_start]d["gols_sofridos_2T_fora"] += gm2 [cite: 265]

        [cite_start]d["gols_marcados"] += marcados [cite: 265]
        [cite_start]d["gols_sofridos"] += sofridos [cite: 265]
        [cite_start]if em_casa: [cite: 265]
           
            [cite_start]d["gols_marcados_casa"] += marcados [cite: 266]
            [cite_start]d["gols_sofridos_casa"] += sofridos [cite: 266]
        [cite_start]else: [cite: 266]
            [cite_start]d["gols_marcados_fora"] += marcados [cite: 266]
            [cite_start]d["gols_sofridos_fora"] += sofridos [cite: 266]

       
        [cite_start]d["total_gols"] += total [cite: 266]
        [cite_start]if em_casa: d["total_gols_casa"] += total [cite: 266]
        [cite_start]else: d["total_gols_fora"] += total [cite: 267]

       
        [cite_start]if total>1.5: d["over15"] += 1 [cite: 267]
        [cite_start]if total>2.5: d["over25"] += 1 [cite: 267]
        [cite_start]if gm>0 and gv>0: d["btts"] += 1 [cite: 267]
        [cite_start]if total1>0.5: d["over05_1T"] += 1 [cite: 267]
        [cite_start]if total2>0.5: d["over05_2T"] += 1 [cite: 267]
        [cite_start]if total2>1.5: d["over15_2T"] += 1 [cite: 267]

        # [cite_start]GAT (Gol em Ambos os Tempos) - NOVO CÁLCULO [cite: 267]
        [cite_start]gol_no_1t = total1 > 0 [cite: 267]
    
        [cite_start]gol_no_2t = total2 > 0 [cite: 268]
        [cite_start]if gol_no_1t and gol_no_2t: [cite: 268]
            [cite_start]d["g_a_t"] += 1 [cite: 268]
            [cite_start]d["g_a_t_casa" if em_casa else "g_a_t_fora"] += 1 [cite: 268]

        # [cite_start]Estatísticas por condição (casa/fora) [cite: 268]
        [cite_start]d["over15_casa" if em_casa else "over15_fora"] += (1 if total > 1.5 else 0) [cite: 268]
        [cite_start]d["over25_casa" if em_casa else "over25_fora"] += (1 if total [cite: 269] > 2.5 else 0)
        [cite_start]d["btts_casa" if em_casa else "btts_fora"] += (1 if gm > 0 and gv > 0 else 0) [cite: 269]
        [cite_start]d["over05_1T_casa" if em_casa else "over05_1T_fora"] += (1 if total1 > 0.5 else 0) [cite: 269]
    
        [cite_start]d["over05_2T_casa" if em_casa else "over05_2T_fora"] += (1 if total2 > 0.5 else 0) [cite: 269]
        [cite_start]d["over15_2T_casa" if em_casa else "over15_2T_fora"] += (1 if total2 > 1.5 else 0) [cite: 269]

        [cite_start]d["gols_marcados_1T"] [cite: 270] += gm1 if em_casa else gv1
        [cite_start]d["gols_sofridos_1T"] += gv1 if em_casa else gm1 [cite: 270]
        [cite_start]d["gols_marcados_2T"] += gm2 if em_casa else gv2 [cite: 270]
        [cite_start]d["gols_sofridos_2T"] += gv2 if em_casa else gm2 # CORREÇÃO DE ERRO DE LÓGICA (uso de gm2) [cite: 270]

    [cite_start]return d [cite: 270]

[cite_start]def formatar_estatisticas(d): [cite: 270]
  
    [cite_start]"""Formata o dicionário de estatísticas para a mensagem do Telegram.""" [cite: 270]
    [cite_start]jt, jc, jf = d["jogos_time"], d.get("jogos_casa", 0), d.get("jogos_fora", 0) [cite: 270]

    [cite_start]if jt == 0: return f"⚠️ **Nenhum [cite: 271] jogo encontrado** para **{escape_markdown(d['time'])}** com o filtro selecionado."
    
    [cite_start]return (f"📊 **Estatísticas - {escape_markdown(d['time'])}**\n" [cite: 271]
            [cite_start]f"📅 Jogos: {jt} (Casa: {jc} | [cite: 272] Fora: {jf})\n\n"
            [cite_start]f"⚽ Over 1.5: **{pct(d['over15'], jt)}** (C: {pct(d.get('over15_casa',0), jc)} | F: {pct(d.get('over15_fora',0), jf)})\n" [cite: 272]
            [cite_start]f"⚽ Over 2.5: **{pct(d['over25'], jt)}** (C: {pct(d.get('over25_casa',0), jc)} | F: {pct(d.get('over25_fora',0), jf)})\n" [cite: 272]
            [cite_start]f"🔁 BTTS: **{pct(d['btts'], jt)}** (C: {pct(d.get('btts_casa',0), jc)} | F: {pct(d.get('btts_fora',0), jf)})\n" [cite: 272]
            [cite_start]f"🥅 **G.A.T. [cite: 273] (Gol em Ambos os Tempos)[cite_start]: {pct(d.get('g_a_t',0), jt)}** (C: {pct(d.get('g_a_t_casa',0), jc)} | F: {pct(d.get('g_a_t_fora',0), jf)})\n\n" # LINHA G.A.T. ADICIONADA [cite: 273]
            
            [cite_start]f"⏱️ 1ºT Over 0.5: {pct(d['over05_1T'], jt)} (C: {pct(d['over05_1T_casa'], jc)} | F: {pct(d['over05_1T_fora'], jf)})\n" [cite: 273]
            [cite_start]f"⏱️ 2ºT Over 0.5: {pct(d['over05_2T'], jt)} (C: {pct(d['over05_2T_casa'], jc)} | F: {pct(d['over05_2T_fora'], jf)})\n" [cite: 273]
            [cite_start]f"⏱️ 2ºT Over 1.5: {pct(d['over15_2T'], jt)} (C: {pct(d['over15_2T_casa'], jc)} | F: [cite: 274] {pct(d['over15_2T_fora'], jf)})\n\n"
            
            [cite_start]f"➕ **Média gols marcados:** {media(d['gols_marcados'], jt)} (C: {media(d.get('gols_marcados_casa',0), jc)} | F: {media(d.get('gols_marcados_fora',0), jf)})\n" [cite: 274]
           
            [cite_start]f"➖ **Média gols sofridos:** {media(d['gols_sofridos'], jt)} (C: {media(d.get('gols_sofridos_casa',0), jc)} | F: {media(d.get('gols_sofridos_fora',0), jf)})\n\n" [cite: 274]

            [cite_start]f"⏱️ Média gols 1ºT (GP/GC): {media(d['gols_marcados_1T'], jt)} / {media(d['gols_sofridos_1T'], jt)}\n" [cite: 274]
       
            [cite_start]f"⏱️ Média gols 2ºT (GP/GC): {media(d['gols_marcados_2T'], jt)} / {media(d['gols_sofridos_2T'], jt)}\n\n" [cite: 275]
            
            [cite_start]f"🔢 **Média total de gols:** {media(d['total_gols'], jt)} (C: {media(d.get('total_gols_casa',0), jc)} | [cite: 276] F: {media(d.get('total_gols_fora',0), jf)})"
    [cite_start]) [cite: 276]

[cite_start]def listar_ultimos_jogos(time, aba, ultimos=None, casa_fora=None): [cite: 276]
    [cite_start]"""Lista os últimos N jogos de um time com filtros.""" [cite: 276]
    [cite_start]try: linhas = get_sheet_data(aba) [cite: 276]
    [cite_start]except: return f"⚠️ Erro ao ler dados da planilha para {escape_markdown(time)}." [cite: 277]

    [cite_start]if casa_fora == "casa": [cite: 277]
        [cite_start]linhas = [l for l in linhas if l['Mandante'] == time] [cite: 277]
    [cite_start]elif casa_fora == "fora": [cite: 277]
        [cite_start]linhas = [l for l in linhas if l['Visitante'] == time] [cite: 277]
    [cite_start]else: [cite: 277]
  
        [cite_start]linhas = [l for l in linhas if l['Mandante'] == time or l['Visitante'] == time] [cite: 277]

 
    [cite_start]try: linhas.sort(key=lambda x: datetime.strptime(x['Data'], "%d/%m/%Y"), reverse=False) [cite: 277]
    [cite_start]except: pass [cite: 277]

    [cite_start]if ultimos: [cite: 277]
        [cite_start]linhas = linhas[-ultimos:] [cite: 277]

    [cite_start]if not linhas: return f"Nenhum jogo encontrado para **{escape_markdown(time)}** com o filtro selecionado." [cite: 277]

    [cite_start]texto_jogos = "" [cite: 277]
    [cite_start]for l in linhas: [cite: 277]
        [cite_start]data = l['Data'] [cite: 277]
        [cite_start]gm, gv = safe_int(l['Gols Mandante']), safe_int(l['Gols Visitante']) [cite: 278]

        [cite_start]if l['Mandante'] == time: [cite: 278]
        
            [cite_start]oponente = escape_markdown(l['Visitante']) [cite: 278]
            [cite_start]condicao = "(CASA)" [cite: 278]
            [cite_start]m_cor = "🟢" if gm > gv else ("🟡" if gm == gv else "🔴") [cite: 278]
            [cite_start]texto_jogos += f"{m_cor} {data} {condicao}: **{escape_markdown(time)}** {gm} x {gv} {oponente}\n" [cite: 279]
   
        [cite_start]else: [cite: 279]
            [cite_start]oponente = escape_markdown(l['Mandante']) [cite: 279]
           
            [cite_start]condicao = "(FORA)" [cite: 279]
            [cite_start]m_cor = "🟢" if gv > gm else ("🟡" if gv == gm else "🔴") [cite: 279]
            [cite_start]texto_jogos += f"{m_cor} {data} {condicao}: {oponente} {gm} x {gv} **{escape_markdown(time)}**\n" [cite: 279]

    [cite_start]return texto_jogos [cite: 279]

# =================================================================================
# [cite_start]🤖 FUNÇÕES [cite: 280] DO BOT: HANDLERS E FLUXOS
# =================================================================================
[cite_start]async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE): [cite: 280]
    [cite_start]text = ( [cite: 280]
        [cite_start]"👋 Bem-vindo ao **Bot de Estatísticas de Confronto**!\n\n" [cite: 280]
        [cite_start]"Selecione um comando para começar:\n" [cite: 280]
        [cite_start]"• **/stats** 📊: Inicia a análise estatística de um confronto futuro ou ao vivo." [cite: 280]
    [cite_start]) [cite: 280]
    [cite_start]await update.message.reply_text(text, parse_mode='Markdown') [cite: 280]

[cite_start]async def listar_competicoes(update: Update, context: ContextTypes.DEFAULT_TYPE): [cite: 280]
    [cite_start]"""Primeira tela: Lista todas as competições.""" [cite: 280]
    [cite_start]title = "📊 **Estatísticas de Confronto:** Escolha a [cite: 281] Competição:"

    [cite_start]keyboard = [] [cite: 281]
    [cite_start]abas_list = list(LIGAS_MAP.keys()) [cite: 281]
    [cite_start]for i in range(0, len(abas_list), 3): [cite: 281]
        [cite_start]row = [] [cite: 281]
        [cite_start]for aba in abas_list[i:i + 3]: [cite: 281]
            [cite_start]row.append(InlineKeyboardButton(aba, callback_data=f"c|{aba}")) [cite: 281]
        [cite_start]keyboard.append(row) [cite: 281]

    [cite_start]reply_markup = InlineKeyboardMarkup(keyboard) [cite: 281]

    [cite_start]if update.message: [cite: 281]
        [cite_start]await update.message.reply_text(title, reply_markup=reply_markup, parse_mode='Markdown') [cite: 281]
    [cite_start]else: [cite: 282]
        [cite_start]# [cite: 282] Se for um callback, edita a mensagem anterior
        [cite_start]try: [cite: 282]
            [cite_start]await update.callback_query.edit_message_text(title, reply_markup=reply_markup, parse_mode='Markdown') [cite: 282]
        [cite_start]except BadRequest: [cite: 282]
  
            # [cite_start]Fallback: Se a edição falhar, envia nova mensagem [cite: 282]
             [cite_start]await update.callback_query.message.reply_text(title, reply_markup=reply_markup, parse_mode='Markdown') [cite: 282]


[cite_start]async def mostrar_menu_status_jogo(update: Update, context: ContextTypes.DEFAULT_TYPE, aba_code: str): [cite: 282]
    """
    [cite_start]Segundo menu: Escolhe entre Jogos AO VIVO e [cite: 283] Próximos Jogos (Future).
    """

    [cite_start]title = f"**{aba_code}** - Escolha o Tipo de Partida:" [cite: 283]

    [cite_start]keyboard = [ [cite: 283]
        [cite_start][InlineKeyboardButton("🔴 AO VIVO (API)", callback_data=f"STATUS|LIVE|{aba_code}")], [cite: 283]
        [cite_start][InlineKeyboardButton("📅 PRÓXIMOS JOGOS (Planilha)", callback_data=f"STATUS|FUTURE|{aba_code}")], [cite: 283]
        [cite_start][InlineKeyboardButton("⬅️ Voltar para Ligas", callback_data="VOLTAR_LIGA")] [cite: 283]
    [cite_start]] [cite: 283]
    [cite_start]reply_markup = InlineKeyboardMarkup(keyboard) [cite: 283]
    
    [cite_start]try: [cite: 283]
        [cite_start]await update.callback_query.edit_message_text(title, reply_markup=reply_markup, parse_mode='Markdown') [cite: 283]
    [cite_start]except Exception as e: [cite: 284]
       
        [cite_start]logging.error(f"ERRO ao editar mensagem em mostrar_menu_status_jogo (c|{aba_code}): {e}") [cite: 284]
        [cite_start]await update.callback_query.message.reply_text( [cite: 284]
            [cite_start]f"**{aba_code}** - Escolha o Tipo de Partida:", [cite: 284]
            [cite_start]reply_markup=reply_markup, [cite: 284]
            [cite_start]parse_mode='Markdown' [cite: 284]
        [cite_start]) [cite: 284]


[cite_start]async def listar_jogos(update: Update, context: ContextTypes.DEFAULT_TYPE, aba_code: str, status: str): [cite: 284]
    [cite_start]"""Terceira tela: Lista jogos futuros (GSheets) ou ao vivo (API).""" [cite: 284]
    [cite_start]jogos_a_listar = [] [cite: 284]

    [cite_start]if [cite: 285] status == "FUTURE":
 
        [cite_start]try: [cite: 285]
            [cite_start]await update.callback_query.edit_message_text( [cite: 285]
                [cite_start]f"⏳ Buscando os próximos **{MAX_GAMES_LISTED}** jogos em **{aba_code}** (Planilha)...", [cite: 286] 
                [cite_start]parse_mode='Markdown' [cite: 286]
            [cite_start]) [cite: 286]
        [cite_start]except Exception as e: [cite: 286]
            
            [cite_start]logging.error(f"Erro ao editar mensagem de loading FUTURE: {e}") [cite: 286]
            [cite_start]pass [cite: 286] 

        [cite_start]jogos_agendados = get_sheet_data_future(aba_code) [cite: 286]

        [cite_start]jogos_futuros_filtrados = [] [cite: 286]
        [cite_start]agora_utc = datetime.now(timezone.utc).replace(tzinfo=None) [cite: 286]

        [cite_start]for jogo in jogos_agendados: [cite: 286]
            [cite_start]try: [cite: 287]
                [cite_start]data_utc = datetime.strptime(jogo['Data_Hora'][:16], '%Y-%m-%dT%H:%M') [cite: 287]
        
                [cite_start]if data_utc > agora_utc: [cite: 287]
                    [cite_start]jogos_futuros_filtrados.append(jogo) [cite: 287]
            [cite_start]except Exception as e: [cite: 287]
                [cite_start]logging.warning(f"Erro ao parsear data de jogo futuro: {e}") [cite: 287]
                [cite_start]continue [cite: 287]

   
        [cite_start]jogos_agendados = jogos_futuros_filtrados [cite: 288]

        [cite_start]if not jogos_agendados: [cite: 288]
            [cite_start]await update.callback_query.edit_message_text( [cite: 288]
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

        keyboard = []
        for jogo in jogos_a_listar:
            try:
                M_full = jogo['Mandante_Nome']
                V_full = jogo['Visitante_Nome']
                data_str = jogo['Data_Hora']
                try:
                    data_utc = datetime.strptime(data_str[:16], '%Y-%m-%dT%H:%M')
                    matchday_num = jogo.get('Matchday', "N/A")
                    data_local = data_utc - timedelta(hours=3) # Ajuste para UTC-3 (Horário de Brasília)
                    data_label = data_local.strftime('%d/%m %H:%M')
                except ValueError:
                    data_label = data_str
                    matchday_num = "N/A"

                M_safe = escape_markdown(M_full)
                V_safe = escape_markdown(V_full)
                label = f"({matchday_num}) {data_label} | {M_safe} x {V_safe}"
                
                # O callback_data não deve conter caracteres que precisam de escape
                callback_data = f"JOGO|{aba_code}|{M_full}|{V_full}"
                
                # Se o callback for muito longo (limite do Telegram é 64 bytes), tenta encurtar.
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
            pass

        jogos_a_listar = buscar_jogos_live(aba_code)

        if not jogos_a_listar:
            await update.callback_query.edit_message_text(
                f"⚠️ **Nenhum jogo AO VIVO** encontrado em **{aba_code}** no momento.",
                parse_mode='Markdown'
            )
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
            callback_data = f"JOGO|{aba_code}|{M_full}|{V_full}"
            
            # Se o callback for muito longo, tenta encurtar.
            if len(callback_data.encode('utf-8')) > 64:
                M_safe_short = M_full.split(' ')[0][:8]
                V_safe_short = V_full.split(' ')[0][:8]
                callback_data = f"JOGO|{aba_code}|{M_safe_short}|{V_safe_short}"
                label = f"🔴 {tempo} | {M_safe_short} {placar_m} x {placar_v} {V_safe_short}"

            keyboard.append([InlineKeyboardButton(label, callback_data=callback_data)])

    keyboard.append([InlineKeyboardButton("⬅️ Voltar para Status", callback_data=f"VOLTAR_LIGA_STATUS|{aba_code}")])
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.callback_query.edit_message_text(
        f"**SELECIONE A PARTIDA** ({aba_code} - **{matchday_label}**):",
        reply_markup=reply_markup,
        parse_mode='Markdown'
    )


# =================================================================================
# ✅ FUNÇÃO CORRIGIDA: MOSTRA MENU DE FILTROS (Após selecionar o JOGO)
# O menu agora é SEMPRE enviado como NOVA MENSAGEM.
# =================================================================================
async def mostrar_menu_acoes(update: Update, context: ContextTypes.DEFAULT_TYPE, aba_code: str, mandante: str, visitante: str):
    [cite_start]""" Quarta tela: Menu para escolher o filtro de Estatísticas/Resultados. [cite: 203] [cite_start]CORREÇÃO: SEMPRE envia o menu de filtros como uma NOVA MENSAGEM. [cite: 204] """
    [cite_start]m_sanitized = escape_markdown(mandante) [cite: 204]
    [cite_start]v_sanitized = escape_markdown(visitante) [cite: 204]
    [cite_start]title = f"Escolha o filtro para o confronto **{m_sanitized} x {v_sanitized}**:" [cite: 204]
    
    [cite_start]keyboard = [] [cite: 204]
    # [cite_start]Cria botões para Estatísticas e Resultados (Mandante/Visitante implícito) [cite: 204]
    [cite_start]for idx, (label, tipo_filtro, ultimos, condicao_m, condicao_v) in enumerate(CONFRONTO_FILTROS): [cite: 204]
        # [cite_start]O callback agora inclui Mandante e Visitante para o cálculo final [cite: 204]
        callback_data = f"{tipo_filtro}|{aba_code}|{mandante}|{visitante}|{idx}" # Usa nome sem escape aqui para evitar problemas de callback
        [cite_start]keyboard.append([InlineKeyboardButton(label, callback_data=callback_data)]) [cite: 204]
        
    # [cite_start]Opções de Voltar [cite: 204]
    [cite_start]keyboard.append([InlineKeyboardButton("⬅️ Voltar para Jogos", callback_data=f"VOLTAR_LIGA_STATUS|{aba_code}")]) [cite: 204]
    [cite_start]reply_markup = InlineKeyboardMarkup(keyboard) [cite: 204]
    
    # [cite_start]<<<<<<<<<<<<<< CORREÇÃO PRINCIPAL AQUI >>>>>>>>>>>>>>>>> [cite: 204]
    # [cite_start]Apenas envia a mensagem como uma nova resposta, mantendo o histórico [cite: 204]
    [cite_start]await update.effective_message.reply_text(title, reply_markup=reply_markup, parse_mode='Markdown') [cite: 204]
    
    # [cite_start]É importante responder ao callback para fechar o relógio [cite: 205] de loading no botão clicado (JOGO|...)
    # [cite_start]Este 'answer()' é crucial quando a função é chamada após um clique em 'JOGO|...' [cite: 205]
    await update.callback_query.answer()


# =================================================================================
# ✅ FUNÇÃO CORRIGIDA: EXIBIÇÃO DAS ESTATÍSTICAS
# Envia como NOVA MENSAGEM e reexibe o menu de filtros.
# =================================================================================
async def exibir_estatisticas(update: Update, context: ContextTypes.DEFAULT_TYPE, mandante: str, visitante: str, aba_code: str, filtro_idx: int):
    [cite_start]""" Exibe as estatísticas detalhadas. CORREÇÃO DE UX: Envia o resultado como NOVA MENSAGEM e REEXIBE o menu. [cite: 206] """
    [cite_start]if not (0 <= filtro_idx < len(CONFRONTO_FILTROS)): return [cite: 206]
    
    # [cite_start]Filtro: (Label, Tipo, Últimos, Condicao_M, Condicao_V) [cite: 206]
    [cite_start]_, _, ultimos, condicao_m, condicao_v = CONFRONTO_FILTROS[filtro_idx] [cite: 206]

    # [cite_start]Calcula estatísticas para ambos os times e concatena [cite: 206]
    [cite_start]d_m = calcular_estatisticas_time(mandante, aba_code, ultimos=ultimos, casa_fora=condicao_m) [cite: 206]
    [cite_start]d_v = calcular_estatisticas_time(visitante, aba_code, ultimos=ultimos, casa_fora=condicao_v) [cite: 207]
    
    # [cite_start]Gera o texto formatado para Mandante e Visitante [cite: 207]
    [cite_start]texto_estatisticas = ( [cite: 207]
        [cite_start]formatar_estatisticas(d_m) + "\n\n---\n\n" + formatar_estatisticas(d_v) [cite: 207]
    )
    
    # [cite_start]1. Responde com [cite: 207] a ESTATÍSTICA como uma NOVA MENSAGEM na conversa (UX solicitada)
    [cite_start]await update.effective_message.reply_text( [cite: 207]
        [cite_start]f"**Confronto:** {escape_markdown(mandante)} x {escape_markdown(visitante)}\n\n{texto_estatisticas}", [cite: 207]
        [cite_start]parse_mode='Markdown' [cite: 207]
    )
    
    # [cite_start]2. Reexibe o menu de opções logo abaixo da estatística (CORREÇÃO DE UX) [cite: 208]
    # [cite_start]É necessário chamar a função de menu, que enviará uma nova mensagem com os botões. [cite: 208]
    [cite_start]await mostrar_menu_acoes(update, context, aba_code, mandante, visitante) [cite: 208]
    
    # [cite_start]3. Fecha o relógio de loading do botão (sem pop-up) [cite: 208]
    await update.callback_query.answer()


# =================================================================================
# ✅ FUNÇÃO CORRIGIDA: EXIBIÇÃO DOS ÚLTIMOS RESULTADOS
# Envia como NOVA MENSAGEM e reexibe o menu de filtros.
# =================================================================================
async def exibir_ultimos_resultados(update: Update, context: ContextTypes.DEFAULT_TYPE, mandante: str, visitante: str, aba_code: str, filtro_idx: int):
    """ Exibe os últimos resultados. CORREÇÃO DE UX: Envia o resultado como NOVA MENSAGEM e REEXIBE o menu. """
    if not (0 <= filtro_idx < len(CONFRONTO_FILTROS)): return
    
    # Filtro: (Label, Tipo, Últimos, Condicao_M, Condicao_V)
    _, _, ultimos, condicao_m, condicao_v = CONFRONTO_FILTROS[filtro_idx]

    # Lista resultados para Mandante
    texto_m = listar_ultimos_jogos(mandante, aba_code, ultimos=ultimos, casa_fora=condicao_m)
    # Lista resultados para Visitante
    texto_v = listar_ultimos_jogos(visitante, aba_code, ultimos=ultimos, casa_fora=condicao_v)

    # Gera o texto formatado
    texto_resultados = (
        f"📅 **Últimos Resultados - {escape_markdown(mandante)}**\n"
        f"{texto_m}\n"
        f"-----------------------------\n"
        f"📅 **Últimos Resultados - {escape_markdown(visitante)}**\n"
        f"{texto_v}"
    )

    # 1. Responde com o RESULTADO como uma NOVA MENSAGEM na conversa (UX solicitada)
    await update.effective_message.reply_text(
        f"**Confronto:** {escape_markdown(mandante)} x {escape_markdown(visitante)}\n\n{texto_resultados}",
        parse_mode='Markdown'
    )
    
    # 2. Reexibe o menu de opções logo abaixo do resultado (CORREÇÃO DE UX)
    await mostrar_menu_acoes(update, context, aba_code, mandante, visitante)
    
    # 3. Fecha o relógio de loading do botão
    await update.callback_query.answer()


# =================================================================================
# 🔄 HANDLER CENTRAL DE CALLBACKS
# =================================================================================
async def callback_query_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Trata todos os callbacks dos botões inline."""
    query = update.callback_query
    await query.answer() # Responde ao callback imediatamente para tirar o 'relógio'

    data = query.data
    # logging.info(f"Callback data recebido: {data}")

    try:
        # Padrão: TIPO|LIGA|PARAM1|PARAM2|...

        # TELA 1: Escolha da Liga (c|LIGA_CODE)
        if data.startswith("c|"):
            _, aba_code = data.split('|')
            await mostrar_menu_status_jogo(update, context, aba_code)
            return

        # VOLTAR: Voltar para a lista de Ligas (VOLTAR_LIGA)
        elif data == "VOLTAR_LIGA":
            await listar_competicoes(update, context)
            return

        # TELA 2: Escolha do Status (STATUS|LIVE|LIGA_CODE ou STATUS|FUTURE|LIGA_CODE)
        elif data.startswith("STATUS|"):
            _, status, aba_code = data.split('|')
            await listar_jogos(update, context, aba_code, status)
            return
        
        # VOLTAR: Voltar para a escolha de Status da Liga (VOLTAR_LIGA_STATUS|LIGA_CODE)
        elif data.startswith("VOLTAR_LIGA_STATUS|"):
            _, aba_code = data.split('|')
            await mostrar_menu_status_jogo(update, context, aba_code)
            return

        # TELA 3: Escolha do Jogo (JOGO|LIGA_CODE|MANDANTE|VISITANTE)
        elif data.startswith("JOGO|"):
            # O nome do time aqui não tem escape, mas será sanitizado ao ser usado
            _, aba_code, mandante, visitante = data.split('|')
            # CORREÇÃO CRÍTICA: Envia o menu de ações como NOVA MENSAGEM
            await mostrar_menu_acoes(update, context, aba_code, mandante, visitante)
            # O answer() já está dentro de mostrar_menu_acoes

        # TELA 4: Escolha do Filtro (STATS_FILTRO|LIGA|MANDANTE|VISITANTE|INDEX)
        elif data.startswith("STATS_FILTRO|"):
            _, aba_code, mandante, visitante, idx_str = data.split('|')
            await exibir_estatisticas(update, context, mandante, visitante, aba_code, int(idx_str))
        
        # TELA 4: Escolha do Filtro (RESULTADOS_FILTRO|LIGA|MANDANTE|VISITANTE|INDEX)
        elif data.startswith("RESULTADOS_FILTRO|"):
            _, aba_code, mandante, visitante, idx_str = data.split('|')
            await exibir_ultimos_resultados(update, context, mandante, visitante, aba_code, int(idx_str))

    except Exception as e:
        logging.error(f"ERRO CRÍTICO no callback_query_handler: {e}", exc_info=True)
        try:
             await update.callback_query.edit_message_text("❌ Ocorreu um erro interno. Tente novamente iniciando com /stats.")
        except:
             await update.effective_message.reply_text("❌ Ocorreu um erro interno. Tente novamente iniciando com /stats.")


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
        job_queue.run_repeating(atualizar_planilhas, interval=3600, first=0, name="AtualizacaoPlanilhas")
        asyncio.run(pre_carregar_cache_sheets())
    else:
        logging.warning("Job Queue de atualização desativado: Conexão com GSheets não estabelecida.")
    
    logging.info("Bot rodando...")
    app.run_polling()

if __name__ == "__main__":
    main()
