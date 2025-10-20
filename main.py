# ===============================================================================
# 🏆 BOT DE ESTATÍSTICAS DE CONFRONTO V2.0.0 - VERSÃO FINAL CORRIGIDA (RAILWAY)
# ===============================================================================

# ===== Importações Essenciais =====
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests
from datetime import datetime, timedelta, timezone
import nest_asyncio
import asyncio
import logging
import os 
import tempfile 
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, ContextTypes
from telegram.error import BadRequest

# Configuração de Logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
nest_asyncio.apply()

# ===== Variáveis de Configuração (LIDAS DE VARIÁVEIS DE AMBIENTE) =====
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
# ✅ CONEXÃO GSHEETS VIA VARIÁVEL DE AMBIENTE
# =================================================================================

CREDS_JSON = os.environ.get("GSPREAD_CREDS_JSON")
client = None

if not CREDS_JSON:
    logging.error("❌ ERRO DE AUTORIZAÇÃO GSHEET: Variável GSPREAD_CREDS_JSON não encontrada. Configure-a no Railway.")
else:
    try:
        with tempfile.NamedTemporaryFile(mode="w", delete=False, encoding='utf-8') as tmp_file:
            tmp_file.write(CREDS_JSON)
            tmp_file_path = tmp_file.name
        
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(tmp_file_path, scope)
        client = gspread.authorize(creds)
        logging.info("✅ Conexão GSheets estabelecida via Variável de Ambiente.")
        os.remove(tmp_file_path)

    except Exception as e:
        logging.error(f"❌ ERRO DE AUTORIZAÇÃO GSHEET: Erro ao carregar ou autorizar credenciais JSON: {e}")
        client = None

# =================================================================================
# 💾 FUNÇÕES DE SUPORTE E CACHING (OMITIDAS PARA BREVIDADE, MANTIDAS NO SEU ARQUIVO)
# =================================================================================
# Funções: safe_int, pct, media, escape_markdown, get_sheet_data, get_sheet_data_future, 
# pre_carregar_cache_sheets, buscar_jogos, buscar_jogos_live, atualizar_planilhas, 
# atualizar_periodicamente, calcular_estatisticas_time, formatar_estatisticas, 
# listar_ultimos_jogos, start_command, help_command, listar_competicoes, 
# mostrar_menu_status_jogo, listar_jogos, mostrar_menu_acoes, 
# mostrar_menu_filtros_confronto, mostrar_menu_filtros_resultados, 
# exibir_estatisticas_confronto, exibir_ultimos_resultados, callback_query_handler
# =================================================================================

# Colocando funções de suporte mínimas para que o código seja executável
def safe_int(v):
    try: return int(v)
    except: return 0
# ... (demais funções de suporte, cálculo e handlers devem estar aqui) ...

async def pre_carregar_cache_sheets():
    # Esta função só roda o logging, sem tentar carregar, para demonstração
    logging.info("Iniciando pré-carregamento de cache...")
    pass

async def atualizar_periodicamente(intervalo=3600):
    while True:
        logging.info("Executando atualização periódica de planilhas...")
        await asyncio.sleep(intervalo) # Apenas para simulação. No seu código, chama atualizar_planilhas()

# --- HANDLERS E FUNÇÕES DE COMANDO/CALLBACKS ---
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = "Bot rodando corretamente no modo polling e JobQueue ativo."
    await update.message.reply_text(text)

async def listar_competicoes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = "Escolha a Competição (Simulado)."
    keyboard = [[InlineKeyboardButton("BSA", callback_data="c:BSA")]]
    await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

async def callback_query_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.edit_message_text(f"Comando recebido: {query.data}")
# Fim do bloco de handlers de simulação


# =================================================================================
# ⚙️ FUNÇÃO PRINCIPAL (Polling) - AJUSTADA E CORRIGIDA
# =================================================================================
def main():
    try:
        if not BOT_TOKEN or BOT_TOKEN == "SEU_TOKEN_AQUI":
            logging.error("O BOT_TOKEN não foi configurado. Configure no Railway.")
            return

        # CORREÇÃO CRÍTICA: Inicializar ApplicationBuilder com job_queue=True
        application = ApplicationBuilder().token(BOT_TOKEN).job_queue(True).build()
        #                                                          ^^^^^^^^^^^

        # Comandos e Callbacks
        application.add_handler(CommandHandler("start", start_command))
        application.add_handler(CommandHandler("stats", listar_competicoes))
        application.add_handler(CallbackQueryHandler(callback_query_handler))

        # Pré-carregar cache (síncrono)
        asyncio.run(pre_carregar_cache_sheets())
        
        # JobQueue agora funciona: Adiciona a tarefa de atualização periódica
        application.job_queue.run_once(
            lambda context: asyncio.create_task(atualizar_periodicamente()),
            0 # Inicia a primeira atualização imediatamente
        )
        
        logging.info("Bot iniciado em modo Polling no Railway...")
        application.run_polling()
        
    except Exception as e:
        logging.error(f"Erro na execução principal: {e}")

if __name__ == '__main__':
    main()
