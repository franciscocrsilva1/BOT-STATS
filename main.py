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
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, ContextTypes, JobQueue # Adicionado JobQueue
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
# 💾 FUNÇÕES DE SUPORTE E CACHING (AS FUNÇÕES DE LÓGICA PERMANECEM NO SEU ARQUIVO)
# =================================================================================

def safe_int(v):
    try: return int(v)
    except: return 0
# ... (demais funções de suporte, cálculo e handlers devem estar aqui, mantidas por você) ...

async def pre_carregar_cache_sheets():
    # Simulação de pré-carregamento: no seu código real, esta função busca os dados
    logging.info("Iniciando pré-carregamento de cache...")
    pass

async def atualizar_periodicamente(intervalo=3600):
    while True:
        logging.info("Executando atualização periódica de planilhas...")
        # AQUI VOCÊ CHAMA SUA FUNÇÃO DE ATUALIZAÇÃO (asyncio.create_task(atualizar_planilhas()))
        await asyncio.sleep(intervalo) 

# --- HANDLERS E FUNÇÕES DE COMANDO/CALLBACKS (MANTENHA OS SEUS CÓDIGOS ORIGINAIS) ---
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = "Bot iniciado com sucesso no Railway. Use /stats para começar."
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

        # 1. Cria o ApplicationBuilder
        application = ApplicationBuilder().token(BOT_TOKEN).build()
        
        # 2. Cria o JobQueue separadamente (CORREÇÃO para o erro 'bool' object)
        job_queue = JobQueue(application)
        
        # 3. Anexa o JobQueue ao Application e inicia
        job_queue.start()
        application.job_queue = job_queue # Garantindo que o objeto JobQueue seja anexado

        # Comandos e Callbacks
        application.add_handler(CommandHandler("start", start_command))
        application.add_handler(CommandHandler("stats", listar_competicoes))
        application.add_handler(CallbackQueryHandler(callback_query_handler))

        # Pré-carregar cache (síncrono)
        asyncio.run(pre_carregar_cache_sheets())
        
        # 4. Chama o JobQueue para iniciar a tarefa de atualização
        application.job_queue.run_once(
            lambda context: asyncio.create_task(atualizar_periodicamente()),
            0 
        )
        
        logging.info("Bot iniciado em modo Polling no Railway...")
        application.run_polling()
        
    except Exception as e:
        logging.error(f"Erro na execução principal: {e}")

if __name__ == '__main__':
    main()
