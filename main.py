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
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, ContextTypes, JobQueue 
from telegram.error import BadRequest

# Configuração de Logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
# Aplica nest_asyncio para evitar erros se rodar dentro de um ambiente com loop de eventos já ativo (como notebooks, embora não seja o caso direto do Railway, é bom manter)
nest_asyncio.apply()

# ===== Variáveis de Configuração (LIDAS DE VARIÁVEIS DE AMBIENTE) =====
# Bot Token e API Key devem ser configurados como variáveis de ambiente no Railway
BOT_TOKEN = os.environ.get("BOT_TOKEN", "SEU_TOKEN_AQUI") 
API_KEY = os.environ.get("API_KEY", "SUA_API_KEY_AQUI")
SHEET_URL = "https://docs.google.com/spreadsheets/d/1ChFFXQxo1qQElNzh2OC8-UPgofRXxyVWN06ExBQ3YqY/edit?usp=drivesdk"

# Mapeamento de Ligas
LIGAS_MAP = {
    "CL": {"sheet_past": "CL", "sheet_future": "CL_FJ"},
    "BSA": {"sheet_past": "BSA", "sheet_future": "BSA_FJ"},
    # Adicione demais ligas aqui
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
# ✅ CONEXÃO GSHEETS VIA VARIÁVEL DE AMBIENTE (Corrigido o nome do módulo em log, era gspread)
# =================================================================================

CREDS_JSON = os.environ.get("GSPREAD_CREDS_JSON")
client = None

if not CREDS_JSON:
    logging.error("❌ ERRO DE AUTORIZAÇÃO GSHEET: Variável GSPREAD_CREDS_JSON não encontrada. Configure-a no Railway.")
else:
    try:
        # Cria um arquivo temporário para as credenciais JSON
        with tempfile.NamedTemporaryFile(mode="w", delete=False, encoding='utf-8') as tmp_file:
            tmp_file.write(CREDS_JSON)
            tmp_file_path = tmp_file.name
        
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(tmp_file_path, scope)
        client = gspread.authorize(creds)
        logging.info("✅ Conexão GSheets estabelecida via Variável de Ambiente.")
        # Remove o arquivo temporário
        os.remove(tmp_file_path)

    except Exception as e:
        logging.error(f"❌ ERRO DE AUTORIZAÇÃO GSHEET: Erro ao carregar ou autorizar credenciais JSON: {e}")
        client = None

# =================================================================================
# 💾 FUNÇÕES DE SUPORTE E HANDLERS (Preencha as suas funções originais aqui)
# =================================================================================
def safe_int(v):
    try: return int(v)
    except: return 0

# A função pre_carregar_cache_sheets e todas as que usam o 'client' só funcionarão se a conexão acima for bem-sucedida.
async def pre_carregar_cache_sheets():
    if not client:
        logging.warning("Pré-carregamento de cache ignorado: Conexão GSheets falhou ou cliente não definido.")
        return
    logging.info("Iniciando pré-carregamento de cache...")
    # Sua lógica real de carregamento de planilhas aqui. Ex:
    # try:
    #     sh = client.open_by_url(SHEET_URL)
    #     for liga_key, mapping in LIGAS_MAP.items():
    #         # Carrega a aba de histórico
    #         aba_past = sh.worksheet(mapping["sheet_past"])
    #         # ... sua lógica de leitura de dados ...
    #         logging.info(f"Cache de histórico para {liga_key} pré-carregado.")
    # except Exception as e:
    #     logging.error(f"Erro ao pré-carregar sheets: {e}")
    
async def atualizar_periodicamente(context: ContextTypes.DEFAULT_TYPE): # Aceita context para rodar via JobQueue
    # Sua lógica de atualização periódica aqui (ex: buscar jogos live, recalcular estatísticas)
    logging.info("Executando atualização periódica...")

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = "Bot iniciado com sucesso no Railway. Use /stats para começar."
    await update.message.reply_text(text)

async def listar_competicoes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = "Escolha a Competição."
    keyboard = [
        [InlineKeyboardButton("BSA", callback_data="c:BSA")],
        [InlineKeyboardButton("CL", callback_data="c:CL")]
    ]
    await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

async def callback_query_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    # Exemplo simples de resposta
    data = query.data
    liga = data.split(":")[1]
    await query.edit_message_text(f"Liga {liga} selecionada. (Continuação da sua lógica aqui)")

# =================================================================================
# ⚙️ FUNÇÃO PRINCIPAL (Polling) - AJUSTADA E CORRIGIDA FINALMENTE
# =================================================================================
def main():
    try:
        if not BOT_TOKEN or BOT_TOKEN == "SEU_TOKEN_AQUI":
            logging.error("O BOT_TOKEN não foi configurado. Configure no Railway.")
            return

        # 1. Cria o ApplicationBuilder
        application = ApplicationBuilder().token(BOT_TOKEN).build()
        
        # 2. CORREÇÃO FINAL DO JOBQUEUE: Inicializa o JobQueue sem argumentos
        job_queue = JobQueue() 
        
        # 3. Anexa o JobQueue ao Application e inicia
        application.job_queue = job_queue 
        # NOTA: O .start() não é estritamente necessário se usar application.run_polling(), mas é boa prática se for executar tarefas antes do polling.
        # job_queue.start() # Removendo para deixar o run_polling gerenciar o loop, mas se precisar de start manual, esta é a linha.

        # Comandos e Callbacks
        application.add_handler(CommandHandler("start", start_command))
        application.add_handler(CommandHandler("stats", listar_competicoes))
        application.add_handler(CallbackQueryHandler(callback_query_handler))

        # Pré-carregar cache (Chama a função assíncrona de forma síncrona, se o loop ainda não estiver rodando)
        asyncio.run(pre_carregar_cache_sheets())
        
        # 4. Agenda a tarefa de atualização periódica
        # O intervalo de 3600 segundos (1 hora) é definido no código, mas aqui usamos 0 para iniciar imediatamente após o bot subir.
        application.job_queue.run_repeating(
            atualizar_periodicamente,
            interval=CACHE_DURATION_SECONDS,
            first=0
        )
        
        logging.info("Bot iniciado em modo Polling no Railway...")
        application.run_polling()
        
    except Exception as e:
        logging.error(f"Erro na execução principal: {e}")

if __name__ == '__main__':
    main()
