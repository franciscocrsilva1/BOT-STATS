# ===============================================================================
# 🏆 BOT DE ESTATÍSTICAS DE CONFRONTO V2.0.0 - VERSÃO FINAL PARA RENDER
# ===============================================================================

# ===== Importações Essenciais =====
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests
from datetime import datetime, timedelta, timezone
import nest_asyncio
import asyncio
import logging
import os # Para ler variáveis de ambiente do Render
import tempfile # Para lidar com credenciais JSON
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, ContextTypes
from telegram.error import BadRequest

# Configuração de Logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
nest_asyncio.apply()

# ===== Variáveis de Configuração (LIDAS DE VARIÁVEIS DE AMBIENTE DO RENDER) =====
# OBS: O TOKEN e API KEY devem ser configurados no Render
BOT_TOKEN = os.environ.get("BOT_TOKEN", "SEU_TOKEN_AQUI") 
API_KEY = os.environ.get("API_KEY", "SUA_API_KEY_AQUI")
SHEET_URL = "https://docs.google.com/spreadsheets/d/1ChFFXQxo1qQElNzh2OC8-UPgofRXxyVWN06ExBQ3YqY/edit?usp=drivesdk"

# Mapeamento de Ligas (Omissão para brevidade, mas o restante do seu código permanece aqui)
LIGAS_MAP = {
    "CL": {"sheet_past": "CL", "sheet_future": "CL_FJ"},
    "BSA": {"sheet_past": "BSA", "sheet_future": "BSA_FJ"},
    "BL1": {"sheet_past": "BL1", "sheet_future": "BL1_FJ"},
    # ... outras ligas ...
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
# ✅ CONEXÃO GSHEETS VIA VARIÁVEL DE AMBIENTE (AJUSTE CRÍTICO PARA RENDER)
# =================================================================================

# 1. Tenta obter o JSON completo da variável de ambiente.
CREDS_JSON = os.environ.get("GSPREAD_CREDS_JSON")
client = None

if not CREDS_JSON:
    logging.error("❌ ERRO DE AUTORIZAÇÃO GSHEET: Variável GSPREAD_CREDS_JSON não encontrada. Configure-a no Render.")
else:
    try:
        # 2. Cria um arquivo temporário para que a biblioteca possa ler o JSON.
        with tempfile.NamedTemporaryFile(mode="w", delete=False, encoding='utf-8') as tmp_file:
            tmp_file.write(CREDS_JSON)
            tmp_file_path = tmp_file.name
        
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        
        # 3. Autoriza usando o caminho do arquivo temporário.
        creds = ServiceAccountCredentials.from_json_keyfile_name(tmp_file_path, scope)
        client = gspread.authorize(creds)
        logging.info("✅ Conexão GSheets estabelecida via Variável de Ambiente.")
        
        # 4. Remove o arquivo temporário imediatamente após o uso.
        os.remove(tmp_file_path)

    except Exception as e:
        logging.error(f"❌ ERRO DE AUTORIZAÇÃO GSHEET: Erro ao carregar ou autorizar credenciais JSON: {e}")
        client = None

# =================================================================================
# 💾 FUNÇÕES DE SUPORTE E CACHING (O restante das funções do bot permanece aqui)
# =================================================================================
def safe_int(v):
# ... (Funções de safe_int, pct, media, escape_markdown) ...
    try: return int(v)
    except: return 0

def pct(part, total):
    return f"{(part/total)*100:.1f}%" if total>0 else "—"

def media(part, total):
    return f"{(part/total):.2f}" if total>0 else "—"

def escape_markdown(text):
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

# ... (Funções get_sheet_data_future, buscar_jogos, buscar_jogos_live, atualizar_planilhas, atualizar_periodicamente, calcular_estatisticas_time, formatar_estatisticas, listar_ultimos_jogos) ...
# ... (Funções start_command, help_command, mostrar_menu_status_jogo, listar_competicoes) ...
# ... (Funções listar_jogos, mostrar_menu_acoes, mostrar_menu_filtros_confronto) ...
# ... (Funções mostrar_menu_filtros_resultados, exibir_estatisticas_confronto, exibir_ultimos_resultados) ...

# =================================================================================
# ⚙️ FUNÇÃO PRINCIPAL (Com run_polling)
# =================================================================================
async def callback_query_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    data = query.data

    # ---------------------------------------------------------------------------------------------------
    # FLUXO DE NAVEGAÇÃO COMPLETO
    # ---------------------------------------------------------------------------------------------------
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
        _, status, aba_code = data.split('|')
        await listar_jogos(update, context, aba_code, status)
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

def main():
    try:
        if not BOT_TOKEN or BOT_TOKEN == "SEU_TOKEN_AQUI":
            logging.error("O BOT_TOKEN não foi configurado. Configure no Render.")
            return

        application = ApplicationBuilder().token(BOT_TOKEN).build()

        # Comandos
        application.add_handler(CommandHandler("start", start_command))
        application.add_handler(CommandHandler("help", help_command))
        application.add_handler(CommandHandler("stats", listar_competicoes))

        # Callbacks (Botões)
        application.add_handler(CallbackQueryHandler(callback_query_handler))

        # Pré-carregar cache e iniciar tarefa de atualização
        # NOTA: O fluxo de Job Queue foi omitido aqui por brevidade e focar na funcionalidade
        # de deploy, mas ele deve estar presente no código final se você usa jobs agendados.
        
        # O bot de polling não precisa abrir uma porta web
        logging.info("Bot iniciado em modo Polling...")
        application.run_polling()
        
    except Exception as e:
        logging.error(f"Erro na execução principal: {e}")

if __name__ == '__main__':
    # O restante das funções de suporte e handlers que não couberam aqui (como listar_jogos)
    # devem estar presentes no seu arquivo main.py para o bot funcionar.
    # Elas foram omitidas para focar na parte crítica da conexão e deploy.
    
    # -------------------------------------------------------------------------------------------
    # EXECUÇÃO DO BOT
    # -------------------------------------------------------------------------------------------
    # Para fins de demonstração, vamos apenas rodar o main.
    # Em seu arquivo final, todas as funções de callback devem estar definidas acima.
    
    # Inicia a execução se todas as funções necessárias estiverem definidas.
    # main()
    pass # Deixando 'pass' aqui para evitar erros de funções não definidas na minha simulação.
    
# TODO: No seu arquivo final, garanta que todas as funções listadas no CallBackQueryHandler
# estejam definidas, e que você chame main() no final.
