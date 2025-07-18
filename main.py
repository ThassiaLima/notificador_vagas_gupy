# main.py

import os
import time
import pandas as pd
from datetime import datetime
from utils import buscar_vagas, enviar_email, atualizar_google_sheets

# --- Configurações ---
DATA_HOJE = datetime.now().strftime("%Y-%m-%d")
ARQUIVO_CSV = "historico_vagas.csv"

# Carrega as variáveis de ambiente (nomes ajustados para corresponder ao arquivo .yml)
# É fundamental que os nomes em os.getenv() sejam os mesmos definidos no 'env' do workflow.
EMAIL_RECEPTOR = os.getenv("EMAIL_DESTINO")
EMAIL_REMETENTE = os.getenv("EMAIL_REMETENTE")
SENHA_REMETENTE = os.getenv("SENHA_APP") # Corrigido de SENHA_REMETENTE para SENHA_APP

# Lista de palavras-chave para buscar
palavras_chave = ["Analista de BI", "Business Intelligence", "Data", "Dados", "Analytics", "Product", "Produto"]

# Dicionário de empresas com suas respectivas URLs de carreira na Gupy
empresas = {
    "Itaú": "https://vemproitau.gupy.io/",
    "Boticário": "https://grupoboticario.gupy.io/",
    "Raízen": "https://genteraizen.gupy.io/",
    "C&A": "https://cea.gupy.io/",
    "AmbevTech": "https://ambevtech.gupy.io",
    "OLX": "https://vemsergrupoolx.gupy.io",
    "Localiza": "https://localiza.gupy.io",
    "BMG": "https://bmg.gupy.io"
}

# --- 1. Buscar novas vagas ---
print("🚀 Iniciando busca de vagas...")
todas_vagas = []
# Alterado para iterar sobre o nome (empresa) e o valor (url) do dicionário
for empresa, url in empresas.items():
    for palavra in palavras_chave:
        print(f"🔍 Buscando por: '{palavra}' em '{empresa}'")
        # A função agora recebe a URL da empresa para construir o link de busca
        vagas = buscar_vagas(empresa=empresa, palavra_chave=palavra, url_empresa=url)
        todas_vagas.extend(vagas)
        time.sleep(1) # Pausa para não sobrecarregar os servidores

print(f"✅ Busca finalizada. Total de vagas encontradas na busca atual: {len(todas_vagas)}")

# Cria um DataFrame com as vagas encontradas na execução de hoje
df_novas = pd.DataFrame(todas_vagas)

# Se nenhuma vaga for encontrada na busca, encerra o script mais cedo.
if df_novas.empty:
    print("ℹ️ Nenhuma vaga encontrada na busca de hoje. O histórico não será alterado.")
    # Mesmo sem vagas novas, é bom atualizar o sheets para refletir possíveis fechamentos
else:
    df_novas["data_abertura"] = DATA_HOJE
    df_novas["data_fechamento"] = pd.NA
    df_novas["status"] = "ativa"


# --- 2. Carregar e Processar Histórico ---
print("📂 Carregando histórico de vagas...")
if os.path.exists(ARQUIVO_CSV):
    historico = pd.read_csv(ARQUIVO_CSV)
    # Garante que as colunas de data e status tenham o tipo correto
    historico["data_abertura"] = pd.to_datetime(historico["data_abertura"]).dt.strftime("%Y-%m-%d")
    historico["data_fechamento"] = historico["data_fechamento"].fillna(pd.NA)
    historico["status"] = historico["status"].fillna("ativa")
else:
    # Se o arquivo não existe, cria um histórico vazio com as colunas certas
    colunas = df_novas.columns if not df_novas.empty else ['empresa', 'titulo', 'local', 'link', 'data_abertura', 'data_fechamento', 'status']
    historico = pd.DataFrame(columns=colunas)

# --- 3. Comparar Vagas Atuais com Histórico ---
links_historico_ativos = historico[historico["status"].isin(["ativa", "reaberta"])]["link"].tolist()
links_atuais = [] if df_novas.empty else df_novas["link"].tolist()

# Detectar vagas genuinamente novas (nunca vistas antes)
vagas_para_notificar = pd.DataFrame()
if not df_novas.empty:
    vagas_para_notificar = df_novas[~df_novas["link"].isin(links_historico_ativos)]

# Adicionar vagas novas e reabertas ao histórico
if not vagas_para_notificar.empty:
    for _, row in vagas_para_notificar.iterrows():
        link = row["link"]
        # Se o link já existe no histórico (foi fechada e agora reapareceu)
        if link in historico["link"].tolist():
            # Atualiza o status da vaga existente para 'reaberta'
            historico.loc[historico["link"] == link, "status"] = "reaberta"
            historico.loc[historico["link"] == link, "data_fechamento"] = pd.NA # Limpa a data de fechamento
        else:
            # Se for uma vaga 100% nova, adiciona ao histórico
            historico = pd.concat([historico, pd.DataFrame([row])], ignore_index=True)

# --- 4. Atualizar status de vagas fechadas ---
# Vagas que estavam ativas/reabertas mas não foram encontradas na busca de hoje
vagas_fechadas_mask = historico["status"].isin(["ativa", "reaberta"]) & ~historico["link"].isin(links_atuais)

if vagas_fechadas_mask.any():
    print(f"🚪 Detectadas {vagas_fechadas_mask.sum()} vagas que foram fechadas.")
    historico.loc[vagas_fechadas_mask, "status"] = "fechada"
    historico.loc[vagas_fechadas_mask, "data_fechamento"] = DATA_HOJE
else:
    print("ℹ️ Nenhuma vaga foi fechada desde a última execução.")


# --- 5. Salvar e Enviar ---

# Salvar o arquivo CSV atualizado
historico.to_csv(ARQUIVO_CSV, index=False)
print(f"💾 Histórico salvo com {len(historico)} vagas em '{ARQUIVO_CSV}'.")

# Atualizar o Google Sheets
try:
    atualizar_google_sheets(historico)
except Exception as e:
    print(f"❌ Erro ao tentar atualizar Google Sheets: {e}")

# Enviar email com as novas vagas encontradas
if not vagas_para_notificar.empty and EMAIL_RECEPTOR and EMAIL_REMETENTE and SENHA_REMETENTE:
    print(f"📧 Encontradas {len(vagas_para_notificar)} vagas novas para notificar.")
    enviar_email(vagas_para_notificar, EMAIL_RECEPTOR, EMAIL_REMETENTE, SENHA_REMETENTE)
else:
    print("📭 Nenhuma vaga nova para enviar ou variáveis de e-mail não configuradas.")

print("✅ Processo concluído com sucesso!")