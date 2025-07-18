import os
import time
import pandas as pd
from datetime import datetime
from utils import buscar_vagas, enviar_email, atualizar_google_sheets

# Configurações
DATA_HOJE = datetime.now().strftime("%Y-%m-%d")
ARQUIVO_CSV = "historico_vagas.csv"
EMAIL_RECEPTOR = os.getenv("EMAIL_RECEPTOR")
EMAIL_REMETENTE = os.getenv("EMAIL_REMETENTE")
SENHA_REMETENTE = os.getenv("SENHA_REMETENTE")

palavras_chave = ["Analista de BI", "Business Intelligence", "Data", "Dados", "Analytics", "Product", "Produto"]
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

# Buscar novas vagas
todas_vagas = []
for empresa in empresas:
    for palavra in palavras_chave:
        print(f"🔍 Buscando por: '{palavra}' em '{empresa}'")
        vagas = buscar_vagas(empresa=empresa, palavra_chave=palavra)
        todas_vagas.extend(vagas)
        time.sleep(1)

df_novas = pd.DataFrame(todas_vagas)
df_novas["data_abertura"] = DATA_HOJE
df_novas["data_fechamento"] = pd.NA
df_novas["status"] = "ativa"

# Carregar histórico
if os.path.exists(ARQUIVO_CSV):
    historico = pd.read_csv(ARQUIVO_CSV)
    historico["data_abertura"] = pd.to_datetime(historico["data_abertura"]).dt.strftime("%Y-%m-%d")
    historico["data_fechamento"] = historico["data_fechamento"].fillna(pd.NA)
    historico["status"] = historico["status"].fillna("ativa")
else:
    historico = pd.DataFrame(columns=df_novas.columns)

# Links para comparação
links_historico_ativos = historico[historico["status"].isin(["ativa", "reaberta"])]["link"].tolist()
links_atuais = df_novas["link"].tolist()

# Detectar novas vagas
novas_vagas = df_novas[~df_novas["link"].isin(links_historico_ativos)]

# Adicionar novas e reabertas
for _, row in novas_vagas.iterrows():
    link = row["link"]
    if link in historico["link"].tolist():
        row["status"] = "reaberta"
    historico = pd.concat([historico, pd.DataFrame([row])], ignore_index=True)

# Atualizar status e data de fechamento para vagas que sumiram
def atualizar_fechamento(row):
    if pd.notna(row["status"]) and row["status"] in ["ativa", "reaberta"] and row["link"] not in links_atuais:
        return DATA_HOJE
    return row["data_fechamento"]

def atualizar_status(row):
    if pd.notna(row["status"]) and row["status"] in ["ativa", "reaberta"] and row["link"] not in links_atuais:
        return "fechada"
    return row["status"]

historico["data_fechamento"] = historico.apply(atualizar_fechamento, axis=1)
historico["status"] = historico.apply(atualizar_status, axis=1)

# Salvar histórico
historico.to_csv(ARQUIVO_CSV, index=False)
print(f"💾 Histórico salvo com {len(historico)} vagas em '{ARQUIVO_CSV}'.")

# Atualizar Google Sheets (opcional)
try:
    atualizar_google_sheets(historico)
except Exception as e:
    print(f"❌ Erro ao atualizar Google Sheets: {e}")

# Enviar email com novas vagas
if not novas_vagas.empty and EMAIL_RECEPTOR and EMAIL_REMETENTE and SENHA_REMETENTE:
    enviar_email(novas_vagas, EMAIL_RECEPTOR, EMAIL_REMETENTE, SENHA_REMETENTE)
else:
    print("📭 Nenhuma vaga nova para enviar ou variáveis de e-mail não configuradas.")
