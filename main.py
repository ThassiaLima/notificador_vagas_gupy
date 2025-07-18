import os
import time
import pandas as pd
from datetime import date
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
import random
import gspread
from gspread_dataframe import set_with_dataframe
import json
from oauth2client.service_account import ServiceAccountCredentials

# ==================== CONFIGURAÇÕES ====================
PALAVRAS_CHAVE = ["Analista de BI", "Business Intelligence", "Data", "Dados", "Analytics", "Product", "Produto"]
EMPRESAS = {
    "Itaú": "https://vemproitau.gupy.io/",
    "Boticário": "https://grupoboticario.gupy.io/",
    "Raízen": "https://genteraizen.gupy.io/",
    "C&A": "https://cea.gupy.io/",
    "AmbevTech": "https://ambevtech.gupy.io",
    "OLX": "https://vemsergrupoolx.gupy.io",
    "Localiza": "https://localiza.gupy.io",
    "BMG": "https://bmg.gupy.io"
}
ARQUIVO_CSV = "historico_vagas.csv"
DATA_HOJE = str(date.today())

EMAIL_REMETENTE = os.getenv("EMAIL_REMETENTE")
SENHA_APP = os.getenv("SENHA_APP")
EMAIL_DESTINO = os.getenv("EMAIL_DESTINO")

# ==================== WEBDRIVER CONFIG ====================
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
driver = webdriver.Chrome(options=chrome_options)
wait = WebDriverWait(driver, 25)

# ==================== FUNÇÕES ====================
def buscar_vagas():
    todas_vagas = []

    for empresa, url in EMPRESAS.items():
        for termo in PALAVRAS_CHAVE:
            print(f"🔍 Buscando por: '{termo}' em '{empresa}'")
            try:
                driver.get(url)
                wait.until(EC.presence_of_element_located((By.ID, "job-search")))
                campo = driver.find_element(By.ID, "job-search")
                campo.clear()
                campo.send_keys(termo)
                campo.send_keys(Keys.ENTER)
                time.sleep(random.uniform(2, 4))

                vagas = driver.find_elements(By.CSS_SELECTOR, "li[data-testid='job-list__listitem']")
                if not vagas:
                    print(f"-> Nenhuma vaga encontrada para '{termo}' em '{empresa}'.")
                    continue

                for v in vagas:
                    try:
                        link_element = v.find_element(By.CSS_SELECTOR, "a[data-testid='job-list__listitem-href']")
                        titulo = link_element.find_element(By.CSS_SELECTOR, "div > div:first-child").text.strip()
                        link = link_element.get_attribute("href")

                        todas_vagas.append({
                            "empresa": empresa,
                            "titulo": titulo,
                            "link": link,
                        })
                    except Exception as e_item:
                        print(f"⚠️ Erro ao extrair vaga: {e_item}")
                        continue

            except Exception as e_geral:
                print(f"❌ Erro ao buscar por '{termo}' em '{empresa}': {e_geral}")
                continue

    print(f"✅ Extração concluída: {len(todas_vagas)} vagas encontradas.")
    return todas_vagas

def enviar_email(vagas):
    if not vagas:
        print("📭 Nenhuma vaga nova para enviar.")
        return

    msg = MIMEMultipart()
    msg["From"] = f"Vagas Gupy <{EMAIL_REMETENTE}>"
    msg["To"] = EMAIL_DESTINO
    msg["Subject"] = f"📢 {len(vagas)} Novas Vagas Encontradas"

    corpo = ""
    for v in vagas:
        corpo += f"<b>{v['titulo']}</b><br>{v['empresa']}<br><a href='{v['link']}'>{v['link']}</a><br><br>"

    msg.attach(MIMEText(corpo, "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(EMAIL_REMETENTE, SENHA_APP)
            server.send_message(msg)
        print("✅ E-mail enviado com sucesso!")
    except Exception as e:
        print(f"❌ Erro ao enviar e-mail: {e}")

# ==================== EXECUÇÃO ====================
colunas_esperadas = ["empresa", "titulo", "link", "data_abertura", "data_fechamento", "status"]

if os.path.exists(ARQUIVO_CSV):
    historico = pd.read_csv(ARQUIVO_CSV)
    for col in colunas_esperadas:
        if col not in historico.columns:
            historico[col] = pd.NA
    historico = historico[colunas_esperadas]
else:
    historico = pd.DataFrame(columns=colunas_esperadas)

vagas_atuais = buscar_vagas()
driver.quit()
df_atuais = pd.DataFrame(vagas_atuais).drop_duplicates(subset="link")

links_atuais = set(df_atuais["link"])
historico_aberto = historico[historico["status"].isin(["ativa", "reaberta"])]

novas_vagas = df_atuais[~df_atuais["link"].isin(historico_aberto["link"])].copy()
novas_vagas["data_abertura"] = DATA_HOJE
novas_vagas["data_fechamento"] = pd.NA
novas_vagas["status"] = novas_vagas["link"].apply(
    lambda l: "reaberta" if l in historico[historico["status"] == "fechada"]["link"].values else "ativa"
)

# Marcar vagas que sumiram como fechadas
historico["data_fechamento"] = historico.apply(
    lambda row: DATA_HOJE if pd.notna(row["status"]) and row["status"] in ["ativa", "reaberta"] and pd.notna(row["link"]) and row["link"] not in links_atuais else row["data_fechamento"],
    axis=1
)

historico["status"] = historico.apply(
    lambda row: "fechada" if pd.notna(row["status"]) and row["status"] in ["ativa", "reaberta"] and pd.notna(row["link"]) and row["link"] not in links_atuais else row["status"],
    axis=1
)


# Atualiza histórico
historico_atualizado = pd.concat([historico, novas_vagas], ignore_index=True)
historico_atualizado = historico_atualizado.sort_values("data_abertura").drop_duplicates(
    subset=["link", "data_abertura"], keep="last"
)

historico_atualizado.to_csv(ARQUIVO_CSV, index=False)
print(f"💾 Histórico salvo com {len(historico_atualizado)} vagas em '{ARQUIVO_CSV}'.")

# ==================== GOOGLE SHEETS ====================
def atualizar_google_sheets(df, tentativas=3):
    for tentativa in range(tentativas):
        try:
            import numpy as np
            with open("credenciais.json", "r") as f:
                creds_dict = json.load(f)

            scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
            creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
            client = gspread.authorize(creds)
            spreadsheet = client.open("Historico Vagas Gupy")
            sheet = spreadsheet.sheet1

            df = df.replace([np.inf, -np.inf], np.nan).fillna("")
            sheet.clear()
            set_with_dataframe(sheet, df)

            print("✅ Planilha Google Sheets atualizada com sucesso!")
            return
        except Exception as e:
            print(f"❌ Tentativa {tentativa + 1} falhou: {e}")
            if "Quota exceeded" in str(e) and tentativa < tentativas - 1:
                tempo_espera = 60 * (tentativa + 1)
                print(f"⏳ Aguardando {tempo_espera} segundos antes de tentar novamente...")
                time.sleep(tempo_espera)
            else:
                break

atualizar_google_sheets(historico_atualizado)

# ==================== E-MAIL ====================
if EMAIL_REMETENTE and SENHA_APP and EMAIL_DESTINO:
    enviar_email(novas_vagas.to_dict("records"))
else:
    print("⚠️ Variáveis de e-mail não configuradas.")
