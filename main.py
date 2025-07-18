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
# ==================== FUNÇÕES (VERSÃO CORRIGIDA) ====================

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
                
                # Uma pequena pausa para garantir que os resultados da busca carreguem
                time.sleep(random.uniform(2, 4)) 

                vagas = driver.find_elements(By.CSS_SELECTOR, "li[data-testid='job-list__listitem']")
                
                if not vagas:
                    print(f"-> Nenhuma vaga encontrada para '{termo}' em '{empresa}'.")
                    continue

                print(f"🔎 {len(vagas)} vaga(s) encontrada(s). A extrair dados...")
                for v in vagas:
                    try:
                        # ======================================================================
                        # ALTERAÇÃO FINAL E CORRETA - Baseada no HTML que você forneceu
                        # Encontramos o link e, a partir dele, navegamos para o primeiro <div> filho
                        # que contém o título.
                        # ======================================================================
                        link_element = v.find_element(By.CSS_SELECTOR, "a[data-testid='job-list__listitem-href']")
                        titulo = link_element.find_element(By.CSS_SELECTOR, "div > div:first-child").text.strip()
                        link = link_element.get_attribute("href")
                        
                        todas_vagas.append({
                            "empresa": empresa,
                            "titulo": titulo,
                            "link": link,
                        })
                    except Exception as e_item:
                        # Este print ajuda a diagnosticar se um seletor falhar no futuro
                        print(f"⚠️ Erro ao extrair detalhe de uma vaga específica: {e_item}")
                        continue
            
            except Exception as e_geral:
                print(f"❌ Erro geral ao buscar por '{termo}' em '{empresa}': {e_geral}")
                continue
    
    print(f"✅ Extração concluída. Total de vagas salvas na memória: {len(todas_vagas)}")
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
# Carrega histórico anterior, se existir
colunas_esperadas = ["empresa", "titulo", "link", "data_abertura", "data_fechamento"]

if os.path.exists(ARQUIVO_CSV):
    try:
        historico = pd.read_csv(ARQUIVO_CSV)
        # Garante que todas as colunas existam
        for col in colunas_esperadas:
            if col not in historico.columns:
                historico[col] = pd.NA
        historico = historico[colunas_esperadas]
    except Exception:
        historico = pd.DataFrame(columns=colunas_esperadas)
else:
    historico = pd.DataFrame(columns=colunas_esperadas)


# Busca atual
vagas_atuais = buscar_vagas()
driver.quit()

df_atuais = pd.DataFrame(vagas_atuais).drop_duplicates(subset="link")

# Prepara novas vagas
novas_vagas = df_atuais[~df_atuais["link"].isin(historico["link"])].copy()
novas_vagas["data_abertura"] = DATA_HOJE
novas_vagas["data_fechamento"] = pd.NA

# Marca vagas fechadas no histórico
links_atuais = set(df_atuais["link"])
historico["data_fechamento"] = historico.apply(
    lambda row: DATA_HOJE if pd.isna(row["data_fechamento"]) and row["link"] not in links_atuais else row["data_fechamento"],
    axis=1
)

# Junta histórico com novas vagas
historico_atualizado = pd.concat([historico, novas_vagas], ignore_index=True)

# Remove duplicatas mantendo o último registro
historico_atualizado = historico_atualizado.sort_values("data_abertura").drop_duplicates(subset="link", keep="last")

# Salva CSV
historico_atualizado.to_csv(ARQUIVO_CSV, index=False)
print(f"💾 Histórico salvo com {len(historico_atualizado)} vagas em '{ARQUIVO_CSV}'.")



import time

def atualizar_google_sheets(df, tentativas=3):
    for tentativa in range(tentativas):
        try:
            from gspread_dataframe import set_with_dataframe
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
            return  # sai da função se deu certo

        except Exception as e:
            print(f"❌ Tentativa {tentativa + 1} falhou: {e}")
            if "Quota exceeded" in str(e) and tentativa < tentativas - 1:
                tempo_espera = 60 * (tentativa + 1)
                print(f"⏳ Aguardando {tempo_espera} segundos antes de tentar novamente...")
                time.sleep(tempo_espera)
            else:
                break

    try:
        import numpy as np
        from gspread_dataframe import set_with_dataframe

        # Carrega credenciais do JSON
        with open("credenciais.json", "r") as f:
            creds_dict = json.load(f)

        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)

        # Nome da planilha
        spreadsheet = client.open("Historico Vagas Gupy")
        sheet = spreadsheet.sheet1

        # Limpa dados inválidos
        df = df.replace([np.inf, -np.inf], np.nan).fillna("")

        # Escreve todo o DataFrame de uma vez
        sheet.clear()
        set_with_dataframe(sheet, df)

        print("✅ Planilha Google Sheets atualizada com sucesso!")

    except Exception as e:
        print(f"❌ Erro ao atualizar Google Sheets: {e}")

    try:
        import numpy as np  # Garante que esteja importado dentro da função, se quiser local

        # Carrega credenciais do JSON
        with open("credenciais.json", "r") as f:
            creds_dict = json.load(f)

        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)

        # Nome da planilha
        spreadsheet = client.open("Historico Vagas Gupy")
        sheet = spreadsheet.sheet1

        # Limpeza dos dados para evitar erro de JSON
        df = df.replace([np.inf, -np.inf], np.nan).fillna("")

        # Limpa e reescreve tudo
        sheet.clear()
        sheet.append_row(df.columns.tolist())
        for row in df.values.tolist():
            sheet.append_row(row)
        print("✅ Planilha Google Sheets atualizada com sucesso!")

    except Exception as e:
        print(f"❌ Erro ao atualizar Google Sheets: {e}")

    try:
        # Carrega credenciais do JSON
        with open("credenciais.json", "r") as f:
            creds_dict = json.load(f)

        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)

        # Nome da planilha
        spreadsheet = client.open("Historico Vagas Gupy")
        sheet = spreadsheet.sheet1

        # Limpa e reescreve tudo
        sheet.clear()
        sheet.append_row(df.columns.tolist())
        for row in df.values.tolist():
            sheet.append_row(row)
        print("✅ Planilha Google Sheets atualizada com sucesso!")

    except Exception as e:
        print(f"❌ Erro ao atualizar Google Sheets: {e}")

# Chamada da função
atualizar_google_sheets(historico_atualizado)


# Envia e-mail com novas vagas
if EMAIL_REMETENTE and SENHA_APP and EMAIL_DESTINO:
    enviar_email(novas_vagas.to_dict("records"))
else:
    print("⚠️ Variáveis de e-mail não configuradas.")