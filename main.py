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

# ==================== FUNÇÕES ====================

def buscar_vagas(driver):
    """Busca vagas nas empresas e retorna uma lista de dicionários."""
    todas_vagas = []
    wait = WebDriverWait(driver, 20)

    for empresa, url in EMPRESAS.items():
        for termo in PALAVRAS_CHAVE:
            print(f"🔍 Buscando por: '{termo}' em '{empresa}'")
            try:
                driver.get(url)
                campo = wait.until(EC.presence_of_element_located((By.ID, "job-search")))
                campo.clear()
                campo.send_keys(termo)
                campo.send_keys(Keys.ENTER)
                time.sleep(random.uniform(2, 4))

                vagas_elements = driver.find_elements(By.CSS_SELECTOR, "li[data-testid='job-list__listitem']")
                
                if not vagas_elements:
                    continue

                for vaga_el in vagas_elements:
                    try:
                        link_el = vaga_el.find_element(By.CSS_SELECTOR, "a[data-testid='job-list__listitem-href']")
                        titulo = link_el.find_element(By.CSS_SELECTOR, "div > div:first-child").text.strip()
                        link = link_el.get_attribute("href")
                        
                        todas_vagas.append({"empresa": empresa, "titulo": titulo, "link": link})
                    except Exception as e_item:
                        print(f"⚠️ Erro ao extrair detalhe de uma vaga: {e_item}")
                        continue
            except Exception as e_geral:
                print(f"❌ Erro geral ao buscar em '{empresa}': {e_geral}")
                continue
    
    print(f"✅ Extração concluída. Total de vagas encontradas: {len(todas_vagas)}")
    return todas_vagas

def enviar_email(vagas_para_notificar):
    """Envia um e-mail com a lista de novas vagas."""
    if not vagas_para_notificar:
        print("📭 Nenhuma vaga nova para enviar por e-mail.")
        return

    msg = MIMEMultipart()
    msg["From"] = f"Notificador de Vagas <{EMAIL_REMETENTE}>"
    msg["To"] = EMAIL_DESTINO
    msg["Subject"] = f"📢 {len(vagas_para_notificar)} Novas Vagas Encontradas!"

    corpo_html = "<h2>🚀 Foram encontradas as seguintes oportunidades:</h2>"
    for vaga in vagas_para_notificar:
        corpo_html += f"<p><b>{vaga['titulo']}</b><br><b>Empresa:</b> {vaga['empresa']}<br><a href='{vaga['link']}'>Ver vaga</a></p><hr>"

    msg.attach(MIMEText(corpo_html, "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(EMAIL_REMETENTE, SENHA_APP)
            server.send_message(msg)
        print("✅ E-mail enviado com sucesso!")
    except Exception as e:
        print(f"❌ Erro ao enviar e-mail: {e}")

def atualizar_google_sheets(df):
    """Atualiza a planilha do Google Sheets com o DataFrame fornecido."""
    print("🔄 Tentando atualizar o Google Sheets...")
    try:
        with open("credenciais.json", "r") as f:
            creds_dict = json.load(f)

        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)

        spreadsheet = client.open("Historico Vagas Gupy")
        sheet = spreadsheet.sheet1
        
        df_to_upload = df.fillna("")

        sheet.clear()
        set_with_dataframe(sheet, df_to_upload)
        print("✅ Planilha Google Sheets atualizada com sucesso!")
    except Exception as e:
        print(f"❌ Erro ao atualizar Google Sheets: {e}")

# ==================== EXECUÇÃO PRINCIPAL ====================
if __name__ == "__main__":
    # 1. Carrega o histórico
    colunas_historico = ["empresa", "titulo", "link", "data_abertura", "status", "data_fechamento"]
    if os.path.exists(ARQUIVO_CSV):
        # Garantir que a coluna de link seja lida como texto (string) para evitar problemas de tipo
        historico_df = pd.read_csv(ARQUIVO_CSV, dtype={"link": str})
    else:
        historico_df = pd.DataFrame(columns=colunas_historico)

    # 2. Busca as vagas atuais
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=chrome_options)
    
    vagas_atuais_lista = buscar_vagas(driver)
    driver.quit()

    if not vagas_atuais_lista:
        print("Nenhuma vaga encontrada na busca atual. Encerrando.")
    else:
        vagas_atuais_df = pd.DataFrame(vagas_atuais_lista).drop_duplicates(subset="link")

        # 3. Lógica de comparação e atualização com LOGS DE DEBUG
        print("\n--- INICIANDO COMPARAÇÃO DE VAGAS ---")
        print(f"Histórico carregado com {len(historico_df)} vagas.")
        print(f"Busca atual encontrou {len(vagas_atuais_df)} vagas únicas.")

        links_historico = set(historico_df["link"])
        links_atuais = set(vagas_atuais_df["link"])

        print(f"Total de links únicos no histórico: {len(links_historico)}")
        print(f"Total de links únicos na busca atual: {len(links_atuais)}")
        
        links_novos = links_atuais - links_historico
        print(f"Número de links novos encontrados (diferença): {len(links_novos)}")

        vagas_para_notificar_df = vagas_atuais_df[vagas_atuais_df["link"].isin(links_novos)].copy()
        
        # =================================================================
        # LOG SOLICITADO: MOSTRA AS VAGAS QUE SERÃO ENVIADAS POR E-MAIL
        # =================================================================
        print("\n--- VAGAS IDENTIFICADAS COMO NOVAS PARA NOTIFICAÇÃO ---")
        if not vagas_para_notificar_df.empty:
            print(vagas_para_notificar_df.to_string())
        else:
            print("NENHUMA VAGA NOVA PARA NOTIFICAR.")
        print("--- FIM DO LOG DE VAGAS NOVAS ---\n")
        # =================================================================

        if not vagas_para_notificar_df.empty:
            vagas_para_notificar_df["data_abertura"] = DATA_HOJE
            vagas_para_notificar_df["status"] = "ativa"
            vagas_para_notificar_df["data_fechamento"] = pd.NA
            historico_df = pd.concat([historico_df, vagas_para_notificar_df], ignore_index=True)

        links_fechados = links_historico - links_atuais
        if links_fechados:
            print(f"🚪 {len(links_fechados)} vagas foram fechadas.")
            historico_df.loc[
                (historico_df["link"].isin(links_fechados)) & (historico_df["status"] == "ativa"), 
                ["status", "data_fechamento"]
            ] = ["fechada", DATA_HOJE]

        # 4. Salvar e enviar
        historico_df.to_csv(ARQUIVO_CSV, index=False)
        print(f"💾 Histórico final salvo com {len(historico_df)} vagas em '{ARQUIVO_CSV}'.")

        # 5. Atualizar Google Sheets
        atualizar_google_sheets(historico_df)

        # 6. Enviar e-mail APENAS com as vagas novas
        if EMAIL_REMETENTE and SENHA_APP and EMAIL_DESTINO:
            enviar_email(vagas_para_notificar_df.to_dict("records"))
        else:
            print("⚠️ Variáveis de e-mail não configuradas.")