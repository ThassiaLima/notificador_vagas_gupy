# utils.py

import smtplib
import ssl
from email.message import EmailMessage
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time
import os

# --- 1. FUNÇÃO PARA BUSCAR VAGAS (WEB SCRAPING) ---

def buscar_vagas(empresa: str, palavra_chave: str, url_empresa: str) -> list:
    """
    Realiza a busca de vagas em uma página da Gupy usando Selenium e BeautifulSoup.
    
    Args:
        empresa (str): O nome da empresa (para preencher no resultado).
        palavra_chave (str): O termo a ser buscado.
        url_empresa (str): A URL base da página de carreiras da empresa na Gupy.

    Returns:
        list: Uma lista de dicionários, onde cada dicionário representa uma vaga encontrada.
    """
    vagas_encontradas = []
    # Constrói a URL de busca. Ex: https://cea.gupy.io/jobs?job_name=data
    url_busca = f"{url_empresa.rstrip('/')}/jobs?job_name={palavra_chave}"
    
    # Configurações do Selenium para rodar no GitHub Actions (sem interface gráfica)
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    
    try:
        # Inicializa o driver do Chrome automaticamente
        driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)
        driver.get(url_busca)

        # Espera a lista de vagas carregar (até 10 segundos)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "ul[data-test='job-list']"))
        )
        
        # Pequena pausa para garantir que todo o JS foi renderizado
        time.sleep(2) 

        # Pega o HTML da página e usa o BeautifulSoup para analisar
        soup = BeautifulSoup(driver.page_source, "html.parser")
        
        # Encontra todos os itens de vaga na lista
        lista_vagas = soup.find_all("li", {"data-test": "job-list-item"})

        for vaga in lista_vagas:
            titulo_tag = vaga.find("h2", {"data-test": "job-name"})
            local_tag = vaga.find("span", {"data-test": "job-location"})
            link_tag = vaga.find("a")

            if titulo_tag and link_tag:
                titulo = titulo_tag.text.strip()
                link = link_tag["href"]
                # Garante que o link seja absoluto
                if not link.startswith("http"):
                    link = f"{url_empresa.rstrip('/')}{link}"
                
                # O local pode não existir em vagas remotas
                local = local_tag.text.strip() if local_tag else "Remoto"

                vagas_encontradas.append({
                    "empresa": empresa,
                    "titulo": titulo,
                    "local": local,
                    "link": link,
                })

    except Exception as e:
        print(f"❌ Erro ao buscar vagas em '{empresa}' com a palavra '{palavra_chave}': {e}")
    finally:
        if 'driver' in locals():
            driver.quit() # Fecha o navegador
            
    return vagas_encontradas


# --- 2. FUNÇÃO PARA ATUALIZAR O GOOGLE SHEETS ---

def atualizar_google_sheets(df_historico: pd.DataFrame):
    """
    Atualiza uma planilha do Google Sheets com o DataFrame do histórico de vagas.

    Args:
        df_historico (pd.DataFrame): O DataFrame completo com todas as vagas.
    """
    print("🔄 Atualizando Google Sheets...")
    try:
        # Usa as credenciais configuradas na variável de ambiente
        # O arquivo 'credenciais.json' deve estar na raiz do projeto
        gc = gspread.service_account(filename=os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "credenciais.json"))
        
        # Substitua "NOME_DA_SUA_PLANILHA" pelo nome exato da sua planilha
        planilha = gc.open("Notificador de Vagas Gupy") 
        
        # Seleciona a primeira aba (worksheet) ou uma com nome específico
        aba = planilha.worksheet("historico_vagas")
        
        # Limpa a aba antes de inserir os novos dados
        aba.clear()
        
        # Usa gspread-dataframe para enviar o DataFrame para a planilha
        set_with_dataframe(aba, df_historico)
        
        print("✅ Google Sheets atualizado com sucesso!")

    except gspread.exceptions.SpreadsheetNotFound:
        print("❌ Erro: Planilha não encontrada. Verifique o nome da planilha.")
    except Exception as e:
        print(f"❌ Erro inesperado ao atualizar o Google Sheets: {e}")
        print("ℹ️ Dica: Você compartilhou a planilha com o e-mail do 'client_email' que está no seu arquivo credenciais.json?")


# --- 3. FUNÇÃO PARA ENVIAR E-MAIL ---

def enviar_email(df_novas_vagas: pd.DataFrame, email_receptor: str, email_remetente: str, senha_remetente: str):
    """
    Envia um e-mail com a lista de novas vagas encontradas.

    Args:
        df_novas_vagas (pd.DataFrame): DataFrame contendo apenas as vagas novas.
        email_receptor (str): E-mail de destino.
        email_remetente (str): Seu e-mail (remetente).
        senha_remetente (str): Sua senha de aplicativo gerada para o e-mail.
    """
    print(f"📬 Preparando e-mail para {email_receptor}...")
    
    # Converte o DataFrame para uma tabela HTML bonita
    html_vagas = df_novas_vagas.to_html(index=False, render_links=True, escape=False)
    
    # Corpo do e-mail
    corpo_email = f"""
    <html>
    <head>
        <style>
            body {{ font-family: sans-serif; }}
            table {{ border-collapse: collapse; width: 100%; }}
            th, td {{ border: 1px solid #dddddd; text-align: left; padding: 8px; }}
            th {{ background-color: #f2f2f2; }}
        </style>
    </head>
    <body>
        <h2>🚀 Novas vagas encontradas!</h2>
        <p>Olá! Seu notificador encontrou as seguintes oportunidades:</p>
        {html_vagas}
        <br>
        <p>Boa sorte!</p>
    </body>
    </html>
    """
    
    msg = EmailMessage()
    msg["Subject"] = f"🤖 Novas Vagas Encontradas ({pd.to_datetime('today').strftime('%d/%m/%Y')})"
    msg["From"] = email_remetente
    msg["To"] = email_receptor
    msg.add_header('Content-Type', 'text/html')
    msg.set_payload(corpo_email)

    try:
        # Conexão segura com o servidor SMTP do Gmail
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(email_remetente, senha_remetente)
            server.send_message(msg)
        print("✅ E-mail enviado com sucesso!")
        
    except Exception as e:
        print(f"❌ Falha ao enviar e-mail: {e}")
        print("ℹ️ Dica: Verifique se o e-mail e a senha de aplicativo estão corretos e se o IMAP está ativado na sua conta Google.")