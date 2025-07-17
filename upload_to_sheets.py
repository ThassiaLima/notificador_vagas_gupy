import json
import gspread
import os
from datetime import datetime

# Caminho para o arquivo JSON de histórico de vagas
PREVIOUS_JOBS_FILE = "historico_vagas.csv"

# Nome da planilha Google Sheets e aba onde os dados serão armazenados
GOOGLE_SHEETS_NAME = os.getenv("GOOGLE_SHEETS_NAME", "ifood_bi_jobs_history")
SHEET_WORKSHEET_NAME = os.getenv("SHEET_WORKSHEET_NAME", "Vagas") # Nome da aba na planilha

# Variável de ambiente para armazenar as credenciais da conta de serviço (JSON string)
GOOGLE_SERVICE_ACCOUNT_CREDENTIALS = os.getenv("GOOGLE_SERVICE_ACCOUNT_CREDENTIALS")

def upload_data_to_google_sheets():
    """
    Carrega o histórico de vagas do JSON e faz o upload/atualiza no Google Sheets.
    """
    print(f"\n--- Iniciando upload de dados para o Google Sheets ({GOOGLE_SHEETS_NAME}) ---")

    try:
        # Autenticação com a conta de serviço
        # As credenciais virão de uma variável de ambiente no GitHub Actions
        if not GOOGLE_SERVICE_ACCOUNT_CREDENTIALS:
            raise ValueError("Variável de ambiente GOOGLE_SERVICE_ACCOUNT_CREDENTIALS não configurada.")

        # gspread espera um arquivo JSON ou um dicionário de credenciais
        # Carrega as credenciais da string JSON
        creds_dict = json.loads(GOOGLE_SERVICE_ACCOUNT_CREDENTIALS)
        gc = gspread.service_account_from_dict(creds_dict)

        # Abre a planilha pelo nome
        spreadsheet = gc.open(GOOGLE_SHEETS_NAME)
        worksheet = spreadsheet.worksheet(SHEET_WORKSHEET_NAME)
        print(f"Planilha '{GOOGLE_SHEETS_NAME}' e aba '{SHEET_WORKSHEET_NAME}' abertas com sucesso.")

        # Carregar dados do arquivo JSON
        if not os.path.exists(PREVIOUS_JOBS_FILE):
            print(f"Erro: Arquivo '{PREVIOUS_JOBS_FILE}' não encontrado. Não há dados para upload.")
            return

        with open(PREVIOUS_JOBS_FILE, 'r', encoding='utf-8') as f:
            all_jobs_history = json.load(f)

        if not all_jobs_history:
            print("O arquivo JSON de histórico de vagas está vazio. Nenhuma atualização no Google Sheets.")
            # Limpa a planilha se o JSON estiver vazio, exceto o cabeçalho
            # worksheet.clear() # CUIDADO: Isso limpa TUDO. Considere limpar apenas os dados.
            # worksheet.update([['title', 'link', 'date_entrada', 'date_saida', 'status']], range_name='A1')
            return

        # Prepare os dados para o Google Sheets
        # Garante que a ordem das colunas seja consistente
        header = ['title', 'link', 'location', 'date_entrada', 'date_saida', 'status']
        data_to_upload = [header] # Primeira linha é o cabeçalho

        for job in all_jobs_history:
            row = [
                job.get('title', ''),
                job.get('link', ''),
                job.get('location', ''),
                job.get('date_entrada', ''),
                job.get('date_saida', ''),
                job.get('status', '')
            ]
            data_to_upload.append(row)

        # Atualizar a planilha
        # clear() limpa tudo, depois update escreve. Alternativa é update('A1', data, value_input_option='RAW')
        # worksheet.clear() # CUIDADO: Se você tiver outros dados ou gráficos na planilha, isso pode ser destrutivo.
                         # É mais seguro limpar apenas a área de dados.
        # Para limpar apenas a área de dados e garantir que o cabeçalho seja reescrito:
        # Pega a última linha da aba para saber o range total
        num_rows_existing = worksheet.row_count
        if num_rows_existing > 1: # Se tiver mais de uma linha (cabeçalho + dados)
            # Limpa da segunda linha até o final da aba
            worksheet.delete_rows(2, num_rows_existing)

        # Adiciona as linhas de dados, incluindo o cabeçalho
        # worksheet.update(data_to_upload) # Isso escreverá tudo a partir de A1.
        # Melhor usar append_rows se você não limpou tudo ou se o cabeçalho já existe.
        # Ou, se o cabeçalho já foi definido e você quer apenas substituir os dados:
        worksheet.update('A1', data_to_upload) # Reescreve da célula A1 em diante, incluindo o cabeçalho.
                                                # Isso é mais seguro se você tiver certeza que o cabeçalho está sempre lá.


        print(f"Dados do histórico de vagas atualizados com sucesso na planilha '{GOOGLE_SHEETS_NAME}'.")

    except gspread.exceptions.SpreadsheetNotFound:
        print(f"Erro: Planilha '{GOOGLE_SHEETS_NAME}' não encontrada no Google Drive da conta de serviço.")
    except gspread.exceptions.WorksheetNotFound:
        print(f"Erro: Aba '{SHEET_WORKSHEET_NAME}' não encontrada na planilha '{GOOGLE_SHEETS_NAME}'.")
    except Exception as e:
        print(f"Ocorreu um erro no upload para o Google Sheets: {e}")
        print("Certifique-se de que a conta de serviço tem permissão de 'Editor' na planilha.")

if __name__ == "__main__":
    # Teste local (requer que GOOGLE_SERVICE_ACCOUNT_CREDENTIALS esteja no ambiente local ou arquivo)
    # Para testar localmente, salve o conteúdo do seu JSON de credenciais em uma variável de ambiente temporária
    # ou crie um arquivo 'service_account.json' na raiz do projeto e use:
    # gc = gspread.service_account(filename='service_account.json')
    # Para o GitHub Actions, a variável de ambiente é a melhor abordagem.
    upload_data_to_google_sheets()
