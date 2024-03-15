import pandas as pd
import PyPDF2
import tabula
from tabula.io import read_pdf

string_endereco_csv = 'tabela0'
count = 0

def get_num_pages(file):
    with open(file, 'rb') as f:
        # leitor de pdf
        pdf_reader = PyPDF2.PdfReader(f)

        # numero de paginas
        num_pages = len(pdf_reader.pages)
        return num_pages

num_pages = get_num_pages('pdf_document_example.pdf')

# Um dicionario contendo lista de tabelas para cada pagina do pdf
dicionario_de_tabelas = {}

for page_num in range(num_pages):
    dicionario_de_tabelas[page_num] = tabula.read_pdf("pdf_document_example.pdf", pages = str(page_num+1), lattice = True)

# dicionario de dataframes pandas
dict_pandas = {}

for page_num in range(num_pages):
    if(len(dicionario_de_tabelas[page_num]) == 3):
       dict_pandas[page_num] = pd.DataFrame(data = dicionario_de_tabelas[page_num][2])
    else:
       dict_pandas[page_num] = pd.DataFrame(data = dicionario_de_tabelas[page_num][1])
    
    dict_pandas[page_num] = dict_pandas[page_num].replace({r'\r': ' '}, regex=True)
    dict_pandas[page_num].columns = dict_pandas[page_num].columns.str.replace(r'\r', ' ')

    count+=1
    string_endereco_csv = string_endereco_csv.removesuffix(string_endereco_csv[-1])
    if (count > 10):
        string_endereco_csv = string_endereco_csv.removesuffix(string_endereco_csv[-1])
    string_endereco_csv += str(count)
    dict_pandas[page_num].to_excel(string_endereco_csv + '.xlsx')









