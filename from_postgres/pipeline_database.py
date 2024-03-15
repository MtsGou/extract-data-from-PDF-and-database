import psycopg2
import pandas as pd
import sys

query_tabela_proposta = '''
SELECT pro_nomecompleto, 
       pro_cpf, 
       pro_datanascimento
FROM schema.\"TABLE1\"
'''

query_tabela_conjuge = '''
SELECT pro_id,
       prc_nomeconjuge,
       prc_cpfconjuge
FROM schema.\"TABLE2\"
'''

query_tabela_banco = '''
SELECT ban_id,
       ban_nome
FROM schema.\"TABLE3\"
'''

column_names_proposta = [
    'pro_nomecompleto',
    'pro_cpf',
    'pro_datanascimento'
]

column_names_conjuge = [
    'pro_id',
    'prc_nomeconjuge',
    'prc_cpfconjuge'
]

column_names_banco = [
    'ban_id',
    'ban_nome'
]

dict_final_columns_names = {
    "pro_nomecompleto":'Nome_completo_cliente',
    "pro_cpf":'CPF_cliente',
    "pro_datanascimento":'Data_nascimento_cliente'
}

def connect_to_db(user_log: str, pswd: str):
    db_connection = None
    try:
        print('Connecting to database...')
        db_connection = psycopg2.connect(
            database="YOUR_DATABASE",
            user= user_log,
            password= pswd,
            host="link_to_host",
            port='5433'
        )
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error during connection. Couldn't connect.\n\r")
        print(error)
        sys.exit(1)

    print("Connection established successfully\n\r")
    return db_connection

def sql_to_dataframe(conn, query, column_names):
   cursor = conn.cursor()
   try:
      cursor.execute(query)
   except (Exception, psycopg2.DatabaseError) as error:
      print("Error: %s" % error)
      cursor.close()
      return 1
   # The execute returns a list of tuples:
   tuples_list = cursor.fetchall()
   cursor.close()
   # Now we need to transform the list into a pandas DataFrame:
   df = pd.DataFrame(tuples_list, columns=column_names)
   return df

def get_credentials():
    user_login = input('User: ')
    password = input('Password: ')
    return user_login, password

def define_how_many_data():
    global query_tabela_proposta
    global query_tabela_conjuge
    global query_tabela_banco
    num = None
    response = input("Get all data from database? N or Y  ")
    while (response != 'N' and response != 'Y' and response != 'y' and response != 'n'):
        response = input("Invalid. Digit N ou Y  ")
    if (response == 'Y'):
        return
    while (type(num) is not int or num<0):
        try:
            if num == None:
                num = int(input("How many data? Write a number: "))
            else:
                num = int(input("Invalid. Write a number: "))
        except:
            num = -1
            continue
    print(f'{num} most recent data on database')
    query_tabela_proposta += ' ORDER BY pro_id DESC LIMIT {}'.format(num)
    query_tabela_conjuge += ' ORDER BY pro_id DESC LIMIT {}'.format(num)
    query_tabela_banco += ' ORDER BY ban_id DESC LIMIT {}'.format(num)
    return num

def join_tables(df_1, df_2, df_3):
    df = df_1.copy()
    df = df.merge(df_2, how = 'left', on = ['pro_id'])
    df = df.merge(df_3, how = 'left', on = ['ban_id'])
    return df

def run():
    #user_login, password = get_credentials()
    db_connection = connect_to_db('admin', 'your_password')
    #define_how_many_data()
    df_pro = sql_to_dataframe(db_connection, query_tabela_proposta, column_names_proposta)
    df_prc = sql_to_dataframe(db_connection, query_tabela_conjuge, column_names_conjuge)
    df_ban = sql_to_dataframe(db_connection, query_tabela_banco, column_names_banco)
    df_excel = join_tables(df_pro, df_prc, df_ban)
    df_excel.drop(columns = {'pro_id', 'ban_id'}, inplace=True)
    df_excel.rename(columns=dict_final_columns_names, inplace=True)
    #df_excel.set_index('Nome_completo_cliente', inplace=True)
    try:
        df_excel.to_excel('final_data.xlsx')
    except:
        print("Mistake during creation of the file.")
        sys.exit(1)
    print("Data exported to xlsx successfully.")

if __name__ == "__main__":
    run()