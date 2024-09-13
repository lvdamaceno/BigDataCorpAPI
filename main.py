import http.client
import json
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv


class BigDataCorpAPI:
    def __init__(self, base_url, token, token_id):
        self.base_url = base_url
        self.token = token
        self.token_id = token_id
        self.connection = http.client.HTTPSConnection(self.base_url, timeout=5)

    def make_request(self, doc, dataset):
        """
        Faz uma requisição POST para obter dados com base no CPF.

        :param dataset:
        :param doc: CPF ou outro identificador
        :return: Resposta da requisição em formato JSON
        """
        headers = {
            'AccessToken': self.token,
            'TokenId': self.token_id,
            'Content-Type': 'application/json'
        }

        payload = json.dumps({
            "q": f"doc{{{doc}}}",
            "Datasets": dataset
        })

        try:
            self.connection.request("POST", "/pessoas", body=payload, headers=headers)
            response = self.connection.getresponse()
            data = response.read()
            return data.decode("utf-8")
        except Exception as e:
            print(f"Erro na requisição: {e}")
            return ""


def get_rg(doc, api):
    """
    Obtém o RG principal com base no CPF.

    :param doc: CPF (documento a ser consultado)
    :param api: Instância da API BigDataCorp
    :return: RG principal ou string vazia se não encontrado
    """
    data = json.loads(api.make_request(doc, "basic_data"))

    alternative_id = data.get("Result", [{}])[0].get("BasicData", {}).get("AlternativeIdNumbers", {})

    if alternative_id:
        chave, valor = next(iter(alternative_id.items()))
        return valor
    return ""


def get_email(doc, api):
    """
    Obtém o e-mail principal associado ao CPF.

    :param doc: CPF (documento a ser consultado)
    :param api: Instância da API BigDataCorp
    :return: E-mail principal ou string vazia se não encontrado
    """
    try:
        data = json.loads(api.make_request(doc, "registration_data"))
        email = data.get("Result", [{}])[0].get("RegistrationData", {}).get("Emails", {}).get("Primary", {}).get(
            "EmailAddress", "")
    except (json.JSONDecodeError, ConnectionError) as e:
        email = ""

    return email


def get_telefone(order, doc, api):
    """
    Obtém o telefone com base no CPF e na ordem especificada.

    :param order: 'Primary' ou 'Secondary'
    :param doc: CPF
    :param api: Instância da API BigDataCorp
    :return: Número de telefone com código de área ou string vazia se não encontrado
    """
    try:
        data = json.loads(api.make_request(doc, "registration_data"))
        phones = data.get("Result", [{}])[0].get("RegistrationData", {}).get("Phones", {})
        areacode = phones.get(order, {}).get("AreaCode", "")
        number = phones.get(order, {}).get("Number", "")
    except (IndexError, KeyError, TypeError, json.JSONDecodeError):
        areacode = ""
        number = ""
    return areacode + number


def get_sexo(doc, api):
    """
    Obtém o gênero (sexo) associado ao CPF.

    :param doc: CPF (documento a ser consultado)
    :param api: Instância da API BigDataCorp
    :return: Gênero (sexo) ou string vazia se não encontrado
    """
    try:
        data = json.loads(api.make_request(doc, "basic_data"))
        sexo = data.get("Result", [{}])[0].get("BasicData", {}).get("Gender", "")
    except (json.JSONDecodeError, ConnectionError) as e:
        sexo = ""

    return sexo


def get_idade(doc, api):
    """
    Obtém a idade associada ao CPF.

    :param doc: CPF (documento a ser consultado)
    :param api: Instância da API BigDataCorp
    :return: Idade como string ou string vazia se não encontrado
    """
    try:
        data = json.loads(api.make_request(doc, "basic_data"))
        idade = data.get("Result", [{}])[0].get("BasicData", {}).get("Age")
        str_idade = str(idade) if idade is not None else ""
    except (json.JSONDecodeError, ConnectionError):
        str_idade = ""

    return str_idade


def get_dt_nasc(doc, api):
    """
    Obtém a data de nascimento no formato brasileiro com base no CPF.

    :param doc: CPF (documento a ser consultado)
    :param api: Instância da API BigDataCorp
    :return: Data de nascimento formatada como dd/mm/yyyy ou string vazia se não encontrada
    """
    try:
        data = json.loads(api.make_request(doc, "basic_data"))
        dt_nasc = data.get("Result", [{}])[0].get("BasicData", {}).get("BirthDate")

        if dt_nasc:
            data_datetime = datetime.strptime(dt_nasc, "%Y-%m-%dT%H:%M:%SZ")
            data_br = data_datetime.strftime("%d/%m/%Y")
        else:
            data_br = ""
    except (json.JSONDecodeError, ConnectionError, ValueError):
        data_br = ""

    return data_br


FUNC_MAP = {
    'RG': get_rg,
    'EMAIL': get_email,
    'SEXO': get_sexo,
    'DT_NASC': get_dt_nasc,
    'IDADE': get_idade
}


def preencher_registro_vazio(row, registro, funcao, api):
    """
    Preenche o registro vazio usando a função adequada.

    :param funcao: Nome da função a ser usada
    :param registro: Nome do registro a ser preenchido
    :param row: Linha do DataFrame
    :param api: Instância da API BigDataCorp
    :return: Registro preenchido ou o valor original
    """
    if pd.isna(row[registro]):
        func = FUNC_MAP.get(funcao)
        if func:
            print(row['CPF'] + f' - {registro} ' + ': ' + func(row['CPF'], api))
            return func(row['CPF'], api)
        else:
            return row[registro]
    return row[registro]


def preencher_telefone_vazio(row, order, api):
    """
    Preenche o campo de telefone vazio em uma linha do DataFrame usando a função get_telefone.

    :param row: Linha do DataFrame
    :param order: Indica se o telefone é 'Primary' (P) ou 'Secondary' (S)
    :param api: Instância da API BigDataCorp
    :return: Telefone preenchido ou o valor original
    """
    order_map = {
        'P': ('Primary', 1),
        'S': ('Secondary', 2)
    }

    ordem, number = order_map.get(order, ('', 1))
    telefone_col = f'TELEFONE{number}'

    if pd.isna(row[telefone_col]):
        telefone = get_telefone(ordem, row['CPF'], api)
        print(f"{row['CPF']} - {telefone_col}: {telefone}")
        return telefone

    return row[telefone_col]


def main():
    # Configurar opções de exibição do pandas
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
    load_dotenv()
    # Criar uma instância da API
    api = BigDataCorpAPI(
        base_url="plataforma.bigdatacorp.com.br",
        token=os.getenv('ACCESS_TOKEN'),
        token_id=os.getenv('TOKEN_ID')
    )

    # Ler o CSV
    df = pd.read_csv('dataset_test.csv', delimiter=',', decimal=',',
                     dtype={'CPF': str, 'TELEFONE': str, 'TELEFONE2': str})

    registros = {'RG', 'EMAIL', 'SEXO', 'IDADE', 'DT_NASC', 'TELEFONE1', 'TELEFONE2'}

    for registro in registros:
        if registro.startswith('TELEFONE'):
            ordem = 'P' if registro == 'TELEFONE1' else 'S'
            df[registro] = df.apply(preencher_telefone_vazio, axis=1, order=ordem, api=api)
        else:
            df[registro] = df.apply(preencher_registro_vazio, axis=1, registro=registro, funcao=registro, api=api)

    # Salvar o DataFrame atualizado
    df.to_csv('dataset_test_atualizado.csv', index=False, sep=',', decimal=',')


if __name__ == '__main__':
    main()
