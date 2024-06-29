import os
import pathlib
import csv
from collections.abc import Generator

# Minha função RNG do tipo LGC.
# Parâmetos tirados de:
# https://www.ams.org/journals/mcom/1999-68-225/S0025-5718-99-00996-5/S0025-5718-99-00996-5.pdf 

def my_rng(seed: int) -> Generator[None]:
    mod = 4294967291 # 2**32 - 5
    a = 1815976680
    while True:
        seed = (a * seed ) % mod
        yield seed


# Ideia: tentar inferir se houve perda de pacote a partir dos parâmetros
# do teste de RTT. Para isso, preciso preparar melhor os dados.
#
# Da forma que as etapas 1 e 2 foram feitas, já garanto que todas as linhas
# estão devidamente populadas com dados.
#
# Outliers não serão removidos, pois são úteis como métrica de RTT para
# inferir que algo está errado e que pode levar à perda de pacote.
#
# Linhas que não têm variância nem desvio padrão são eliminadas. É altamente
# improvável um teste de RTT não ter alguma variância.
#
# Estratégia: vou usar o packet loss como meu "y" nos modelos de treinamento.
# Quando y=0.0, significa "sem perda". Quando y!=0.0, significa que houve perda.
# Não vou me preocupar com a taxa de perda em si, apenas se houve ou não perda.
#
# Como a maioria dos dados são sem perda, pra evitar problema de classe majoritária
# ao treinar os modelos, seguirei a seguinte estratégia "inocente":
#
# 1. Ordeno cada CSV pelas perdas de forma decrescente;
# 2. Conto as linhas com perda "cp" e as linhas sem perda "sp"
# 3. Se cp > sp, separo as N linhas "sp" e escolho N linhas "cp" aleatoriamente;
# 4. Se sp > cp, faço a recíproca;
# 
# A escolha aleatória e feita da seguinte forma:
#
# 1. Ordeno as linhas por variância;
# 2. Defino ranges por "tamanho da lista" divido por N;
# 3. Seleciono aleatoriamente uma linha de cada range.
# 
# A estratégia de escolha das amostras busca trazer exemplares de diferentes tipos
# de variância, visto que há exemplos mal comportados que ainda assim não perdem
# pacotes. Dizer que essa é a melhor estratégia carece de investigação a fundo
# e comparação com outras estratégias, mas para o escopo desse trabalho, acredito
# ser suficientemente boa.
#
# Para a seleção aleatória das linhas sem perda, usarei um algoritmo de geração
# de números aleatórios conforme teoria apresentada em aula.
#
# Ao escrever as linhas no arquivo, aproveito para eliminar a coluna de timestamp
# e a de variância. Aproveito também para transformar a coluna de perda, traduzindo
# qualquer valor diferente de 0.0 como "1" e os valores "0.0" como 0.
#
# Ao escrever os valores de média, min, max e variancia, aproveito para truncar os
# valores em oito casas decimais, para evitar grandes problemas com float64 (16 casas
# decimais + expoente). Como estou tratando tudo aqui como string, uso máscara para isso.
#
# Com o conjunto final tendo 50% de representatividade de exemplos com perda e 50%
# sem perda, usarei as ferramentas do scikit-learn para separar esse conjunto
# entre treinamento e validação com uma proporção de 80/20. Isso será feito no código
# de treinamento de modelo.


# Diretório com os CSVs.
script_directory = pathlib.Path(__file__).parent.resolve()
file_path = os.listdir(str(script_directory) + '\\json_data_2\\')

# Arquivo com os dados retirados dos CSVs a serem usados na próxima fase
processed_file = open(str(script_directory) + "\\processed_file.csv", "w", encoding="utf-8", newline='')
writer = csv.writer(processed_file)

# Inicializo meu RNG.
rng = my_rng(0xBEBACAFE)

# Para cada arquivo CSV, leio numa lista e ordeno pelo packet loss (coluna 6)
for file in file_path:
    with open(str(script_directory) + "\\json_data_2\\" + file, 'r', encoding="utf-8") as f:
        reader = list(csv.reader(f, delimiter=","))

        # Elimino linhas sem variância. Que sintaxe louca.
        reader[:] = [tup for tup in reader if tup[5] != "0.0"]

        # Conto as ocorrências de pacotes perdidos ou não
        packet_was_lost = 0
        packet_zero_lost = 0
        for row in reader:
            if row[6] == "0.0":
                packet_zero_lost += 1
            else:
                packet_was_lost += 1

        # se não tenho exemplares de perda ou zero perda, não me interessa.
        if (not packet_was_lost) or (not packet_zero_lost):
            continue

        # Se tive menos ocorrência de perda de pacote do que zero perda,
        # ordeno a lista de forma decrescente, para ter as perdas em cima.
        # Do contrário, tive menos ocorrências de zero perda do que alguma perda,
        # então ordeno a lista de forma crescente, para ter as zero perdas em cima.
        reverse_order = False
        if packet_was_lost < packet_zero_lost:
            reverse_order = True
        else:
            reverse_order = False

        # Ordeno conforme a necessidade pela coluna de perda
        sortedlist = sorted(reader, key=lambda row: row[6], reverse=reverse_order)
        n = 0

        # Conto as n primeiras linhas, seja de perda ou sem perda
        for line in sortedlist:
            if (line[6] != "0.0" and reverse_order) or (line[6] == "0.0" and not reverse_order):
                n += 1
                perda = 0 if line[6] == "0.0" else 1
                # Gravo as colunas que interessam
                writer.writerow((#line[0], 
                                 "{:.8s}".format(line[1]),
                                 "{:.8s}".format(line[2]),
                                 "{:.8s}".format(line[3]),
                                 #"{:.8s}".format(line[4]),
                                 "{:.8s}".format(line[5]),
                                 perda))
            else: # cheguei ao fim das linhas iniciais que queria, posso sair
                break
        del sortedlist[:n] # removo as n primeiras linhas

        # Reordeno a lista por variância, defino os ranges de tamanho len(lista)//N
        # e seleciono aleatoriamente uma amostra de cada range usando a minha função RNG.
        # A divisão len(lista)//N pode ter resto. Lido com ele ao calculá-lo por
        # remainder = len(lista) % N, depois em cada range, a começar pelo primeiro,
        # adiciono 1 item ao range e decremento o remainder até que seja zero. Após isso,
        # os ranges terão o tamanho do quociente da divisão como originalmente calculado.
        sortedlist_byvariance = sorted(sortedlist, key=lambda row: row[4], reverse=False)
        list_size = len(sortedlist_byvariance)
        quotient = list_size // n
        remainder = list_size % n
        ini = 0
        for i in range(0, n):
            if not ini: # primeira execução, ini vale zero
                end = quotient if remainder else (quotient - 1)

            if end == 0: # edge case: tenho apenas 1 item na lista
                x = 0
            else:
                x = (next(rng) % (end - ini)) + ini

            line = sortedlist_byvariance[x]
            perda = 0 if line[6] == "0.0" else 1
            # Gravo as colunas que interessam
            writer.writerow((#line[0], 
                            "{:.8s}".format(line[1]),
                            "{:.8s}".format(line[2]),
                            "{:.8s}".format(line[3]),
                            #"{:.8s}".format(line[4]),
                            "{:.8s}".format(line[5]),
                            perda))

            # atualizo as variáveis
            ini = end + 1
            end = (end + quotient + 1) if remainder else (end + quotient)
            remainder = 0 if not remainder else (remainder - 1) # atualizo remainder
    f.close
processed_file.close
