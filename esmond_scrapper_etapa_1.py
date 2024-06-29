import requests
import pathlib
import time

script_directory = pathlib.Path(__file__).parent.resolve()

# Array com os códigos dos estados
uf = ["ac", "al", "am", "ap", "ba", "ce", "df", "es", "go", "ma", \
      "mg", "ms", "mt", "pa", "pb", "pe", "pi", "pr", "rj", "rn", \
      "ro", "rr", "rs", "sc", "se", "sp", "to"]


# Primeira etapa: consulto o Esmond para pegar todas as metadata-keys
# da última semana entre todos os pares de UFs
base_url = 'https://monipe-central.rnp.br'
for i in uf:
    for j in uf:
        if i == j: # não computo quando src e dst são iguais
            continue
        webdir = '/esmond/perfsonar/archive/?source=monipe-' + i + \
              '-atraso.rnp.br&destination=monipe-' + j + '-atraso.rnp.br&time-range=604800'
        
        time.sleep(0.5) # 2 páginas por segundo
        response = requests.get(base_url + webdir, verify=False) # pego o json e gravo em disco
        with open(str(script_directory) + '\\json_data\\' + i + '-' + j + '.json', \
                  "w", encoding="utf-8") as file:
            file.write(response.text)
        file.close()
