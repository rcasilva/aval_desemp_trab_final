import os
import json
import requests
import pathlib
import time

script_directory = pathlib.Path(__file__).parent.resolve()

# Segunda etapa: leio os jsons da etapa 1 e busco pelos que contenham
# dados de "histogram-rtt/statistics/0" e "packet-count-lost-bidir/base".
# Caso exista, gravo num arquivo.
# Depois, varro o arquivo buscando no Esmond os dados.
# Por fim, gravo em arquivo .csv os dados que interessam. Isso já é
# uma primeira etapa de pré-processamento que decidi fazer "on-the-fly"

base_url = 'https://monipe-central.rnp.br'
file_path = os.listdir(str(script_directory) + '\\json_data\\')

for file in file_path:
    with open(str(script_directory) + "\\json_data\\" + file, 'r', encoding="utf-8") as f:
        jsondata = json.load(f)
    f.close

    rtt_uri = "UNKNOWN"
    plrb_uri = "UNKNOWN"
    for i in jsondata:
        # vejo se é um teste de rtt
        if i['pscheduler-test-type'] == "rtt":
            for j in i['event-types']:
                # vejo se é histograma
                if j['event-type'] == "histogram-rtt":
                    for k in j['summaries']:
                        # vejo se tenho estatísticas granulares de rtt
                        if k['summary-type'] == "statistics" and k['summary-window'] == "0":
                            rtt_uri = k['uri']
                # vejo se tenho packet loss
                elif j['event-type'] == "packet-loss-rate-bidir":
                    plrb_uri = j['base-uri']
    
    # Caso não tenha encontrado alguma das URIs, ignoro
    if(rtt_uri == "UNKNOWN" or plrb_uri == "UNKNOWN"):
        continue

    # Só no final notei que essa linha cria arquivos xpto.json.csv. Segue o jogo. São CSVs.
    with open(str(script_directory) + '\\json_data_2\\' + os.path.basename(file).split()[0] + ".csv", \
              "w", encoding="utf-8") as f2:
        
        time.sleep(0.5)
        response = requests.get(base_url + rtt_uri + "?limit=10000000", verify=False)

        time.sleep(0.5)
        response2 = requests.get(base_url + plrb_uri + "?limit=10000000", verify=False)

        line = ""
        pktloss = {}

        # Leio os packet losses indexados pelo timestamp
        for i in json.loads(response2.text):
            pktloss[str(i['ts'])] = i['val']

        # Vou montar um CSV a partir do JSON porque prefiro CSV
        for i in json.loads(response.text):
            # vejo se tenho packet loss pro timestamp atual do rtt
            if str(i['ts']) in pktloss:
                # monto a linha do CSV com o que interessa
                line = str(i['ts']) + "," + \
                    str(i['val']['mean']) + "," + \
                    str(i['val']['minimum']) + "," + \
                    str(i['val']['maximum']) + "," + \
                    str(i['val']['variance']) + "," + \
                    str(i['val']['standard-deviation']) + "," + \
                    str(pktloss[str(i['ts'])]) + "\n"
                f2.write(line)
        f2.close
