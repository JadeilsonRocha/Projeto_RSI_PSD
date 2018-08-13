
#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import unicodedata
import binascii

caminho = os.getcwd()
listaNomesDados = os.listdir(caminho)
macsGenuinos=[]
macsFal=[]
listaTodosDados =[]

for arq in listaNomesDados:
    if os.path.isfile(arq) and arq.lower().endswith(".txt"):
        dados = open(str(arq), encoding="utf8")
        listaDados = dados.readlines()
        listaTodosDados.append(listaDados)
        dados.close()

def converterMac(mac):
    scale = 16
    num_of_bits = 8
    binario = bin(int(mac, scale))[2:].zfill(num_of_bits)
    if binario[-2] =='0':
        return True
    return False
arq = open("limpo04.txt", "w+", encoding="utf8")
for ListaMac in listaTodosDados:
    for mac in ListaMac:
        linha = mac
        mac = mac.replace('\n',"").split(",")
        mac2= mac[2]
        x = mac2[:-15]
        if converterMac(x):
            arq.writelines(linha)
            if mac2 not in macsGenuinos:
                macsGenuinos.append(mac2)
        else:
            if mac2 not in macsFal:
                macsFal.append(mac2)

print("Macs genuinos SEM repetição:",len(macsGenuinos))
print("Macs Falsos SEM repetição:",len(macsFal))
arq.close()
