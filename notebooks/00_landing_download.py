# %% [markdown]
# # 00 · Landing: download dos dados abertos da ANEEL
# Busca o recurso CSV de **Geração Distribuída** pela API CKAN do portal de dados abertos
# e grava o arquivo em `Files/landing/` do Lakehouse, particionado pela data da carga.
# No Fabric, este notebook é a primeira atividade do pipeline `pl_aneel_gd`.

# %% tags=["parameters"]
data_carga = ""  # yyyy-MM-dd; vazio = hoje. Preenchido pelo pipeline.

# %%
import datetime as dt
import os

import requests

API = "https://dadosabertos.aneel.gov.br/api/3/action/package_show"
PACOTE = "relacao-de-empreendimentos-de-geracao-distribuida"

data_carga = data_carga or dt.date.today().isoformat()
recursos = requests.get(API, params={"id": PACOTE}, timeout=60).json()["result"]["resources"]
csv = next(r for r in recursos if r["format"].upper() == "CSV" and "empreendimento" in r["url"].lower())
print(csv["name"], csv["url"])

# %%
destino = f"/lakehouse/default/Files/landing/geracao_distribuida/data_carga={data_carga}/gd.csv"
os.makedirs(os.path.dirname(destino), exist_ok=True)
with requests.get(csv["url"], stream=True, timeout=600) as resp:
    resp.raise_for_status()
    with open(destino, "wb") as f:
        for bloco in resp.iter_content(chunk_size=8 * 1024 * 1024):
            f.write(bloco)

print(f"{os.path.getsize(destino) / 1e6:.1f} MB gravados em {destino}")
mssparkutils.notebook.exit(data_carga)  # disponível no runtime do Fabric
