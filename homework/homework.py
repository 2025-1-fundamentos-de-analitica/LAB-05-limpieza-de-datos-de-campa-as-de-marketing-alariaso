"""
Escriba el codigo que ejecute la accion solicitada.
"""

import calendar
import pandas as pd
from zipfile import ZipFile
from pathlib import Path

# pylint: disable=import-outside-toplevel


months = {v: k for (k,v) in enumerate(map(str.lower, calendar.month_abbr))}


def yesno_to_num(x, s="yes"):
    return 1 if x == s else 0


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """

    dfs = []

    for file in Path("files/input").iterdir():
        with ZipFile(file) as file:
            for member in file.namelist():
                with file.open(member) as csvfile:
                    df = pd.read_csv(csvfile, index_col=0)
                    df = df.set_index("client_id")
                    dfs.append(df)


    df = pd.concat(dfs)
    df["nmonth"] = df["month"].map(months)
    df["day"] = df["day"].apply(lambda x: f"{x:02d}")
    clients = df[["age", "job", "marital", "education", "credit_default", "mortgage"]].copy()
    clients["job"] = clients["job"].str.replace(".", "").str.replace("-", "_")
    clients["education"] = clients["education"].str.replace(".", "_").replace("unknown", pd.NA)
    clients["credit_default"] = clients["credit_default"].apply(yesno_to_num)
    clients["mortgage"] = clients["mortgage"].apply(yesno_to_num)

    campaign = df[["number_contacts", "contact_duration", "previous_campaign_contacts", "previous_outcome", "campaign_outcome"]].copy()
    campaign["previous_outcome"] = campaign["previous_outcome"].apply(yesno_to_num, s="success")
    campaign["campaign_outcome"] = campaign["campaign_outcome"].apply(yesno_to_num)
    campaign["last_contact_date"] = df["nmonth"].apply(lambda x: f"2022-{x:02d}-") + df["day"]

    economics = df[["cons_price_idx", "euribor_three_months"]].copy()

    outdir = Path("files/output")
    outdir.mkdir(parents=True, exist_ok=True)

    clients.to_csv(outdir / "client.csv")
    campaign.to_csv(outdir / "campaign.csv")
    economics.to_csv(outdir / "economics.csv")

    return


if __name__ == "__main__":
    clean_campaign_data()
