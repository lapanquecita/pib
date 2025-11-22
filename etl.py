"""

Fuente PIB por industria:
https://www.inegi.org.mx/temas/pib/#tabulados

Fuente PIB por entidad:
https://www.inegi.org.mx/programas/pibent/2018/#datos_abiertos

"""

import pandas as pd

# Cada entidad tiene su propio archivo.
# El cual tiene la abrevitura codificada.
ENTIDADES = {
    "nac": "Nacional",
    "ags": "Aguascalientes",
    "bc": "Baja California",
    "bcs": "Baja California Sur",
    "camp": "Campeche",
    "coah": "Coahuila",
    "col": "Colima",
    "chis": "Chiapas",
    "chih": "Chihuahua",
    "cdmx": "Ciudad de México",
    "dgo": "Durango",
    "gto": "Guanajuato",
    "gro": "Guerrero",
    "hgo": "Hidalgo",
    "jal": "Jalisco",
    "méx": "Estado de México",
    "mich": "Michoacán",
    "mor": "Morelos",
    "nay": "Nayarit",
    "nl": "Nuevo León",
    "oax": "Oaxaca",
    "pue": "Puebla",
    "qro": "Querétaro",
    "qr": "Quintana Roo",
    "slp": "San Luis Potosí",
    "sin": "Sinaloa",
    "son": "Sonora",
    "tab": "Tabasco",
    "tamps": "Tamaulipas",
    "tlax": "Tlaxcala",
    "ver": "Veracruz",
    "yuc": "Yucatán",
    "zac": "Zacatecas",
}


# Este diccionario será utilizado para asignar
# la clave a cada entidad.
CLAVES_ENTIDADES = {
    "Nacional": 0,
    "Aguascalientes": 1,
    "Baja California": 2,
    "Baja California Sur": 3,
    "Campeche": 4,
    "Coahuila": 5,
    "Colima": 6,
    "Chiapas": 7,
    "Chihuahua": 8,
    "Ciudad de México": 9,
    "Durango": 10,
    "Guanajuato": 11,
    "Guerrero": 12,
    "Hidalgo": 13,
    "Jalisco": 14,
    "Estado de México": 15,
    "Michoacán": 16,
    "Morelos": 17,
    "Nayarit": 18,
    "Nuevo León": 19,
    "Oaxaca": 20,
    "Puebla": 21,
    "Querétaro": 22,
    "Quintana Roo": 23,
    "San Luis Potosí": 24,
    "Sinaloa": 25,
    "Sonora": 26,
    "Tabasco": 27,
    "Tamaulipas": 28,
    "Tlaxcala": 29,
    "Veracruz": 30,
    "Yucatán": 31,
    "Zacatecas": 32,
}


def crear_industrias():
    """
    Genera el dataset de PIB por industria.
    """

    # Esta lista almacenará DataFrames individuales.
    dfs = list()

    # Cargamos el dataset de cifras constantes.
    constante = cargar_dataset("PIBT_2")

    # Cargamos el dataset de cifras corrientes.
    corriente = cargar_dataset("PIBT_3")

    # Iteramos sobre cada categoría (inudstria).
    for col in constante.columns:
        # Creamos un DataFrame para cada categoría/industria.
        temp_df = constante[col].to_frame("VALOR_CONSTANTE")

        # Extraemos la clave y nombre de la industria.
        temp_df["CLAVE_INDUSTRIA"] = col.split(" - ")[0].strip()
        temp_df["INDUSTRIA"] = col.split(" - ")[-1].strip()

        # Agregamos el valor corriente desde el otro DataFrame.
        temp_df["VALOR_CORRIENTE"] = corriente[col]

        # Agregamos el DataFrame temporal a nuestra lista de DataFrames.
        dfs.append(temp_df)

    # Unimos todos los DataFrames.
    final = pd.concat(dfs)

    # Renombramos algunas claves.
    final["CLAVE_INDUSTRIA"] = final["CLAVE_INDUSTRIA"].replace(
        {
            "____aB.1bP": "PIB",
            "____aB.1bV": "VAB",
            "Actividades primarias": "1",
            "Actividades secundarias": "2",
            "Actividades terciarias": "4",
        }
    )

    # Nombramos el índice.
    final.index.name = "PERIODO"

    # Ordenamos las columnas.
    final = final[
        ["CLAVE_INDUSTRIA", "INDUSTRIA", "VALOR_CONSTANTE", "VALOR_CORRIENTE"]
    ]

    # Desanualizamos las cifras.
    final["VALOR_CONSTANTE"] /= 4
    final["VALOR_CORRIENTE"] /= 4

    # Quitamos valores nulos.
    final = final.dropna(axis=0)

    # Guardamos el archivo.
    final.to_csv("./pib_industrias.csv", encoding="utf-8")


def cargar_dataset(ruta):
    """
    Carga el dataset y lo prepara para transformación.

    Parameters
    ----------
    ruta: str
        La ruta del archivo.

    """

    # Cada año tiene 7 columnas.
    # Una para cada trimestre, y una para 6, 9 y 12 meses.
    # Solo necesitamos la de los trimestres, sin embargo
    # los nombres requieren mucha limpieza.
    # Así que mejor le damos los nombres finales desde ahora.
    periodos = list()

    for año in range(1993, 2026):
        periodos.extend(
            [
                f"{año}-01-01",
                f"{año}-04-01",
                f"{año}-07-01",
                f"{año}-10-01",
                "",
                "",
                "",
            ]
        )

    # Cargamos el dataset especificado.
    df = pd.read_excel(f"./source_industrias/{ruta}.xlsx", index_col=0, header=4)

    # Solo necesitamos las filas con cifras y no cambio porcentual.
    df = df.iloc[2:186]

    # Quitamos los espacios en blanco en el índice.
    df.index = df.index.str.strip()

    # Invertimos los ejes.
    # Ahora las fechas son el índice.
    df = df.transpose()

    # Los periodos definidos al comienzo de la función
    # serán el nuevo índice.
    df.index = periodos

    # Quitamos filas que no sean trimestres.
    df = df[df.index != ""]

    return df


def crear_estados():
    """
    Genera el dataset de PIB por entidad.
    """

    # Esta lista almacenará DataFrames individuales.
    dfs = list()

    # Definimos la ruta base para los archivos de PIB por entidad.
    ruta_base = "./source_estatal/conjunto_de_datos_pibe_entidad_{}2023_r.csv"

    # Iteramos sobre cada entidad.
    for k, v in ENTIDADES.items():
        # Cargamos el dataset.
        df = pd.read_csv(ruta_base.format(k), index_col=0)

        # Quitamos sufijos de las columnas.
        df.columns = [col[:4] for col in df.columns]

        # Limpiamos el índice.
        df.index = df.index.map(lambda x: x.split("|")[-1].replace("<C1>", "").strip())

        # Creamos DataFrames para valores constantes y corrientes.
        constante = df.iloc[:44].transpose()
        corriente = df.iloc[220:264].transpose()

        # Iteramos sobre cada categoría.
        for col in constante.columns:
            # Creamos un DataFrame para cada categoría/industria.
            temp_df = constante[col].to_frame("VALOR_CONSTANTE")

            # Agregamos una columna con el nombre de la entidad.
            temp_df["ENTIDAD"] = v

            # Extraemos la clave y nombre de la industria.
            temp_df["CLAVE_INDUSTRIA"] = col.split("---")[0].strip()
            temp_df["INDUSTRIA"] = col.split("---")[-1].strip()

            # Agregamos el valor corriente desde el otro DataFrame.
            temp_df["VALOR_CORRIENTE"] = corriente[col]

            # Agregamos el DataFrame temporal a nuestra lista de DataFrames.
            dfs.append(temp_df)

    # Unimos todos los DataFrames.
    final = pd.concat(dfs)

    # Renombramos algunas claves.
    final["CLAVE_INDUSTRIA"] = final["CLAVE_INDUSTRIA"].replace(
        {
            "B.1bP": "PIB",
            "Actividad económica total": "VAB",
            "Actividades primarias": "1",
            "Actividades secundarias": "2",
            "Actividades terciarias": "4",
        }
    )

    # Renombremos la cateogría de Actividad económica total a
    # Valor agregrado bruto. Ya que este es el término más reconocido.
    final["INDUSTRIA"] = final["INDUSTRIA"].replace(
        {"Actividad económica total": "Valor agregado bruto"}
    )

    # Nombramos el índice.
    final.index.name = "PERIODO"

    # Agregamos la clave de cada entidad.
    final["CVE_ENT"] = final["ENTIDAD"].map(CLAVES_ENTIDADES)

    # Ordenamos las columnas.
    final = final[
        [
            "CVE_ENT",
            "ENTIDAD",
            "CLAVE_INDUSTRIA",
            "INDUSTRIA",
            "VALOR_CONSTANTE",
            "VALOR_CORRIENTE",
        ]
    ]

    # Guardamos el archivo.
    final.to_csv("./pib_estatal.csv", encoding="utf-8")


if __name__ == "__main__":
    crear_industrias()
    crear_estados()
