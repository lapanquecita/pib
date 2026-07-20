import json
import os

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from PIL import Image
from plotly.subplots import make_subplots


# Mes y año en que se recopilaron los datos.
FECHA_FUENTE = "julio 2026"


# Definimos los colores que usaremos para las visualizaciones.
PLOT_COLOR = "#1C1F1A"
PAPER_COLOR = "#262B23"
HEADER_COLOR = "#C25B42"


def mapa_per_capita(año, clave_industria):
    """
    Genera un mapa choropleth con una comparación estatal
    per cápita para el año e industria especificados.

    Parameters
    ----------
    año : int
        El año que nos interesa graficar.

    clave_industria : str
        El identificador de la industria.
        Basado en el SCIAN.

    """

    # Cargamos el dataset de la polación total estimada según el CONAPO.
    pop = pd.read_csv("./assets/poblacion.csv", index_col=0)

    # Seleccionamos la población del año especificado.
    pop = pop[str(año)]

    # Cargamos el dataset del PIB estatal.
    df = pd.read_csv("./pib_estatal.csv", index_col="ENTIDAD")

    # Filtramos por el año especificado.
    df = df[df["PERIODO"] == año]

    # Filtramos por la industria especificada.
    df = df[df["CLAVE_INDUSTRIA"] == clave_industria]

    # Extraemos el nombre de la industria.
    nombre_industria = df["INDUSTRIA"].unique()[0].lower()

    # Creamos una nueva columna para usar pesos en vez de millones de pesos.
    df["pesos"] = df["VALOR_CONSTANTE"] * 1000000

    # Agregamos la población a cada estado.
    # En este proceso quitaremos la fila para nacional.
    # Esto se calculará por separado.
    df = df.join(pop).dropna()

    # Calculamos los totales a nivel nacional.
    poblacion_total = pop.sum()
    valor_total = df["pesos"].sum()
    total_per_capita = valor_total / poblacion_total

    # Preparamos el subtítulo.
    subtitulo = f"Nacional: <b>{total_per_capita:,.0f}</b> pesos per cápita"

    # Calculamos el valor per cápita para cada entidad.
    df["capita"] = df["pesos"] / df[str(año)]

    # Ordenamos per cápita de mayor a menor.
    df = df.sort_values("capita", ascending=False)

    # Estos valores serán usados para definir la escala en el mapa.
    valor_min = df["capita"].min()
    valor_max = df["capita"].quantile(0.975)

    # Preparamos las marcas y etiquetas para la escala.
    # No usamos el valor máximo para el límite superior
    # ya que en ocasiones tenemos valores atípicos.
    marcas = np.linspace(valor_min, valor_max, 11)
    etiquetas = [f"{item / 1000:,.0f}k" for item in marcas]

    # Agregamos el símblo de mayor o igual que.
    etiquetas[-1] = f"≥{etiquetas[-1]}"

    # Cargamos el archivo GeoJSON de México.
    geojson = json.loads(open("./assets/mexico.json", "r", encoding="utf-8").read())

    # El formato del título cambia ligeramente con el tipo de dinsturia.
    if clave_industria == "PIB":
        titulo = f"PIB per cápita en México por entidad en {año}"
    else:
        titulo = f"Valor de <b>{nombre_industria}</b> per cápita en México por entidad en {año}"

    fig = go.Figure()

    # Vamos a crear un mapa Choropleth con todas las variables anteriormente definidas.
    fig.add_traces(
        go.Choropleth(
            geojson=geojson,
            locations=df.index,
            z=df["capita"],
            featureidkey="properties.NOM_ENT",
            colorscale="matter_r",
            marker_line_color="#FFFFFF",
            marker_line_width=1.5,
            zmin=valor_min,
            zmax=valor_max,
            colorbar=dict(
                x=0.03,
                y=0.5,
                ypad=50,
                ticks="outside",
                outlinewidth=2,
                tickvals=marcas,
                ticktext=etiquetas,
                tickwidth=3,
                tickcolor="#EEEEEE",
                outlinecolor="#EEEEEE",
                ticklen=10,
            ),
        )
    )

    # Personalizamos la apariencia del mapa.
    fig.update_geos(
        fitbounds="geojson",
        showocean=True,
        oceancolor=PLOT_COLOR,
        showcountries=False,
        framecolor="#EEEEEE",
        framewidth=2,
        showlakes=False,
        coastlinewidth=0,
        landcolor="#1C0A00",
    )

    fig.update_layout(
        showlegend=False,
        font_family="Inter",
        font_color="#FFFFFF",
        font_size=28,
        margin_t=80,
        margin_r=40,
        margin_b=60,
        margin_l=40,
        width=1920,
        height=1080,
        paper_bgcolor=PAPER_COLOR,
        annotations=[
            dict(
                x=0.5,
                y=1.025,
                xanchor="center",
                yanchor="top",
                text=titulo,
                font_size=42,
            ),
            dict(
                x=0.99,
                y=0.93,
                xanchor="right",
                yanchor="top",
                align="left",
                bordercolor="#FFFFFF",
                borderwidth=1,
                borderpad=7,
                bgcolor=PLOT_COLOR,
                text="<b>Nota:</b> Cifras expresadas en<br>pesos constantes, base 2018.",
            ),
            dict(
                x=0.0275,
                y=0.46,
                textangle=-90,
                xanchor="center",
                yanchor="middle",
                text="Pesos per cápita durante el año",
            ),
            dict(
                x=0.01,
                y=-0.056,
                xanchor="left",
                yanchor="top",
                text=f"Fuentes: INEGI, CONAPO ({FECHA_FUENTE})",
            ),
            dict(
                x=0.5,
                y=-0.056,
                xanchor="center",
                yanchor="top",
                text=subtitulo,
            ),
            dict(
                x=1.01,
                y=-0.056,
                xanchor="right",
                yanchor="top",
                text="🧁 @lapanquecita",
            ),
        ],
    )

    fig.write_image("./0.png")

    # Vamos a crear dos tablas, cada una con la información de 16 entidades.
    fig = make_subplots(
        rows=1,
        cols=2,
        horizontal_spacing=0.03,
        specs=[[{"type": "table"}, {"type": "table"}]],
    )

    fig.add_trace(
        go.Table(
            columnwidth=[120, 110, 120],
            header=dict(
                values=[
                    "<b>Entidad</b>",
                    "<b>Valor (MDP)</b>",
                    "<b>Pesos per cápita ↓</b>",
                ],
                line_color="#EEEEEE",
                fill_color=HEADER_COLOR,
                align="center",
                height=45,
                line_width=0.8,
            ),
            cells=dict(
                values=[
                    df.index[:16],
                    df["VALOR_CONSTANTE"][:16],
                    df["capita"][:16],
                ],
                fill_color=PLOT_COLOR,
                height=45,
                format=["", ",.0f"],
                line_width=0.8,
                align=["left", "center"],
            ),
        ),
        col=1,
        row=1,
    )

    fig.add_trace(
        go.Table(
            columnwidth=[120, 110, 120],
            header=dict(
                values=[
                    "<b>Entidad</b>",
                    "<b>Valor (MDP)</b>",
                    "<b>Pesos per cápita ↓</b>",
                ],
                line_color="#EEEEEE",
                fill_color=HEADER_COLOR,
                align="center",
                height=45,
                line_width=0.8,
            ),
            cells=dict(
                values=[
                    df.index[16:],
                    df["VALOR_CONSTANTE"][16:],
                    df["capita"][16:],
                ],
                fill_color=PLOT_COLOR,
                height=45,
                format=["", ",.0f"],
                line_width=0.8,
                align=["left", "center"],
            ),
        ),
        col=2,
        row=1,
    )

    fig.update_layout(
        width=1920,
        height=840,
        font_family="Inter",
        font_color="#FFFFFF",
        font_size=28,
        margin_t=25,
        margin_l=40,
        margin_r=40,
        margin_b=0,
        paper_bgcolor=PAPER_COLOR,
    )

    fig.write_image("./1.png")

    # Unimos el mapa y las tablas en una sola imagen.
    image1 = Image.open("./0.png")
    image2 = Image.open("./1.png")

    result_width = image1.width
    result_height = image1.height + image2.height

    result = Image.new("RGB", (result_width, result_height))
    result.paste(im=image1, box=(0, 0))
    result.paste(im=image2, box=(0, image1.height))

    result.save(f"./mapa_{clave_industria}_{año}.png")

    # Borramos las imágenes originales.
    os.remove("./0.png")
    os.remove("./1.png")


def comparacion_interanual(clave_industria, primer_año, segundo_año):
    """
    Genera una gráfica de barras horizontal
    con la comparación internaual para la
    industria especificada.

    Parameters
    ----------
    clave_industria : str
        El identificador de la industria.
        Basado en el SCIAN.
    
    primre_año : int
        El año base que nos interesa comparar.

    segundo_año : int
        El año destino que nos interesa comparar.

    """

    # Cargamos el dataset de PIB estatal.
    df = pd.read_csv("./pib_estatal.csv")

    # Seleccionamos los años especificados.
    df = df[df["PERIODO"].isin([primer_año, segundo_año])]

    # Filtramos por la idnustria especificada.
    df = df[df["CLAVE_INDUSTRIA"] == clave_industria]

    # Extraemos el nombre de la industria.
    nombre_industria = df["INDUSTRIA"].unique()[0].lower()

    # Convertimos las cifras de millones a billones de pesos.
    df["VALOR_CONSTANTE"] /= 1000000

    # Transformamos el DataFrame para que el ídnice sean las entidades y las columnas los años.
    df = df.pivot_table(index="ENTIDAD", columns="PERIODO", values="VALOR_CONSTANTE")

    # Resaltamos la etiqueta para el valor naiconal.
    df.index = df.index.map(lambda x: "<b>Nacional</b>" if x == "Nacional" else x)

    # Calculamos el cambio porcentual.
    df["cambio"] = (df[segundo_año] - df[primer_año]) / df[primer_año] * 100

    # Preparamos el texto para cada barra.
    # Si el cambio porcentual es mayor al 100% usaremos solo un punto decimal.
    df["texto"] = df.apply(
        lambda x: (
            f" <b>{x['cambio']:,.0f}%</b> ({x[primer_año]:,.2f} → {x[segundo_año]:,.2f}) "
            if abs(x["cambio"]) >= 100
            else f" <b>{x['cambio']:,.1f}%</b> ({x[primer_año]:,.2f} → {x[segundo_año]:,.2f}) "
        ),
        axis=1,
    )

    # Ordenamos de mayor a menor basado en el cambio porcentual.
    df.sort_values("cambio", ascending=False, inplace=True)

    # Ordenamos de mayor a menor basado en el cambio porcentual.
    df.sort_values("cambio", inplace=True)

    # Calculamos el valor máximo para ajustar el rango del eje horizontal.
    valor_max = df["cambio"].abs().max()
    valor_max = ((valor_max // 2) + 1) * 2

    # Determinamos la posición de los textos para cada barra.
    df["ratio"] = df["cambio"].abs() / valor_max
    df["texto_pos"] = df["ratio"].apply(lambda x: "inside" if x >= 0.7 else "outside")

    if clave_industria == "PIB":
        titulo = f"Comparación internaual del <b>Producto Interno Bruto</b> de México ({primer_año} vs. {segundo_año})"
    else:
        titulo = f"Comparación internaual del valor de <b>{nombre_industria}</b> de México ({primer_año} vs. {segundo_año})"

    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            y=df.index,
            x=df["cambio"],
            text=df["texto"],
            textposition=df["texto_pos"],
            textfont_color="#FFFFFF",
            orientation="h",
            marker_color=df["cambio"],
            marker_colorscale="geyser_r",
            marker_cmid=0,
            marker_line_width=0,
            textfont_size=30,
            textfont_family="Oswald",
        )
    )

    fig.update_xaxes(
        range=[valor_max * -1, valor_max],
        ticksuffix="%",
        ticks="outside",
        ticklen=10,
        zeroline=False,
        tickcolor="#EEEEEE",
        linecolor="#EEEEEE",
        linewidth=2,
        showline=True,
        gridwidth=0.5,
        mirror=True,
        nticks=20,
    )

    fig.update_yaxes(
        ticks="outside",
        ticklen=10,
        tickcolor="#EEEEEE",
        linecolor="#EEEEEE",
        linewidth=2,
        gridwidth=0.5,
        showline=True,
        mirror=True,
    )

    fig.update_layout(
        showlegend=False,
        width=1920,
        height=1920,
        font_family="Inter",
        font_color="#FFFFFF",
        font_size=24,
        title_text=titulo,
        title_x=0.5,
        title_y=0.985,
        margin_t=80,
        margin_r=40,
        margin_b=120,
        margin_l=280,
        title_font_size=36,
        paper_bgcolor=PAPER_COLOR,
        plot_bgcolor=PLOT_COLOR,
        annotations=[
            dict(
                x=0.99,
                y=0.0,
                xref="paper",
                yref="paper",
                xanchor="right",
                yanchor="bottom",
                align="left",
                bgcolor=PLOT_COLOR,
                bordercolor="#FFFFFF",
                borderwidth=1.5,
                borderpad=7,
                text="<b>Nota:</b><br>Cifras expresadas en billones de pesos<br>a precios constantes de 2018.",
            ),
            dict(
                x=0.01,
                y=-0.06,
                xref="paper",
                yref="paper",
                xanchor="left",
                yanchor="top",
                text=f"Fuente: INEGI ({FECHA_FUENTE})",
            ),
            dict(
                x=0.58,
                y=-0.06,
                xref="paper",
                yref="paper",
                xanchor="center",
                yanchor="top",
                text="Cambio porcentual (cambio absoluto)",
            ),
            dict(
                x=1.0,
                y=-0.06,
                xref="paper",
                yref="paper",
                xanchor="right",
                yanchor="top",
                text="🧁 @lapanquecita",
            ),
        ],
    )

    fig.write_image(f"./interanual_{clave_industria}_{primer_año}_{segundo_año}.png")


def composicion_vab(año):
    """
    Genera una gráfica de barras normalizada
    comparando la estructura económica por entidad.

    Parameters
    ----------
    año : int
        El año que nos interesa graficar.

    """

    # Cargamos el dataset del PIB estatal.
    df = pd.read_csv("./pib_estatal.csv", index_col="ENTIDAD")

    # Filtramos por el año especificado.
    df = df[df["PERIODO"] == año]

    # Seleccionamos las actividades primarias (1), secundarias (2) y terciarias (4).
    df = df[df["CLAVE_INDUSTRIA"].isin(["1", "2", "4"])]

    # Transformamos el DataFrame para que el índice sean las entidades y las actividades las columnas.
    df = df.pivot_table(index="ENTIDAD", columns="INDUSTRIA", values="VALOR_CONSTANTE")

    # Creamos una columna para el total por entidad.
    df["total"] = df.sum(axis=1)

    # Calculamos la proporción de cada actividad.
    for col in df.columns[:-1]:
        df[col] = df[col] / df["total"] * 100

    # Ordenamos por proporción de actividades terciarias.
    # Estas son las más representativas.
    df.sort_values("Actividades terciarias", inplace=True)

    # Resaltamos la etiqueta para los valores a nivel nacional.
    df.index = df.index.map(lambda x: "<b>Nacional</b>" if x == "Nacional" else x)

    # Renombramos las columnas con alugnos ejemplos de cada tipo de actividad.
    # Esto se verá reflejado en la leyenda.
    nombres = {
        "Actividades primarias": "<b>Actividades primarias</b><br>(agricultura, ganadería y pesca)",
        "Actividades secundarias": "<b>Actividades secundarias</b><br>(industria, manufactura y construcción)",
        "Actividades terciarias": "<b>Actividades terciarias</b><br>(comercio, transporte y servicios)",
    }

    fig = go.Figure()

    # Vamos a crear una gráfica de barras horizontal
    # para cada tipo de actividad económica.
    for col in df.columns[:-1]:
        fig.add_trace(
            go.Bar(
                y=df.index,
                x=df[col],
                text=df[col],
                texttemplate=" %{text:,.1f}% ",
                name=nombres[col],
                textposition="inside",
                orientation="h",
                marker_line_width=0,
                textfont_family="Oswald",
                textfont_size=40,
                textfont_color="#FFFFFF",
            )
        )

    # Nos aseguramos que el rango sea de 0 a 100.
    fig.update_xaxes(
        range=[0, 100],
        ticksuffix="%",
        ticks="outside",
        ticklen=10,
        zeroline=False,
        tickcolor="#EEEEEE",
        linecolor="#EEEEEE",
        linewidth=2,
        showline=True,
        showgrid=False,
        mirror=True,
        nticks=15,
    )

    fig.update_yaxes(
        ticks="outside",
        ticklen=10,
        tickcolor="#EEEEEE",
        linecolor="#EEEEEE",
        linewidth=2,
        showgrid=False,
        showline=True,
        mirror=True,
    )

    fig.update_layout(
        colorway=["#827717", "#1565c0", "#ab47bc"],
        showlegend=True,
        legend_orientation="h",
        legend_traceorder="normal",
        legend_x=0.5,
        legend_xanchor="center",
        legend_y=1.04,
        legend_yanchor="top",
        barmode="stack",
        width=1920,
        height=2400,
        font_family="Inter",
        font_color="#FFFFFF",
        font_size=24,
        title_text=f"Composición del <b>Valor Agregado Bruto</b> de México por entidad durante {año}",
        title_x=0.5,
        title_y=0.98,
        margin_t=190,
        margin_r=40,
        margin_b=120,
        margin_l=280,
        title_font_size=36,
        paper_bgcolor=PAPER_COLOR,
        plot_bgcolor=PLOT_COLOR,
        annotations=[
            dict(
                x=0.01,
                y=-0.045,
                xref="paper",
                yref="paper",
                xanchor="left",
                yanchor="top",
                text=f"Fuente: INEGI ({FECHA_FUENTE})",
            ),
            dict(
                x=0.57,
                y=-0.045,
                xref="paper",
                yref="paper",
                xanchor="center",
                yanchor="top",
                text="Proporción dentro del VAB estatal",
            ),
            dict(
                x=1.01,
                y=-0.045,
                xref="paper",
                yref="paper",
                xanchor="right",
                yanchor="top",
                text="🧁 @lapanquecita",
            ),
        ],
    )

    fig.write_image(f"./vab_estatal_{año}.png")


def crecimiento_anual(entidad, clave_industria):
    """
    Creates a lollipop chart (bar chart) showing the evolution
     of the value of the given industry identifier.

    Parameters
    ----------
    industry_id : str
        The identifier of the industry.
        Check the CSV file for all available ones.

    """

    # Cargamos el dataset de PIB por industiria.
    df = pd.read_csv("./pib_estatal.csv", index_col=0)

    # Filtramos por la clave de la inudstria.
    df = df[df["CLAVE_INDUSTRIA"] == clave_industria]

    df = df[df["ENTIDAD"] == entidad]

    # Extraemos el nombre de la industria.
    nombre_industria = df["INDUSTRIA"].unique()[0].lower()

    # Calculmaos el cambio porcentual.
    df["cambio"] = df["VALOR_CONSTANTE"].pct_change()

    # Las barras serán naranjas cuando el valor se haya reducido
    # y serán azules cuando haya crecido.
    df["color_barra"] = df["cambio"].apply(
        lambda x: "hsl(30, 100%, 55%)" if x < 0 else "hsl(186, 100%, 55%)"
    )

    # Misma lógica para los círculos.
    df["color_circulo"] = df["cambio"].apply(
        lambda x: "hsl(30, 100%, 20%)" if x < 0 else "hsl(186, 100%, 20%)"
    )

    # Solo mostraremos los últimos 15 años.
    df = df.tail(20)

    # La escala vertical cambiará dependiendo
    # de la magnitud del valor máximo.
    valor_max = df["VALOR_CONSTANTE"].max()

    # Billón de pesos.
    if valor_max >= 1000000000000000:
        y_title = "Billones de pesos a precios constantes de 2018 (anualizados)"
        df["VALOR_CONSTANTE"] /= 1000000
        text_template = "%{text:,.2f}"
    else:
        y_title = "Millones de pesos a precios constantes de 2018"
        df["VALOR_TEXTO"] = df["VALOR_CONSTANTE"] / 1000
        text_template = "%{text:,.0f}k"

    fig = go.Figure()

    # The bar chart has a modified width to make them thin.
    fig.add_trace(
        go.Bar(
            x=df.index,
            y=df["VALOR_CONSTANTE"],
            marker_color=df["color_barra"],
            width=0.06,
            marker_line_width=0,
        )
    )

    fig.add_trace(
        go.Scatter(
            x=df.index,
            y=df["VALOR_CONSTANTE"],
            mode="markers",
            marker_color=df["color_circulo"],
            marker_line_color=df["color_barra"],
            marker_line_width=4,
            marker_size=75,
        )
    )

    # The text has a little padding to make it centered inside the 'candy'.
    fig.add_trace(
        go.Scatter(
            x=df.index,
            y=df["VALOR_CONSTANTE"] * 0.991,
            text=df["VALOR_TEXTO"],
            texttemplate=text_template,
            mode="text",
            textposition="middle center",
            textfont_family="Oswald",
            textfont_size=30,
        )
    )

    # The range of the x-axis is modified to remove extra padding on the edges.
    fig.update_xaxes(
        range=[df.index.min() - 0.6, df.index.max() + 0.6],
        ticks="outside",
        ticklen=10,
        zeroline=False,
        tickcolor="#FFFFFF",
        linewidth=2,
        showline=True,
        showgrid=False,
        mirror=True,
        nticks=len(df) + 1,
    )

    # We adjust the range of the y-axis so the lollipop can have enough space.
    fig.update_yaxes(
        range=[0, df["VALOR_CONSTANTE"].max() * 1.08],
        title=y_title,
        ticks="outside",
        separatethousands=True,
        ticklen=10,
        title_standoff=15,
        tickcolor="#FFFFFF",
        linewidth=2,
        showgrid=True,
        gridwidth=0.5,
        showline=True,
        nticks=15,
        mirror=True,
    )

    fig.update_layout(
        showlegend=False,
        width=1920,
        height=1080,
        font_family="Inter",
        font_color="#FFFFFF",
        font_size=24,
        title_text=f"Evolución del <b>PIB real</b> de <b>{entidad}</b> ({df.index.min()}-{df.index.max()})",
        title_x=0.5,
        title_y=0.965,
        margin_t=80,
        margin_l=140,
        margin_r=40,
        margin_b=120,
        title_font_size=36,
        plot_bgcolor=PLOT_COLOR,
        paper_bgcolor=PAPER_COLOR,
        annotations=[
            dict(
                x=0.01,
                y=-0.11,
                xref="paper",
                yref="paper",
                xanchor="left",
                yanchor="top",
                text=f"Fuente: INEGI ({FECHA_FUENTE})",
            ),
            dict(
                x=0.5,
                y=-0.11,
                xref="paper",
                yref="paper",
                xanchor="center",
                yanchor="top",
                text="Año de registro",
            ),
            dict(
                x=0.5,
                y=0.1,
                xref="paper",
                yref="paper",
                xanchor="center",
                yanchor="top",
                bgcolor="#0F0F0F",
                align="left",
                bordercolor="#FFFFFF",
                borderwidth=2,
                borderpad=10,
                text="<span style='font-size:16px; color:hsl(186, 100%, 55%)'>⬤</span>  Incremento respecto al periodo anterior<br><span style='font-size:14px; color:hsl(30, 100%, 55%)'>⬤</span>  Reducción respecto al periodo anterior",
            ),
            dict(
                x=1.01,
                y=-0.11,
                xref="paper",
                yref="paper",
                xanchor="right",
                yanchor="top",
                text="🧁 @lapanquecita",
            ),
        ],
    )

    # Nombramos la imagen con la clave de la industria.
    fig.write_image(f"./evolucion_{entidad}_{clave_industria}.png")


def evolucion_anual(clave_industria, entidad):
    """
    Genera una gráfica tipo lollipop con la evolución
    del valor de una industria en una entidad federativa.

    Parameters
    ----------
    clave_industria : str
        El identificador de la industria.
        Basado en el SCIAN.

    entidad : str
        El nombre de la entidad federativa como aparece en el dataset.

    """

    # Cargamos el dataset de PIB estatal.
    df = pd.read_csv("./pib_estatal.csv", index_col=0)

    # Filtramos por la entidad especificada.
    df = df[df["ENTIDAD"] == entidad]

    # Filtramos por la clave de la industria.
    df = df[df["CLAVE_INDUSTRIA"] == clave_industria]

    # Extraemos el nombre de la industria.
    nombre_industria = df["INDUSTRIA"].unique()[0].lower()

    # Calculmaos el cambio porcentual.
    df["cambio"] = df["VALOR_CONSTANTE"].pct_change()

    # El color de cada lollipop dependerá del cambio
    # porcentual de ese año. Azul será crecimiento y
    # naranja será reducción.
    df["color_barra"] = df["cambio"].apply(
        lambda x: "hsl(30, 100%, 55%)" if x < 0 else "hsl(186, 100%, 55%)"
    )

    df["color_circulo"] = df["cambio"].apply(
        lambda x: "hsl(30, 100%, 20%)" if x < 0 else "hsl(186, 100%, 20%)"
    )

    # Solo mostraremos los 20 años más recientes.
    df = df.tail(20)

    # La escala vertical cambiará dependiendo
    # de la magnitud del valor máximo.
    valor_max = df["VALOR_CONSTANTE"].max()

    # Decenas de billón de pesos.
    if valor_max >= 10000000:
        y_title = "Billones de pesos a precios constantes de 2018 (anualizados)"
        df["VALOR_CONSTANTE"] /= 1000000
        df["texto"] = df.apply(lambda x: f"{x['VALOR_CONSTANTE']:,.2f}", axis=1)
    # Billones de pesos.
    elif valor_max >= 1000000:
        y_title = "Billones de pesos a precios constantes de 2018 (anualizados)"
        df["VALOR_CONSTANTE"] /= 1000000
        df["texto"] = df.apply(lambda x: f"{x['VALOR_CONSTANTE']:,.3f}", axis=1)
    # Cientos y miles de millones.
    else:
        y_title = "Millones de pesos a precios constantes de 2018"
        df["texto"] = df.apply(lambda x: f"{x['VALOR_CONSTANTE'] / 1000:,.0f}k", axis=1)

    # Para el título de la gráfica hay un caso especial para el PIB.
    if clave_industria == "PIB":
        titulo = f"Evolución del valor del <b>Producto Interno Bruto</b> en <b>{entidad}</b> ({df.index.min()}-{df.index.max()})"
    else:
        titulo = f"Evolución del valor agregado bruto de <b>{nombre_industria}</b> en <b>{entidad}</b> ({df.index.min()}-{df.index.max()})"

    # Cada lollipop tendrá 3 elementos.
    # Barra, circulo y texto.
    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            x=df.index,
            y=df["VALOR_CONSTANTE"],
            marker_color=df["color_barra"],
            width=0.06,
            marker_line_width=0,
        )
    )

    fig.add_trace(
        go.Scatter(
            x=df.index,
            y=df["VALOR_CONSTANTE"],
            mode="markers",
            marker_color=df["color_circulo"],
            marker_line_color=df["color_barra"],
            marker_line_width=4,
            marker_size=75,
        )
    )

    fig.add_trace(
        go.Scatter(
            x=df.index,
            y=df["VALOR_CONSTANTE"] * 0.991,
            text=df["texto"],
            mode="text",
            textposition="middle center",
            textfont_family="Oswald",
            textfont_size=30,
        )
    )

    fig.update_xaxes(
        range=[df.index.min() - 0.6, df.index.max() + 0.6],
        ticks="outside",
        ticklen=10,
        zeroline=False,
        tickcolor="#EEEEEE",
        linecolor="#EEEEEE",
        linewidth=2,
        showline=True,
        showgrid=False,
        mirror=True,
        nticks=len(df) + 1,
    )

    fig.update_yaxes(
        range=[0, df["VALOR_CONSTANTE"].max() * 1.08],
        title=y_title,
        ticks="outside",
        separatethousands=True,
        ticklen=10,
        title_standoff=15,
        tickcolor="#EEEEEE",
        linecolor="#EEEEEE",
        linewidth=2,
        showgrid=True,
        gridwidth=0.5,
        showline=True,
        nticks=15,
        mirror=True,
    )

    fig.update_layout(
        showlegend=False,
        width=1920,
        height=1080,
        font_family="Inter",
        font_color="#FFFFFF",
        font_size=24,
        title_text=titulo,
        title_x=0.5,
        title_y=0.965,
        margin_t=80,
        margin_l=140,
        margin_r=40,
        margin_b=120,
        title_font_size=36,
        plot_bgcolor=PLOT_COLOR,
        paper_bgcolor=PAPER_COLOR,
        annotations=[
            dict(
                x=0.01,
                y=-0.11,
                xref="paper",
                yref="paper",
                xanchor="left",
                yanchor="top",
                text=f"Fuente: INEGI ({FECHA_FUENTE})",
            ),
            dict(
                x=0.5,
                y=-0.11,
                xref="paper",
                yref="paper",
                xanchor="center",
                yanchor="top",
                text="Año",
            ),
            dict(
                x=0.5,
                y=0.1,
                xref="paper",
                yref="paper",
                xanchor="center",
                yanchor="top",
                bgcolor="#0F0F0F",
                align="left",
                bordercolor="#EEEEEE",
                borderwidth=2,
                borderpad=10,
                text="<span style='font-size:16px; color:hsl(186, 100%, 55%)'>⬤</span>  Incremento respecto al periodo anterior<br><span style='font-size:14px; color:hsl(30, 100%, 55%)'>⬤</span>  Reducción respecto al periodo anterior",
            ),
            dict(
                x=1.01,
                y=-0.11,
                xref="paper",
                yref="paper",
                xanchor="right",
                yanchor="top",
                text="🧁 @lapanquecita",
            ),
        ],
    )

    # Nombramos la imagen con la clave de la industria.
    fig.write_image(f"./evolucion_{clave_industria}_{entidad}.png")


if __name__ == "__main__":
    mapa_per_capita(2024, "PIB")
    mapa_per_capita(2024, "1")

    comparacion_interanual("PIB", 2023, 2024)
    composicion_vab(2024)

    evolucion_anual("PIB", "Campeche")
