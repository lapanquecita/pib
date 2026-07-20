"""

Este script es utilizado para visualizar el valor agregado bruto de las
industrias disponibles en los registros del PIB por actividad económica.

"""

import pandas as pd
import plotly.graph_objects as go


# La fecha de actualización más reciente.
FECHA_FUENTE = "mayo 2026"

# Definimos los colores que usaremos para las visualizaciones.
PLOT_COLOR = "#1C1F1A"
PAPER_COLOR = "#262B23"


def evolucion_anual(clave_industria):
    """
    Genera una gráfica tipo lollipop con la
    evolución del valor de una industria especificada.

    Parameters
    ----------
    clave_industria : str
        El identificador de la industria.
        Basado en el SCIAN.

    """

    # Cargamos el dataset de PIB por industiria.
    df = pd.read_csv("./pib_industrias.csv", parse_dates=["PERIODO"], index_col=0)

    # Filtramos por la clave de la industria.
    df = df[df["CLAVE_INDUSTRIA"] == clave_industria]

    # Extraemos el nombre de la industria.
    nombre_industria = df["INDUSTRIA"].unique()[0].lower()

    # Solo tomaremos los valores constantes.
    # Estos serán anualizados.
    df = df[["VALOR_CONSTANTE"]] * 4

    # Vamos a remuestrearlos con el promedio anual.
    # Cuando un año tiene registros para los 4 trimestres es equivalente a sumarlos.
    df = df.resample("YS").mean()

    # El índice será el año.
    df.index = df.index.year

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
        y_title = "Millones de pesos a precios constantes de 2018 (anualizados)"
        df["texto"] = df.apply(lambda x: f"{x['VALOR_CONSTANTE'] / 1000:,.1f}k", axis=1)

    # Para el título de la gráfica hay un caso especial para el PIB.
    if clave_industria == "PIB":
        titulo = f"Evolución del valor del <b>Producto Interno Bruto</b> en México ({df.index.min()}-{df.index.max()})"
    else:
        titulo = f"Evolución del valor agregado bruto de <b>{nombre_industria}</b> en México ({df.index.min()}-{df.index.max()})"

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
    fig.write_image(f"./evolucion_{clave_industria}.png")


def participacion(clave_industria):
    """
    Genera una gráfica mostrando la participación
    dentro del PIB de la industria especificada.

    Parameters
    ----------
    clave_industria : str
        El identificador de la industria.
        Basado en el SCIAN.

    """

    # Cargamos el dataset de PIB por industiria.
    df = pd.read_csv("./pib_industrias.csv", parse_dates=["PERIODO"], index_col=0)

    # Extraemos el PIB constante.
    pib = df[df["CLAVE_INDUSTRIA"] == "PIB"]["VALOR_CONSTANTE"]
    pib = pib.resample("YS").sum()

    # Filtramos por la clave de la inudstria.
    df = df[df["CLAVE_INDUSTRIA"] == clave_industria]

    # Extraemos el nombre de la industria.
    nombre_industria = df["INDUSTRIA"].unique()[0].lower()

    # Vamos a remuestrearlos con la suma anual.
    df = df.resample("YS").sum()

    # Agregamos el PIB al DataFrame de la industria.
    df["pib"] = pib

    # Calculmos le porcentaje del PIB.
    df["porcentaje"] = df["VALOR_CONSTANTE"] / df["pib"] * 100

    # El índice será el año.
    df.index = df.index.year

    # Solo mostraremos los 20 años más recientes.
    df = df.tail(20)

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=df.index,
            y=df["porcentaje"],
            text=df["porcentaje"],
            texttemplate="%{text:,.2f}",
            mode="markers+lines+text",
            fill="toself",
            marker_color="#ffca28",
            line_width=5,
            marker_size=24,
            textfont_family="Oswald",
            textfont_color="#FFFFFF",
            textposition="top center",
            line_shape="spline",
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
        showgrid=True,
        gridwidth=0.5,
        showline=True,
        mirror=True,
        nticks=len(df) + 1,
    )

    fig.update_yaxes(
        ticksuffix="%",
        title="Participación en el PIB real (%)",
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
        title_text=f"Participación de <b>{nombre_industria}</b> en el PIB de México  ({df.index.min()}-{df.index.max()})",
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
    fig.write_image(f"./participacion_{clave_industria}.png")


def comparar(*lista_industrias):
    """
    Genera una gráfica mostrando la evolución
    del valor agregado burto de múltiplesindustrias.

    Parameters
    ----------
    lista_industrias : str
        Los identificadores de las industrias.
        Basado en el SCIAN.

    """

    # Cargamos el dataset de PIB por industiria.
    df = pd.read_csv("./pib_industrias.csv", parse_dates=["PERIODO"])

    # Seleccionamos las industrias especificadas.
    df = df[df["CLAVE_INDUSTRIA"].isin(lista_industrias)]

    # Anualizamos las cifras.
    df["VALOR_CONSTANTE"] *= 4

    df = df.pivot_table(
        index=df["PERIODO"].dt.year,
        columns="INDUSTRIA",
        values="VALOR_CONSTANTE",
        aggfunc="mean",
    )

    # La escala vertical cambiará dependiendo
    # de la magnitud del valor máximo.
    valor_max = df.values.max()

    # Billones de pesos.
    if valor_max >= 1000000:
        y_title = "Billones de pesos a precios constantes de 2018 (anualizados)"
        df /= 1000000
    # Cientos y miles de millones.
    else:
        y_title = "Millones de pesos a precios constantes de 2018 (anualizados)"

    fig = go.Figure()

    # Crearemos una gráfica de l'línea con puntos para cada industria.
    for col in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df.index,
                y=df[col],
                mode="markers+lines",
                name=col,
                marker_size=18,
                line_width=4,
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
        gridwidth=0.5,
        mirror=True,
        nticks=25,
    )

    fig.update_yaxes(
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
        colorway=["#ffd740", "#18ffff", "#ff80ab"],
        legend_itemsizing="constant",
        legend_orientation="h",
        showlegend=True,
        legend_x=0.5,
        legend_y=1.06,
        legend_xanchor="center",
        legend_yanchor="top",
        width=1920,
        height=1080,
        font_family="Inter",
        font_color="#FFFFFF",
        font_size=24,
        title_text=f"Evolución del valor agregado bruto de industrias seleccionadas de México ({df.index.min()}-{df.index.max()})",
        title_x=0.5,
        title_y=0.965,
        margin_t=120,
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

    # Nombramos la imagen con las claves especificadas.
    fig.write_image(f"./comparacion_{'_'.join(lista_industrias)}.png")


if __name__ == "__main__":
    evolucion_anual("PIB")
    evolucion_anual("211")

    participacion("211")

    comparar("211", "3241", "3251")
