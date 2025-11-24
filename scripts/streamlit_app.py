import json
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st


def load_json(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


REPORTS_DIR = Path(__file__).resolve().parents[1] / "reports"


def main():
    st.set_page_config(page_title="Supermercado :P", layout="wide")

    st.title("Transacciones")

    # ======================================================================
    # Cargar archivo único generado por el DAG de EDA
    # ======================================================================
    eda_path = REPORTS_DIR / "transacciones_eda.json"
    eda = load_json(eda_path)

    revision = eda.get("revision_inicial", {})
    stats = eda.get("estadisticas", {})

    global_info = stats
    dist = stats.get("estadisticas_categoricas", {})
    top_categorias_info = stats.get("top_categorias_por_ventas", {})

    # ======================================================================
    # (Opcional) Cargar modelos avanzados para la segunda pestaña
    # ======================================================================
    modelos = {}
    modelos_path = REPORTS_DIR / "transacciones_modelos_avanzados.json"
    if modelos_path.exists():
        modelos = load_json(modelos_path)
    seg = modelos.get("segmentacion_clientes", {})

    # ======================================================================
    # Pestañas principales
    # ======================================================================
    tab_eda, tab_modelos = st.tabs(["EDA descriptiva", "Modelos avanzados"])

    # ======================================================================
    # TAB 1: EDA DESCRIPTIVA
    # ======================================================================
    with tab_eda:
        st.header("Resumen ejecutivo")

        # Indicadores principales
        col1, col2, col3 = st.columns([1, 1, 1])

        total_unidades_vendidas = (
            dist.get("top_10_productos_mas_vendidos", {})
            .get("total_productos_vendidos")
        )
        total_transacciones = stats.get("total_transacciones")
        total_clientes_unicos = (
            dist.get("customer_id", {}).get("clientes_unicos")
        )

        col1.metric(
            "Total unidades vendidas",
            f"{total_unidades_vendidas:,}"
            if total_unidades_vendidas is not None
            else "N/A",
        )
        col2.metric(
            "Número de transacciones",
            f"{total_transacciones:,}"
            if total_transacciones is not None
            else "N/A",
        )
        col3.metric(
            "Clientes únicos",
            f"{total_clientes_unicos:,}"
            if total_clientes_unicos is not None
            else "N/A",
        )

        # ------------------------------------------------------------------
        # Top 10 productos vs Top 10 clientes
        # ------------------------------------------------------------------
        c1, spacer, c2 = st.columns([1, 0.1, 1])

        # Top 10 productos
        with c1:
            st.subheader("Top 10 productos más comprados")
            top_products = (
                dist.get("top_10_productos_mas_vendidos", {}).get("top_10", [])
            )
            if top_products:
                df_prod = pd.DataFrame(top_products)
                df_prod["product_label"] = "P" + df_prod["product_id"].astype(str)

                fig_prod = px.bar(
                    df_prod.sort_values("frecuencia_absoluta"),
                    x="frecuencia_absoluta",
                    y="product_label",
                    orientation="h",
                    labels={
                        "frecuencia_absoluta": "Unidades vendidas",
                        "product_label": "Producto",
                    },
                    height=380,
                )
                st.plotly_chart(fig_prod, use_container_width=True)
            else:
                st.info("No hay datos de productos para mostrar.")

        # Top 10 clientes
        with c2:
            st.subheader("Top 10 clientes por número de compras")
            top_clients = (
                dist.get("top_10_clientes_mas_compras", {}).get("top_10", [])
            )
            if top_clients:
                df_cli = pd.DataFrame(top_clients)
                df_cli["customer_label"] = "C" + df_cli["customer_id"].astype(str)

                fig_cli = px.bar(
                    df_cli.sort_values("frecuencia_absoluta"),
                    x="frecuencia_absoluta",
                    y="customer_label",
                    orientation="h",
                    labels={
                        "frecuencia_absoluta": "Número de compras",
                        "customer_label": "Cliente",
                    },
                    height=380,
                )
                st.plotly_chart(fig_cli, use_container_width=True)
            else:
                st.info("No hay datos de clientes para mostrar.")

        # ------------------------------------------------------------------
        # Días pico de compra + Categorías más “rentables”
        # ------------------------------------------------------------------
        c1, spacer, c2 = st.columns([1, 0.1, 1])

        # Días pico de compra
        with c1:
            st.subheader("Días pico de compra (día de la semana)")
            dias = dist.get("distribucion_dia_semana", [])
            if dias:
                df_dias = pd.DataFrame(dias)
                order = [
                    "Lunes",
                    "Martes",
                    "Miércoles",
                    "Jueves",
                    "Viernes",
                    "Sábado",
                    "Domingo",
                ]
                df_dias["dia_semana"] = pd.Categorical(
                    df_dias["dia_semana"], categories=order, ordered=True
                )
                df_dias = df_dias.sort_values("dia_semana")

                fig_dias = px.bar(
                    df_dias,
                    x="dia_semana",
                    y="num_transacciones",
                    labels={
                        "dia_semana": "Día",
                        "num_transacciones": "Nº Transacciones",
                    },
                    height=380,
                )
                st.plotly_chart(fig_dias, use_container_width=True)
            else:
                st.info("No hay datos de distribución por día de la semana.")

        # Categorías más “rentables”
        with c2:
            st.subheader("Categorías más rentables (por volumen de ventas)")
            top_cat = top_categorias_info.get("top", [])
            if top_cat:
                df_cat = pd.DataFrame(top_cat)
                df_cat["category_name"] = df_cat["category_name"].fillna(
                    "Sin categoría"
                )

                chart_type = st.radio(
                    "Tipo de gráfico para categorías",
                    ["Barras", "Pastel"],
                    horizontal=True,
                    key="cat_chart_type",
                )

                if chart_type == "Barras":
                    fig_cat = px.bar(
                        df_cat.sort_values("unidades_vendidas"),
                        x="unidades_vendidas",
                        y="category_name",
                        orientation="h",
                        labels={
                            "unidades_vendidas": "Unidades vendidas",
                            "category_name": "Categoría",
                        },
                        height=380,
                    )
                    st.plotly_chart(fig_cat, use_container_width=True)
                else:
                    fig_pie = px.pie(
                        df_cat,
                        names="category_name",
                        values="unidades_vendidas",
                        hole=0.3,
                    )
                    st.plotly_chart(fig_pie, use_container_width=True)
            else:
                st.info("No hay datos de categorías para mostrar.")

        # ==================================================================
        # VISUALIZACIONES ANALÍTICAS
        # ==================================================================
        st.header("Visualizaciones analíticas")

        # Serie de tiempo + Boxplot
        c_ts, spacer, c_box = st.columns([1.2, 0.1, 1])

        # Serie de tiempo
        with c_ts:
            st.subheader("Serie de tiempo de ventas (nº de transacciones)")
            ts_gran = st.radio(
                "Granularidad",
                ["Diaria", "Semanal"],
                horizontal=True,
                key="gran_ts",
            )

            if ts_gran == "Diaria":
                diaria = dist.get("distribucion_temporal_diaria", [])
                if diaria:
                    df_diaria = pd.DataFrame(diaria)
                    df_diaria["date"] = pd.to_datetime(df_diaria["date"])
                    df_diaria = df_diaria.sort_values("date")

                    fig_ts = px.line(
                        df_diaria,
                        x="date",
                        y="num_transacciones",
                        markers=True,
                        labels={
                            "date": "Fecha",
                            "num_transacciones": "Nº Transacciones",
                        },
                        height=400,
                    )
                    st.plotly_chart(fig_ts, use_container_width=True)
                else:
                    st.info("No hay datos de serie diaria en el JSON.")
            else:
                semanal = dist.get("distribucion_temporal_semanal", [])
                if semanal:
                    df_sem = pd.DataFrame(semanal)
                    df_sem["periodo"] = (
                        df_sem["year"].astype(str)
                        + "-W"
                        + df_sem["week_of_year"].astype(str)
                    )

                    fig_ts_w = px.line(
                        df_sem.sort_values(["year", "week_of_year"]),
                        x="periodo",
                        y="num_transacciones",
                        markers=True,
                        labels={
                            "periodo": "Año-Semana",
                            "num_transacciones": "Nº Transacciones",
                        },
                        height=400,
                    )
                    fig_ts_w.update_xaxes(tickangle=45)
                    st.plotly_chart(fig_ts_w, use_container_width=True)
                else:
                    st.info("No hay datos de serie semanal en el JSON.")

        # Boxplot
        with c_box:
            st.subheader("Boxplot de distribución de totales")
            box_opt = st.radio(
                "Selecciona dimensión",
                ["Clientes (Top 10)", "Categorías (Top 10)"],
                horizontal=True,
                key="box_opt",
            )

            if box_opt.startswith("Clientes"):
                top_clients = (
                    dist.get("top_10_clientes_mas_compras", {}).get("top_10", [])
                )
                if top_clients:
                    df_cli = pd.DataFrame(top_clients)
                    fig_box_cli = px.box(
                        df_cli,
                        y="frecuencia_absoluta",
                        points="suspectedoutliers",
                        labels={
                            "frecuencia_absoluta": "Número de compras (Top 10)",
                        },
                        height=400,
                    )
                    st.plotly_chart(fig_box_cli, use_container_width=True)
                else:
                    st.info("No hay datos de clientes para boxplot.")
            else:
                top_cat = top_categorias_info.get("top", [])
                if top_cat:
                    df_cat = pd.DataFrame(top_cat)
                    fig_box_cat = px.box(
                        df_cat,
                        y="unidades_vendidas",
                        points="suspectedoutliers",
                        labels={
                            "unidades_vendidas": "Unidades vendidas por categoría (Top 10)",
                        },
                        height=400,
                    )
                    st.plotly_chart(fig_box_cat, use_container_width=True)
                else:
                    st.info("No hay datos de categorías para boxplot.")

        # Heatmap correlación
        st.subheader("Heatmap de correlación entre variables (Top clientes)")

        top_clients = dist.get("top_10_clientes_mas_compras", {}).get("top_10", [])
        if top_clients and len(top_clients) > 1:
            df_cli = pd.DataFrame(top_clients)
            numeric_cols = [
                c for c in df_cli.columns if pd.api.types.is_numeric_dtype(df_cli[c])
            ]
            if len(numeric_cols) >= 2:
                corr = df_cli[numeric_cols].corr()

                fig_heat = px.imshow(
                    corr,
                    text_auto=True,
                    aspect="auto",
                    labels={
                        "x": "Variable",
                        "y": "Variable",
                        "color": "Correlación",
                    },
                )
                st.plotly_chart(fig_heat, use_container_width=True)
            else:
                st.info(
                    "No hay suficientes variables numéricas para calcular una matriz de correlación."
                )
        else:
            st.info("No hay suficientes datos de clientes para el heatmap.")

    # ======================================================================
    # TAB 2: MODELOS AVANZADOS (SEGMENTACIÓN DE CLIENTES)
    # ======================================================================
    with tab_modelos:
        st.header("Modelos avanzados: Segmentación de clientes")

        if not seg:
            st.info(
                "No se encontró información de segmentación. "
                "Verifica que el DAG de modelos avanzados haya generado 'transacciones_modelos_avanzados.json'."
            )
            return

        clusters_resumen = seg.get("clusters_resumen", [])
        k = seg.get("k")
        num_clientes = seg.get("num_clientes")

        st.markdown(f"- **Número de clusters (k)**: `{k}`")
        st.markdown(f"- **Clientes segmentados**: `{num_clientes:,}`")

        if clusters_resumen:
            df_clusters = pd.DataFrame(
                [
                    {
                        "cluster": c["cluster"],
                        "num_clientes": c["num_clientes"],
                        "porcentaje_clientes": c["porcentaje_clientes"],
                        "frecuencia": c["promedios"]["frecuencia"],
                        "volumen_total": c["promedios"]["volumen_total"],
                        "diversidad_productos": c["promedios"]["diversidad_productos"],
                        "diversidad_categorias": c["promedios"]["diversidad_categorias"],
                    }
                    for c in clusters_resumen
                ]
            )

            # Nombres amigables (puedes ajustar)
            segment_names = {
                0: "Clientes muy ocasionales",
                1: "Súper heavy users",
                2: "Clientes regulares",
                3: "Frecuentes de alto valor",
            }
            df_clusters["segmento"] = df_clusters["cluster"].map(segment_names)

            c1, spacer, c2 = st.columns([1, 0.1, 1])

            # Distribución de clientes por segmento
            with c1:
                st.subheader("Distribución de clientes por segmento")
                fig_bar_seg = px.bar(
                    df_clusters,
                    x="segmento",
                    y="porcentaje_clientes",
                    text="porcentaje_clientes",
                    labels={
                        "segmento": "Segmento",
                        "porcentaje_clientes": "% de clientes",
                    },
                    height=380,
                )
                fig_bar_seg.update_traces(
                    texttemplate="%{text:.2f}%", textposition="outside"
                )
                fig_bar_seg.update_layout(yaxis_title="% de clientes")
                st.plotly_chart(fig_bar_seg, use_container_width=True)

            # Perfil promedio por segmento
            with c2:
                st.subheader("Perfil promedio por segmento")

                df_melt = df_clusters.melt(
                    id_vars=["segmento"],
                    value_vars=[
                        "frecuencia",
                        "volumen_total",
                        "diversidad_productos",
                        "diversidad_categorias",
                    ],
                    var_name="variable",
                    value_name="valor",
                )

                fig_profile = px.bar(
                    df_melt,
                    x="segmento",
                    y="valor",
                    color="variable",
                    barmode="group",
                    labels={
                        "segmento": "Segmento",
                        "valor": "Valor promedio",
                        "variable": "Métrica",
                    },
                    height=380,
                )
                st.plotly_chart(fig_profile, use_container_width=True)

            # Descripción cualitativa
            st.subheader("Descripción cualitativa de los segmentos")
            st.markdown(
                """
- **Segmento 0 – Clientes muy ocasionales**  
  Poca frecuencia de compra, tickets pequeños y baja diversidad de productos/categorías.

- **Segmento 2 – Clientes regulares**  
  Compran varias veces en el periodo, con volumen moderado y variedad interesante.

- **Segmento 3 – Frecuentes de alto valor**  
  Alta frecuencia y alto volumen, con muchos productos y categorías distintos. Son clientes clave.

- **Segmento 1 – Súper heavy users**  
  Máxima frecuencia y volumen, con la mayor diversidad. Representan el núcleo más valioso del negocio.
                """
            )

        else:
            st.info("No se encontraron clusters en el JSON de modelos avanzados.")


if __name__ == "__main__":
    main()
