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
    st.set_page_config(page_title="Dashboard", layout="wide")

    st.title("Análisis de Transacciones de Supermercado")

    # Load files
    trans_path = REPORTS_DIR / "transacciones_estadisticas_descriptivas.json"
    categorias_path = REPORTS_DIR / "top_categorias_por_ventas.json"

    trans = load_json(trans_path)
    categorias = load_json(categorias_path)

    global_info = trans.get("global", {})
    dist = global_info.get("estadisticas_categoricas", {})

    # Indicator row
    col1, col2, col3 = st.columns([1, 1, 2])

    total_unidades_vendidas = global_info.get("estadisticas_categoricas", {}).get("top_10_productos_mas_vendidos", {}).get("total_productos_vendidos")
    total_transacciones = global_info.get("total_transacciones")

    col1.metric("Total unidades vendidas", f"{total_unidades_vendidas:,}" if total_unidades_vendidas is not None else "N/A")
    col2.metric("Número de transacciones", f"{total_transacciones:,}" if total_transacciones is not None else "N/A")

    c1, spacer, c2 = st.columns([1, 0.2, 1]) 
    
    # Top 10 productos
    with c1:
        st.subheader("Productos más vendidos")
        top_products = global_info.get("estadisticas_categoricas", {}).get("top_10_productos_mas_vendidos", {}).get("top_10", [])
        if top_products:
            df_prod = pd.DataFrame(top_products)
            df_prod["product_label"] = "P" + df_prod["product_id"].astype(str)
            fig_prod = px.bar(df_prod.sort_values("frecuencia_absoluta"), x="frecuencia_absoluta", y="product_label", orientation="h", labels={"frecuencia_absoluta":"Unidades vendidas","product_label":"Product ID"}, height=400)
            st.plotly_chart(fig_prod, use_container_width=True)
        else:
            st.info("No hay datos de productos para mostrar.")

    # Por tienda
    with c2:
        st.subheader("**Ventas por tienda**")
        tiendas = dist.get("distribucion_por_tienda", [])
        if tiendas:
            df_store = pd.DataFrame(tiendas)
            df_store["store_label"] = "S" + df_store["store_id"].astype(str)
            fig_store = px.pie(df_store.sort_values("num_transacciones"), values="num_transacciones", names="store_label", labels={"num_transacciones":"Nº Transacciones","store_label":"Store ID"})
            # fig_pie = px.pie(df_cat, names="category_name", values="unidades_vendidas", hole=0.3)

            st.plotly_chart(fig_store, use_container_width=True)
        else:
            st.info("No hay datos por tienda.")

    c1, spacer, c2 = st.columns([1, 0.2, 1]) 

    # Categorías con más ventas
    with c2:
        st.subheader("Categorías más compradas")
        top_cat = categorias.get("top", [])
        if top_cat:
            df_cat = pd.DataFrame(top_cat)
            df_cat["category_name"] = df_cat["category_name"].fillna("Sin categoría")
            fig_pie = px.pie(df_cat, names="category_name", values="unidades_vendidas", hole=0.3)
            st.plotly_chart(fig_pie, use_container_width=True)    
        else:
            st.info("No hay datos de categorías para mostrar.")

    # Top 10 clientes
    with c1:
        st.subheader("Clientes con más compras")
        top_clients = global_info.get("estadisticas_categoricas", {}).get("top_10_clientes_mas_compras", {}).get("top_10", [])
        if top_clients:
            df_cli = pd.DataFrame(top_clients)
            df_cli["customer_label"] = "C" + df_cli["customer_id"].astype(str)
            fig_cli = px.bar(df_cli.sort_values("frecuencia_absoluta"), x="frecuencia_absoluta", y="customer_label", orientation="h", labels={"frecuencia_absoluta":"Número de compras","customer_label":"Customer ID"}, height=400)
            st.plotly_chart(fig_cli, use_container_width=True)
        else:
            st.info("No hay datos de clientes para mostrar.")

    # Time / distributions
    st.subheader("Distribuciones temporales y por categoría")

    c1, spacer, c2 = st.columns([1, 0.2, 1]) 

    # Día de la semana
    with c1:
        st.markdown("**Distribución por día de la semana**")
        dias = dist.get("distribucion_dia_semana", [])
        if dias:
            df_dias = pd.DataFrame(dias)
            # keep a logical order
            order = ["Lunes","Martes","Miércoles","Jueves","Viernes","Sábado","Domingo"]
            df_dias["dia_semana"] = pd.Categorical(df_dias["dia_semana"], categories=order, ordered=True)
            df_dias = df_dias.sort_values("dia_semana")
            fig_dias = px.bar(df_dias, x="dia_semana", y="num_transacciones", labels={"dia_semana":"Día","num_transacciones":"Nº Transacciones"})
            st.plotly_chart(fig_dias, use_container_width=True)
        else:
            st.info("No hay datos de día de semana.")

    
    # Mensual (serie)
    with c2:
        st.markdown("**Distribución temporal mensual**")
        mensual = dist.get("distribucion_temporal_mensual", [])
        if mensual:
            df_m = pd.DataFrame(mensual)
            # create a datetime for plotting
            df_m["date"] = pd.to_datetime(df_m["year"].astype(str) + "-" + df_m["month"].astype(str) + "-01")
            fig_m = px.line(df_m.sort_values("date"), x="date", y="num_transacciones", markers=True, labels={"date":"Fecha","num_transacciones":"Nº Transacciones"})
            st.plotly_chart(fig_m, use_container_width=True)
        else:
            st.info("No hay datos mensuales.")
   


if __name__ == "__main__":
    main()
