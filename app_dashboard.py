import json
import time
import uuid
import pandas as pd
import streamlit as st
from kafka import KafkaConsumer

st.set_page_config(
    page_title="Amiibo Rarity & Market Tracker",
    page_icon="🎮",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Inyección CSS: Estructura visual simétrica
st.markdown(
    """
    <style>
    .amiibo-card {
        border-radius: 10px;
        padding: 12px;
        background-color: #1e212b;
        margin-bottom: 6px;
        border: 1px solid #303646;
        min-height: 335px;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
    }
    .card-header-bar {
        display: flex;
        justify-content: space-between;
        align-items: center;
        width: 100%;
        margin-bottom: 2px;
    }
    .star-badge {
        font-size: 1.15rem;
        line-height: 1;
    }
    .rank-badge {
        background: rgba(255, 255, 255, 0.08);
        border: 1px solid rgba(255, 255, 255, 0.15);
        color: #f1f2f6;
        font-weight: 800;
        font-size: 0.82rem;
        padding: 2px 7px;
        border-radius: 6px;
    }
    .img-container {
        height: 130px;
        display: flex;
        align-items: center;
        justify-content: center;
        margin: 4px 0;
    }
    .img-container img {
        max-height: 120px;
        max-width: 100%;
        object-fit: contain;
    }
    </style>
""",
    unsafe_allow_html=True,
)

st.title("🎮 Amiibo Real-Time Rarity & Market Tracker")
st.caption(
    "Pipeline de streaming en tiempo real con Apache Kafka | Rastreador de Escasez y Cotizaciones de Mercado"
)


@st.cache_resource
def get_kafka_consumer():
    dynamic_group_id = f"amiibo-ui-{uuid.uuid4().hex[:6]}"
    return KafkaConsumer(
        "amiibo.market.raw",
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=dynamic_group_id,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        consumer_timeout_ms=500,
    )


consumer = get_kafka_consumer()

# Inicialización de estado en memoria
if "amiibo_data" not in st.session_state:
    st.session_state.amiibo_data = {}

# Sincronización de Favoritos por URL (Sin login, persistente por navegador)
if "favorites" not in st.session_state:
    fav_param = st.query_params.get("favs", "")
    st.session_state.favorites = set(
        [f.strip() for f in fav_param.split(",") if f.strip()]
    )


def update_favorite_url():
    st.query_params["favs"] = ",".join(st.session_state.favorites)


def calculate_scarcity_metrics(market_data, retail_price=15.99):
    loose_price = market_data.get("loose_price_usd", retail_price)
    active_listings = market_data.get("active_listings_count", 20)

    price_ratio = loose_price / max(retail_price, 1.0)
    score_price = min(price_ratio * 10, 60)
    score_scarcity = max(5.0, (50 - active_listings) * 0.8)
    internal_score = min(100.0, score_price + score_scarcity)

    find_chance = round(max(0.1, 70.0 - (internal_score * 0.69)), 2)

    if find_chance <= 2.0:
        tier = "🏆 Santo Grial"
        tier_color = "#ff4b4b"
    elif find_chance <= 10.0:
        tier = "✨ Muy Raro"
        tier_color = "#ff793f"
    elif find_chance <= 25.0:
        tier = "🔥 Raro"
        tier_color = "#ffa421"
    elif find_chance <= 45.0:
        tier = "📦 Poco Común"
        tier_color = "#2ed573"
    else:
        tier = "⚪ Común"
        tier_color = "#70a1ff"

    return find_chance, internal_score, tier, tier_color


# --- Sidebar: Filtros y Opciones ---
st.sidebar.header("🔍 Controles & Filtros")
auto_refresh = st.sidebar.toggle("Auto-Refresh en Vivo", value=True)

search_query = st.sidebar.text_input(
    "Buscar por nombre:", placeholder="Ej: Mario, Solaire, Qbby..."
)

sort_option = st.sidebar.selectbox(
    "Ordenar figuras por:",
    options=[
        "Mayor Rareza (Menor Probabilidad)",
        "Menor Rareza (Mayor Probabilidad)",
        "Precio Loose (Mayor a Menor)",
        "Precio Loose (Menor a Mayor)",
        "Nombre (A-Z)",
    ],
    index=0,
)

view_mode = st.sidebar.radio(
    "Formato de visualización:",
    options=["Tarjetas Visuales", "Tabla de Datos (Sin Tarjetas)"],
    index=0,
)

# Ingesta continua desde Kafka
raw_messages = consumer.poll(timeout_ms=200, max_records=200)
for topic_partition, messages in raw_messages.items():
    for msg in messages:
        payload = msg.value
        amiibo_id = payload.get("amiibo_id")
        metadata = payload.get("metadata", {})
        market = payload.get("market_data", {})

        find_chance, internal_score, tier, tier_color = calculate_scarcity_metrics(
            market, metadata.get("retail_price_usd", 15.99)
        )

        name = metadata.get("name", "Desconocido")
        st.session_state.amiibo_data[amiibo_id] = {
            "id": amiibo_id,
            "name": name,
            "game_series": metadata.get("game_series", "Nintendo"),
            "amiibo_series": metadata.get("amiibo_series", "General"),
            "image_url": metadata.get("image_url", ""),
            "loose_price": market.get("loose_price_usd", 0.0),
            "cib_price": market.get("cib_price_usd", 0.0),
            "sealed_price": market.get("sealed_price_usd", 0.0),
            "listings": market.get("active_listings_count", 0),
            "sales_24h": market.get("recent_sales_24h", 0),
            "find_chance": find_chance,
            "rarity_score": internal_score,
            "tier": tier,
            "tier_color": tier_color,
            "last_updated": payload.get("timestamp"),
        }

# --- Renderizado Principal ---
if st.session_state.amiibo_data:
    df = pd.DataFrame(st.session_state.amiibo_data.values())

    # 1. Filtro por nombre
    if search_query.strip():
        df_display = df[
            df["name"].str.contains(search_query.strip(), case=False, na=False)
        ]
    else:
        df_display = df

    # 2. Ordenamiento dinámico
    if sort_option == "Mayor Rareza (Menor Probabilidad)":
        df_display = df_display.sort_values(by="find_chance", ascending=True)
    elif sort_option == "Menor Rareza (Mayor Probabilidad)":
        df_display = df_display.sort_values(by="find_chance", ascending=False)
    elif sort_option == "Precio Loose (Mayor a Menor)":
        df_display = df_display.sort_values(by="loose_price", ascending=False)
    elif sort_option == "Precio Loose (Menor a Mayor)":
        df_display = df_display.sort_values(by="loose_price", ascending=True)
    elif sort_option == "Nombre (A-Z)":
        df_display = df_display.sort_values(by="name", ascending=True)

    # 3. KPIs Globales
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    kpi1.metric("Figuras Mostradas", len(df_display))
    most_expensive = (
        df_display.loc[df_display["loose_price"].idxmax()]
        if not df_display.empty
        else None
    )
    kpi2.metric(
        "Top Cotizado",
        f"{most_expensive['name']}" if most_expensive is not None else "N/A",
        f"${most_expensive['loose_price']:.2f}"
        if most_expensive is not None
        else "$0.00",
    )
    kpi3.metric(
        "Precio Promedio",
        f"${df_display['loose_price'].mean():.2f}"
        if not df_display.empty
        else "$0.00",
    )
    grail_count = (
        len(df_display[df_display["find_chance"] <= 2.0])
        if not df_display.empty
        else 0
    )
    kpi4.metric("Nivel Santo Grial (≤2% Hallazgo)", grail_count)

    st.divider()

    # 4. Pestañas: Catálogo vs Colección de Favoritos
    tab_catalog, tab_favs = st.tabs(
        [
            f"📦 Catálogo ({len(df_display)})",
            f"⭐ Mis Favoritos ({len(st.session_state.favorites)})",
        ]
    )

    def render_card_grid(dataset, is_favorites_tab=False):
        cols = st.columns(4)
        for idx, row in dataset.reset_index().iterrows():
            col = cols[idx % 4]
            with col:
                is_fav = row["id"] in st.session_state.favorites
                star_display = "⭐" if is_fav else "☆"

                img_html = (
                    f'<div class="img-container"><img src="{row["image_url"]}" alt="{row["name"]}"></div>'
                    if (
                        row["image_url"]
                        and row["image_url"] != "0"
                        and not str(row["image_url"]).endswith(
                            "icon_00000000-00000000.png"
                        )
                    )
                    else '<div class="img-container" style="font-size: 55px;">🎮</div>'
                )

                card_html = f"""
                <div class="amiibo-card">
                    <div class="card-header-bar">
                        <span class="star-badge">{star_display}</span>
                        <span class="rank-badge">#{idx + 1}</span>
                    </div>
                    {img_html}
                    <div>
                        <div style="font-size: 1.02rem; font-weight: bold; margin-bottom: 2px;">{row['name']}</div>
                        <div style="font-size: 0.8rem; color: #a4b0be; margin-bottom: 6px;">{row['game_series']} | {row['amiibo_series']}</div>
                        <div style="margin-bottom: 6px;">
                            <span style="font-weight: bold; color: {row['tier_color']};">{row['tier']}</span> 
                            <span style="font-size: 0.83rem; color: #ced6e0;">(Prob. <b>{row['find_chance']}%</b>)</span>
                        </div>
                        <div style="font-size: 0.88rem; margin-bottom: 4px;">
                            💵 <b>Loose:</b> ${row['loose_price']:.2f} | <b>Sealed:</b> ${row['sealed_price']:.2f}
                        </div>
                        <div style="font-size: 0.78rem; color: #747d8c;">
                            Listings: {row['listings']} | Ventas 24h: {row['sales_24h']}
                        </div>
                    </div>
                </div>
                """
                st.markdown(card_html, unsafe_allow_html=True)

                # Botón de alternancia de favorito
                btn_key = f"toggle_fav_{'favtab_' if is_favorites_tab else ''}{row['id']}"
                btn_label = "Quitar de Favoritos" if is_fav else "Guardar en Favoritos"
                if st.button(btn_label, key=btn_key, use_container_width=True):
                    if is_fav:
                        st.session_state.favorites.remove(row["id"])
                    else:
                        st.session_state.favorites.add(row["id"])
                    update_favorite_url()
                    st.rerun()

    # --- Pestaña Catálogo ---
    with tab_catalog:
        if df_display.empty:
            st.warning(f"No hay figuras que coincidan con '{search_query}'.")
        elif view_mode == "Tabla de Datos (Sin Tarjetas)":
            table_df = df_display[
                [
                    "name",
                    "game_series",
                    "amiibo_series",
                    "find_chance",
                    "tier",
                    "loose_price",
                    "cib_price",
                    "sealed_price",
                    "listings",
                    "sales_24h",
                ]
            ].copy()
            table_df.columns = [
                "Amiibo",
                "Juego",
                "Serie",
                "Prob. Hallazgo (%)",
                "Nivel",
                "Loose ($)",
                "CIB ($)",
                "Sealed ($)",
                "Listings",
                "Ventas 24h",
            ]
            st.dataframe(
                table_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Prob. Hallazgo (%)": st.column_config.NumberColumn(
                        format="%.1f%%"
                    ),
                    "Loose ($)": st.column_config.NumberColumn(format="$%.2f"),
                    "CIB ($)": st.column_config.NumberColumn(format="$%.2f"),
                    "Sealed ($)": st.column_config.NumberColumn(format="$%.2f"),
                },
            )
        else:
            render_card_grid(df_display, is_favorites_tab=False)

    # --- Pestaña Favoritos ---
    with tab_favs:
        if not st.session_state.favorites:
            st.info(
                "Aún no tienes figuras guardadas. Haz clic en 'Guardar en Favoritos' debajo de cualquier tarjeta del catálogo."
            )
        else:
            fav_df = df[df["id"].isin(st.session_state.favorites)]
            render_card_grid(fav_df, is_favorites_tab=True)

else:
    st.info(
        "⏳ Esperando eventos desde el broker de Kafka... Verifica que `amiibo_producer.py` esté activo."
    )

if auto_refresh:
    time.sleep(2)
    st.rerun()