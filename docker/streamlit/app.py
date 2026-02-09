import streamlit as st

# CONFIGURAÇÃO DA PÁGINA
st.set_page_config(
    page_title="Tibia Analytics",
    page_icon="⚔️",
    layout="wide"
)

# SIDEBAR GLOBAL
st.sidebar.title("⚙️ Controle")

if st.sidebar.button("🔄 Refresh dados"):
    st.cache_data.clear()
    st.rerun()

st.sidebar.markdown("---")
st.sidebar.markdown("📊 **Tibia Analytics**")
st.sidebar.markdown("Lakehouse • Iceberg • Trino")

# CONTEÚDO PRINCIPAL
st.title("⚔️ Tibia Analytics")
st.subheader("Lakehouse & Rankings Dashboard")

st.markdown(
    """
Este projeto é um **dashboard analítico** construído com **Streamlit**  
para explorar dados do **Tibia** utilizando uma arquitetura moderna:

- 🧊 **Source: Gold lakehouse layer**
- 🧊 **Apache Iceberg**
- 🦆 **Trino**
- ☁️ **MinIO (S3 compatível)**
- 📊 **Dashboards interativos**

Use o menu lateral para navegar entre os rankings.
"""
)


# NAVEGAÇÃO
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.metric("🏆 Experience", "Ranking Global")
    st.markdown("Ranking global de experiência por player")

with col2:
    st.metric("⚔️ Skills", "Ranking por Categoria")
    st.markdown("Rankings separados por skill")

with col3:
    st.metric("🌍 Worlds", "Resumo")
    st.markdown("Distribuição de players por mundo")

with col4:
    st.metric("🌍 Experience", "Estatistica")
    st.markdown("Progressão de experience de jogadores")

with col5:
    st.metric("🌍 Skills", "Estatistica")
    st.markdown("Progressão de Skills de jogadores por categoria")

st.markdown("---")

# FOOTER
st.caption(
    "Projeto de dados Criado por Renato Mariano • Engenharia de Dados"
)
