# app/dashboard.py
import streamlit as st

st.set_page_config(
    page_title="Reddit Sentiment Analysis",
    page_icon="📊",
    layout="wide"
)

st.title("📊 Reddit Sentiment Dashboard")
st.markdown("""
Benvenuto nella dashboard per l'analisi del sentiment su Reddit.  
Naviga nelle pagine a sinistra per:
- 🔍 **Overview**: riepilogo generale dei dati
- 📈 **Trends**: andamento temporale del sentiment
- 🗂 **Dataset**: esplora i post e i commenti
- ⚙️ **Tools**: fetch dati, analisi manuale di testo
""")

# aggiungi opzione per scegliere alcuni temi di interesse e fare preload dei dati (consiglia sulla base di trend gia scaricati nel db)
st.sidebar.header("Seleziona Temi di Interesse")
topics = st.sidebar.multiselect(
    "Scegli i temi di tuo interesse:",
    options=["Tecnologia", "Sport", "Politica", "Intrattenimento"],
    default=["Politica"]
)
