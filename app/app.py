import streamlit as st


# ---- PAGE SETUP ------

## HOME PAGE

home_page = st.Page(
    page = 'pages/home.py',
    title = "Home",
    icon = ":material/home:"
)

statistics = st.Page(
    page = 'pages/analysis_statistics.py',
    title = "Statistics",
    icon = ":material/home:"
)

Data_stack = st.Page(
    page = 'pages/analysis_data_stack.py',
    title = "Data stack",
    icon = ":material/home:"
)


personal = st.Page(
    page = 'pages/personal.py',
    title = "Your profile analysis",
    icon = ":material/home:"
)

# ---- NAVIGATION SETUP
pg = st.navigation(
    {
        "": [home_page],
        "Analysis - Insights": [statistics, Data_stack],
        "You": [personal]
    }
)

pg.run()