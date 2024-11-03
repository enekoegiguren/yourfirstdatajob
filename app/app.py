import streamlit as st


# ---- PAGE SETUP ------

## HOME PAGE

home_page = st.Page(
    page = 'pages/home.py',
    title = "Home",
    icon = ":material/home:"
)

analysis = st.Page(
    page = 'pages/analysis.py',
    title = "Analysis",
    icon = ":material/home:"
)

Data_stack = st.Page(
    page = 'pages/data_stack.py',
    title = "Data stack",
    icon = ":material/home:"
)

# ---- NAVIGATION SETUP
pg = st.navigation(
    {
        "Home": [home_page],
        "Insights": [analysis, Data_stack]
    }
)

pg.run()