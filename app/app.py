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
    icon = ":material/analytics:"
)

Data_stack = st.Page(
    page = 'pages/analysis_data_stack.py',
    title = "Data stack",
    icon = ":material/home_repair_service:"
)

cloud = st.Page(
    page = 'pages/cloud.py',
    title = "Cloud",
    icon = ":material/cloud:"
)



personal = st.Page(
    page = 'pages/personal.py',
    title = "Your profile analysis",
    icon = ":material/person:"
)

contact = st.Page(
    page = 'pages/contact.py',
    title = "Contact me",
    icon = ":material/on_device_training:"
)

# ---- NAVIGATION SETUP
pg = st.navigation(
    {
        "": [home_page],
        "Analysis - Insights": [statistics, Data_stack, cloud, personal],
        "Contact": [contact]
    }
)

pg.run()