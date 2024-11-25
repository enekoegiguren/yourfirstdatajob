import streamlit as st


# ---- PAGE SETUP ------

## HOME PAGE

home_page = st.Page(
    page = 'pages/home.py',
    title = "Home",
    icon = ":material/home:"
)

market_data = st.Page(
    page = 'pages/market_data.py',
    title = "Market data",
    icon = ":material/analytics:"
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

salary = st.Page(
    page = 'pages/salary_pred.py',
    title = "Salary prediction",
    icon = ":material/person:"
)

network = st.Page(
    page = 'pages/network.py',
    title = "Network",
    icon = ":material/on_device_training:"
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
        "Market trends & Education": [market_data,statistics, Data_stack, cloud],
        "Profile analysis": [personal, salary],
        "Network": [network],
        "Contact": [contact]
    }
)

pg.run()