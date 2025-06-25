import pandas as pd
import psycopg2
import streamlit as st
import time
import json
from kafka import KafkaConsumer
from streamlit_autorefresh import st_autorefresh
import plotly.express as px
import plotly.graph_objects as go

# @st.cache_data
def fetch_application_stats():
    conn = psycopg2.connect('host=postgres dbname=recruitment user=postgres password=secret')
    cur = conn.cursor()

    # Fetch total number of applications
    cur.execute('''
                SELECT COUNT(*) FROM application
                ''')
    total_applications = cur.fetchone()[0]

    # Fetch total number of candidates
    cur.execute('''
                SELECT COUNT(*) FROM candidate
                ''')
    total_candidates = cur.fetchone()[0]

    # Fetch total number of positions
    cur.execute('''
                SELECT COUNT(*) FROM position
                ''')
    total_positions = cur.fetchone()[0]

    return total_applications, total_candidates, total_positions

def create_kafka_consumer(topic_name):
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers='broker:29092',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    return consumer

def fetch_data_from_kafka(consumer):
    messages = consumer.poll(timeout_ms=1000)
    data = []

    for message in messages.values():
        for sub_message in message:
            data.append(sub_message.value)
    return data

def update_data():
    last_refresh = st.empty()
    last_refresh.text(f"Last refresh: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Fetch application statistics from postgres
    total_applications, total_candidates, total_positions = fetch_application_stats()

    #Display the statistics
    st.markdown('''---''')
    st.subheader('Overview')
    col1, col2, col3 = st.columns(3)
    col1.metric('Total Applications', total_applications, delta=f"{total_applications - st.session_state.get('last_total_applications', total_applications)}")
    col2.metric('Total Candidates', total_candidates, delta=f"{total_candidates - st.session_state.get('last_total_candidates', total_candidates)}")
    col3.metric('Total Positions', total_positions, delta=f"{total_positions - st.session_state.get('last_total_positions', total_positions)}")


    # Fetch applications per position from Kafka
    consumer = create_kafka_consumer('applications_per_position')
    data = fetch_data_from_kafka(consumer)

    results = pd.DataFrame(data)

    if not results.empty:
        st.markdown('''---''')
        st.subheader('Applications per Position')
        st.dataframe(results.iloc[:, 1:].tail(total_positions).reset_index(drop=True))
    else:
        st.markdown('''---''')
        st.warning('No data available for Applications per Position')

    col1, col2 = st.columns(2)
    with col1:

        # Fetch applications per score range from Kafka
        consumer = create_kafka_consumer('applications_per_score_range')
        data = fetch_data_from_kafka(consumer)

        results = pd.DataFrame(data)

        if not results.empty:
            st.markdown('''---''')
            st.subheader('Applications per Score Range')

            # Histogram using plotly
            fig = go.Figure()
            fig = px.histogram(results, x='score_range', y='application_count', title='Applications per Score Range')
            fig.update_layout(bargap=0.2)
            fig.update_traces(marker_color='indianred', marker_line_color='black', marker_line_width=1.5, opacity=0.6)
            st.plotly_chart(fig)

        else:
            st.markdown('''---''')
            st.warning('No data available for Applications per Score Range')

    with col2:
        # Fetch applications per experience years from Kafka
        consumer = create_kafka_consumer('applications_per_experience_years')
        data = fetch_data_from_kafka(consumer)

        results = pd.DataFrame(data)

        if not results.empty:
            st.markdown('''---''')
            st.subheader('Applications per Experience Years')

            # Pie chart using plotly
            fig = px.pie(results, names='experience_years_range', values='application_count', title='Applications per Experience Years')
            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig)

        else:
            st.markdown('''---''')
            st.warning('No data available for Applications per Experience Years')

    st.session_state['last_update'] = time.time()
    st.session_state['last_total_applications'] = total_applications
    st.session_state['last_total_candidates'] = total_candidates
    st.session_state['last_total_positions'] = total_positions

def sidebar():
    if st.session_state.get('last_update') is None:
        st.session_state['last_update'] = time.time()

    refresh_interval = st.sidebar.slider('Refresh Interval (seconds)', 1, 60, 10)
    st_autorefresh(interval=refresh_interval * 1000, key='auto')

    if st.sidebar.button('Refresh Data'):
        update_data()  

st.set_page_config(page_title='Recruitment Dashboard', layout='wide')
st.title('Recruitment Dashboard 📊')

sidebar()
update_data()