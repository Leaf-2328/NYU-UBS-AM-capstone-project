import streamlit as st
import pandas as pd
import os
from azure.ai.textanalytics import TextAnalyticsClient
from azure.core.credentials import AzureKeyCredential
from city_list import cities_list
from states_list import states_list
from utils1 import load_image_from_local


def convert_df(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv().encode('utf-8')


def add_summary(df, n):
    df = df.reset_index(drop=True)
    summary_ls = []
    summary_df = pd.read_excel('data/summary_complete.xlsx')
    summary_df.set_index('domain_name', inplace=True)
    for i in range(n):
        domain_name = df.loc[i, 'domain']
        if domain_name in summary_df.index:
            summary_txt = summary_df.loc[domain_name, 'Summary']
            # Clean up and filter summary
            if (len(summary_txt) < 5) or ('BBC' in summary_txt):
                summary_txt = ""
            summary_ls.append(summary_txt)
        else:
            summary_ls.append("")

    df['summary'] = summary_ls
    return df


# def additional_filter(df):
#     if state and state != []:
#         df = df[df.state.isin(state)]
#     if city and city != []:
#         df = df[df.city.isin(city)]
#     if employees_number and employees_number != []:
#         df = df[df['Numbers of current employees'] != 'None']
#         df['Numbers of current employees'] = df['Numbers of current employees'].apply(lambda x: int(x.split('.')[0]))
#         df_ls = []
#         if '0~100' in employees_number:
#             df_ls.append(df[(df['Numbers of current employees'] >= 0.0) &
#                             (df['Numbers of current employees'] < 100.0)])
#         if '100~200' in employees_number:
#             df_ls.append(df[(df['Numbers of current employees'] >= 100.0) &
#                             (df['Numbers of current employees'] < 200.0)])
#         if '200~300' in employees_number:
#             df_ls.append(df[(df['Numbers of current employees'] >= 200.0) &
#                             (df['Numbers of current employees'] < 300.0)])
#         if '300~400' in employees_number:
#             df_ls.append(df[(df['Numbers of current employees'] >= 300.0) &
#                             (df['Numbers of current employees'] < 400.0)])
#         if '400~500' in employees_number:
#             df_ls.append(df[(df['Numbers of current employees'] >= 400.0) &
#                             (df['Numbers of current employees'] <= 500.0)])
#         df = pd.concat(df_ls)
#     return df


# Set up page.
st.set_page_config(
        page_title="Personalized Investment Tool",
        page_icon="🐳",
        initial_sidebar_state="expanded"
    )

st.image(load_image_from_local("images/page_icon.png"), width=300)

# st.title("Personalized Investment Tool")
st.subheader("This app helps users easily discover potential target companies in fit investment criteria.\n")

search_option = st.radio('Search Method:', ('Industry Tag Search', 'Custom Search'))

if search_option == 'Custom Search':
    model_option = st.radio(
         'What model do you want to use?',
         ('NLP', 'Tfidf'))

    # Variables initialization
    text = None
    keyword = None
    number = None
    state = None
    city = None
    employees_number = None
    revenue = None
    df = pd.DataFrame()

    if model_option == 'NLP':
        text = st.text_input("Please enter key words or description of the company you want to search and submit")
        number = st.slider('How many top companies do you want to display?', 0, 10, 5)
    else:
        keyword = st.select_slider(
            'Select a keyword of a company: ',
            options=['software', 'cloud', 'cloudbased', 'machinelearning', 'platform', 'technology',
                     'digital', 'saas', 'product'])
        number = st.slider('How many top companies do you want to display?', 0, 30, 15)

    additional_filters = st.checkbox('Additional filters')

    if additional_filters:
        state = st.multiselect('State:', [x for x in states_list])
        city = st.multiselect('City:', [x for x in cities_list])
        employees_number = st.multiselect('Numbers of current employees:', ['0~100', '100~200', '200~300',
                                                                            '300~400', '400~500'])
        revenue = st.multiselect('Revenue:', ['Less than 1 million', '1 million to 10 million',
                                              '10 million to 100 million', 'Large than 100 million'])
        # Add other filters in the future (Employees number, Revenue ...)
    # # Authenticate the client using your key and endpoint
    # def authenticate_client():
    #     ta_credential = AzureKeyCredential(key)
    #     text_analytics_client = TextAnalyticsClient(
    #             endpoint=endpoint,
    #             credential=ta_credential)
    #     return text_analytics_client
    #
    #
    # def key_phrase_extraction_example(client, documents_):
    #     result = []
    #
    #     try:
    #         documents = [documents_]
    #
    #         response = client.extract_key_phrases(documents=documents)[0]
    #
    #         if not response.is_error:
    #             #             print("\tKey Phrases:")
    #             for phrase in response.key_phrases:
    #                 #                 print("\t\t", phrase)
    #                 result.append(phrase)
    #         else:
    #             print(response.id, response.error)
    #
    #     except Exception as err:
    #         print("Encountered exception. {}".format(err))
    #
    #     return result

    if st.button('Submit'):
        # Call (1) function -- get ranked & filtered dataframe （Algorithm Search Team + Predictive Model Team)

        # NLP model or Tfidf model
        if model_option == 'NLP':
            # subprocess.call(['sh', './build.sh'])  # Build whole flow (First time run and no workspace)
            os.system('sh query.sh ' + text)  # Query (Workspace exists)

            df = pd.read_parquet('app_input.parquet')
            # Merge with PM data
            PM_data = pd.read_parquet('data/Predictive_model_data.parquet')
            PM_data = PM_data.rename(columns={'domain_name': 'domain'})
            df = df.merge(PM_data, on='domain')
            # Additional filters
            if state and state != []:
                df = df[df.state.isin(state)]
            if city and city != []:
                df = df[df.city.isin(city)]
            if employees_number and employees_number != []:
                df = df[df['Numbers of current employees'] != 'None']
                df['Numbers of current employees'] = df['Numbers of current employees'].apply(
                    lambda x: int(x.split('.')[0]))
                df_ls = []
                if '0~100' in employees_number:
                    df_ls.append(df[(df['Numbers of current employees'] >= 0.0) &
                                    (df['Numbers of current employees'] < 100.0)])
                if '100~200' in employees_number:
                    df_ls.append(df[(df['Numbers of current employees'] >= 100.0) &
                                    (df['Numbers of current employees'] < 200.0)])
                if '200~300' in employees_number:
                    df_ls.append(df[(df['Numbers of current employees'] >= 200.0) &
                                    (df['Numbers of current employees'] < 300.0)])
                if '300~400' in employees_number:
                    df_ls.append(df[(df['Numbers of current employees'] >= 300.0) &
                                    (df['Numbers of current employees'] < 400.0)])
                if '400~500' in employees_number:
                    df_ls.append(df[(df['Numbers of current employees'] >= 400.0) &
                                    (df['Numbers of current employees'] <= 500.0)])
                df = pd.concat(df_ls)
            if revenue and revenue != []:
                df = df[df['Revenue'] != 'None']
                df['Revenue'] = df['Revenue'].apply(
                    lambda x: int(x.split('.')[0]))
                df_ls = []
                if 'Less than 1 million' in revenue:
                    df_ls.append(df[(df['Revenue'] >= 0) &
                                    (df['Revenue'] < 1000000)])
                if '1 million to 10 million' in revenue:
                    df_ls.append(df[(df['Revenue'] >= 1000000) &
                                    (df['Revenue'] < 10000000)])
                if '10 million to 100 million' in revenue:
                    df_ls.append(df[(df['Revenue'] >= 10000000) &
                                    (df['Revenue'] < 100000000)])
                if 'Large than 100 million' in revenue:
                    df_ls.append(df[(df['Numbers of current employees'] >= 100000000)])
                df = pd.concat(df_ls)
            # df = additional_filters(df, state, city, employees_number)

            df.sort_values(by='score', inplace=True)
            if number <= len(df):
                df = df.iloc[:number]
            df['score'] = (1 / df['score']) * 50
            # df = df.drop(columns='content')
            df = df[['domain', 'company_name', 'score', 'state', 'city',
                     'Numbers of current employees',
                     'Growth rate of employees number (one year)', 'Revenue',
                     'Growth rate of revenue (one year)', 'Operating income',
                     'Growth rate of operating income (one year)'
                     ]]
            df = df.reset_index(drop=True)

        else:
            df = pd.read_csv('data/total_merge.csv')
            # Additional filters
            if state and state != []:
                df = df[df.state.isin(state)]
            if city and city != []:
                df = df[df.city.isin(city)]
            if employees_number and employees_number != []:
                df = df[df['Numbers of current employees'] != 'None']
                df['Numbers of current employees'] = df['Numbers of current employees'].apply(lambda x: int(x.split('.')[0]))
                df_ls = []
                if '0~100' in employees_number:
                    df_ls.append(df[(df['Numbers of current employees'] >= 0.0) &
                                    (df['Numbers of current employees'] < 100.0)])
                if '100~200' in employees_number:
                    df_ls.append(df[(df['Numbers of current employees'] >= 100.0) &
                                    (df['Numbers of current employees'] < 200.0)])
                if '200~300' in employees_number:
                    df_ls.append(df[(df['Numbers of current employees'] >= 200.0) &
                                    (df['Numbers of current employees'] < 300.0)])
                if '300~400' in employees_number:
                    df_ls.append(df[(df['Numbers of current employees'] >= 300.0) &
                                    (df['Numbers of current employees'] < 400.0)])
                if '400~500' in employees_number:
                    df_ls.append(df[(df['Numbers of current employees'] >= 400.0) &
                                    (df['Numbers of current employees'] <= 500.0)])
                df = pd.concat(df_ls)
            if revenue and revenue != []:
                df = df[df['Revenue'] != 'None']
                df['Revenue'] = df['Revenue'].apply(
                    lambda x: int(x.split('.')[0]))
                df_ls = []
                if 'Less than 1 million' in revenue:
                    df_ls.append(df[(df['Revenue'] >= 0) &
                                    (df['Revenue'] < 1000000)])
                if '1 million to 10 million' in revenue:
                    df_ls.append(df[(df['Revenue'] >= 1000000) &
                                    (df['Revenue'] < 10000000)])
                if '10 million to 100 million' in revenue:
                    df_ls.append(df[(df['Revenue'] >= 10000000) &
                                    (df['Revenue'] < 100000000)])
                if 'Large than 100 million' in revenue:
                    df_ls.append(df[(df['Revenue'] >= 100000000)])
                df = pd.concat(df_ls)
            # df = additional_filters(df)

            df = df[['domain_name', 'company_name', keyword, 'state', 'city',
                     'Numbers of current employees',
                     'Growth rate of employees number (one year)', 'Revenue',
                     'Growth rate of revenue (one year)', 'Operating income',
                     'Growth rate of operating income (one year)'
                     ]]
            df = df.rename(columns={keyword: 'score', 'domain_name': 'domain'})
            df = df.sort_values(by="score", ascending=False)
            if number <= len(df):
                df = df.iloc[:number]

        # Call (2) function -- extract key words （Cloud Team)
        # key = "9d85105ce0494d97a9ed554d9c5c6bd4"
        # endpoint = "https://eastus.api.cognitive.microsoft.com/"
        # client = authenticate_client()
        #
        # key_words_ls = []
        # for i in range(number):
        #     keywords = keywords_output_func(i)
        #     key_words_ls.append(keywords)
        #
        # df['Key Phrases'] = key_words_ls

        # data_keywords_add = df.copy()

        # Call (3) function -- summary (Cloud Team)
        # summary_ls = []
        # summary_df = pd.read_excel('data/summary_complete.xlsx')
        # summary_df.set_index('domain_name', inplace=True)
        # for i in range(number):
        #     domain_name = data_keywords_add.loc[i, 'domain']
        #     if domain_name in summary_df.index:
        #         summary_txt = summary_df.loc[domain_name, 'Summary']
        #         # Clean up and filter summary
        #         if (len(summary_txt) < 5) or ('BBC' in summary_txt):
        #             summary_txt = ""
        #         summary_ls.append(summary_txt)
        #     else:
        #         summary_ls.append("")
        #
        # data_keywords_add['summary'] = summary_ls
        # data_keywords_summary_added = data_keywords_add.copy()

        final_df = add_summary(df, len(df))

        # Display the DataFrame
        st.write(final_df)

        # Export dataframe option
        csv = convert_df(final_df)

        st.download_button(
            label="Download data as CSV",
            data=csv,
            file_name='app_result.csv'
        )

    else:
        st.write('Please click Submit after entering input or changing input')
else:
    df_tag = pd.read_csv('data/total_merge.csv')
    final_df = None
    tag_option = st.selectbox("Choose industry tag:", ('enterprise_software', 'data_vendor', 'fintech'))
    number = st.number_input('How many top companies do you want to display?', value=30)
    if st.button('Submit'):
        df = df_tag[df_tag[tag_option] == '1.0']
        df = df.sort_values(by="{}_score".format(tag_option), ascending=False)
        df = df.rename(columns={'domain_name': 'domain'})
        if len(df) < number:
            final_df = df
        else:
            final_df = df.iloc[:number]

        final_df = final_df[['domain', 'company_name', '{}_score'.format(tag_option),
                             'state', 'city', 'Industry',
                             'Numbers of current employees',
                             'Growth rate of employees number (one year)', 'Revenue',
                             'Growth rate of revenue (one year)', 'Operating income',
                             'Growth rate of operating income (one year)'
                             ]]

        final_df = add_summary(final_df, len(final_df))
        st.write(final_df)

        # Export dataframe option
        csv = convert_df(final_df)

        st.download_button(
            label="Download data as CSV",
            data=csv,
            file_name='app_result.csv'
        )
    else:
        st.write('Please click Submit after entering input or changing input')
