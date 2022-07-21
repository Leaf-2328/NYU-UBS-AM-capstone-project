import pyarrow.parquet as pq
import re, string
from sklearn.feature_extraction.text import TfidfVectorizer


def add_weight(times, target, dataframe):
    for i in range(1,times+1):
        dataframe['{}_{}'.format(target, str(i))] = dataframe[target]

    return dataframe


def integrate(df):
    df["content"] = df["content_search"]
    df = df.drop(["content_search"], axis=1)
    df['content'] = df[df.columns[1:17]].apply(lambda x: '. '.join(x.dropna().astype(str)), axis=1)
    df = df.drop(df.iloc[:, 1:17], axis=1)
    return df


def preprocess(text):
    text = text.lower() # lowercase text
    text = text.strip() # get rid of leading/trailing whitespace
    text = re.compile('<.*?>').sub('', text) # remove HTML tags/markups
    text = re.compile('[%s]' % re.escape(string.punctuation)).sub(' ', text) # replace punctuation with space
    text = re.sub('\s+', ' ', text) # remove extra space and tabs
    text = re.sub(r'\[[0-9]*\]',' ',text)  # [0-9] matches any digit (0 to 10000...)
    text = re.sub(r'[^\w\s]', '', str(text).lower().strip())
    text = re.sub(r'\d',' ',text) # matches any digit from 0 to 100000..., \D matches non-digits
    text = re.sub(r'\s+',' ',text) # \s matches any whitespace, \s+ matches multiple whitespace, \S matches non-whitespace
    return text


# search function
def find(arr, item):
    for i in range(0, len(arr)):
        if arr[i] == item:
            return i
    else:
        return -1


def tfidf_search(ddf, keyword):
    if find(name, keyword) == -1:
        print("No content contains this keyword!")
    else:
        ddf = ddf.copy()
        ddf[keyword] = X[:, find(name, keyword)].toarray()
        # ddf = ddf.sort_values(by="score" , ascending=False)
        # print(ddf.iloc[:num])
        return ddf


if __name__ == '__main__':
    df = pq.read_table(u'./data/WebsiteTxt_0_True_True_fintent_mongo_cb_202203_c9e93402ee-search.parquet').to_pandas()
    df = add_weight(10, 'website_title_short', df)
    df2 = integrate(df)
    df2['content'] = df2['content'].apply(lambda x: preprocess(x))

    # TfidfVectorizer
    vectorizer = TfidfVectorizer()
    X = vectorizer.fit_transform(df2['content'])
    name = vectorizer.get_feature_names_out()
    key_words = ['software', 'cloud', 'cloudbased', 'machinelearning', 'platform', 'technology',
                 'digital', 'saas', 'product']

    for key in key_words:
        df2 = tfidf_search(df2, key)

    df2.to_csv('tfidf.csv')

