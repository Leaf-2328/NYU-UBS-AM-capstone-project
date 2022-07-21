__copyright__ = "Copyright (c) 2021 Jina AI Limited. All rights reserved."
__license__ = "Apache-2.0"

import os
import sys
import click
import random
import pandas as pd
from jina import Flow, Document, DocumentArray
from jina.logging.predefined import default_logger as logger

MAX_DOCS = int(os.environ.get('JINA_MAX_DOCS', 21000))


def config(dataset: str):
    # if dataset == 'toy':
    #     os.environ['JINA_DATA_FILE'] = os.environ.get('JINA_DATA_FILE', 'data/toy-input.txt')
    # elif dataset == 'full':
    #     os.environ['JINA_DATA_FILE'] = os.environ.get('JINA_DATA_FILE', 'data/input.txt')
    if dataset == 'full':
        # os.environ['JINA_DATA_FILE'] = os.environ.get('JINA_DATA_FILE', 'data/rawdata.txt')
        os.environ['JINA_DATA_FILE'] = os.environ.get('JINA_DATA_FILE', 'data/rawdata.txt')
    os.environ['JINA_PORT'] = os.environ.get('JINA_PORT', str(45678))
    cur_dir = os.path.dirname(os.path.abspath(__file__))
    os.environ.setdefault('JINA_WORKSPACE', os.path.join(cur_dir, 'workspace'))
    os.environ.setdefault('JINA_WORKSPACE_MOUNT',
                          f'{os.environ.get("JINA_WORKSPACE")}:/workspace/workspace')


def print_topk(resp, sentence):
    score_ls = []
    content = []
    domain = []
    df = pd.DataFrame(columns=['domain','score','content'])
    for doc in resp.data.docs:
        # print(f"\n\n\n Here's what we found for: {sentence}")
        for idx, match in enumerate(doc.matches):
            score = match.scores['cosine'].value
            # print(score)
            # print(match.text)
            # if str(match.text) != '\n':
            sp = str(match.text).split('\t', 1)
            if len(sp) == 2:
                score_ls.append(score)
                content.append(sp[1])
                domain.append(sp[0])
                # print(f'> {idx:>2d}({score:.2f}). {match.text}')

    df['domain'] = domain
    df['score'] = score_ls
    # df['content'] = content # test
    # df.sort_values(by='score',inplace=True)

    # df_top_50 = df.iloc[:50] # test
    # df_bottom_50 = df.iloc[-50:] # test
    # df = pd.concat([df_top_50,df_bottom_50]) # test
    df_total = df.copy()
    df_total.to_parquet('app_input.parquet', engine='pyarrow')

def input_generator(num_docs: int, file_path: str):
    with open(file_path) as file:
        lines = file.readlines()
    num_lines = len(lines)
    random.shuffle(lines)
    for i in range(min(num_docs, num_lines)):
        yield Document(text=lines[i], )


def index(num_docs):
    flow = Flow().load_config('flows/flow.yml')
    # flow = Flow().load_config('flows/flow_20000.yml')
    # data_path = os.path.join(os.path.dirname(__file__), os.environ.get('JINA_DATA_FILE', None))
    # data_path = 'data/rawdata.txt'
    data_path = 'data/rawdata.txt'
    with flow:
        flow.post(on='/index', inputs=input_generator(num_docs, data_path),
                  show_progress=True)


def query(top_k):
    flow = Flow().load_config('flows/flow.yml')
    # flow = Flow().load_config('flows/flow_20000.yml')
    with flow:
        text = input('Please type a sentence: ')
        doc = Document(content=text)

        result = flow.post(on='/search', inputs=DocumentArray([doc]),
                           parameters={'top_k': top_k},
                           line_format='text',
                           return_results=True,
                           )
        print_topk(result[0], text)


@click.command()
@click.option(
    '--task',
    '-t',
    type=click.Choice(['index', 'query'], case_sensitive=False),
)
@click.option('--num_docs', '-n', default=MAX_DOCS)
@click.option('--top_k', '-k', default=20000)
# @click.option('--dataset', '-d', type=click.Choice(['toy', 'full']), default='toy')
@click.option('--dataset', '-d', type=click.Choice(['full']), default='full')


def main(task, num_docs, top_k, dataset):
    config(dataset)
    if task == 'index':
        if os.path.exists(os.environ.get("JINA_WORKSPACE")):
            logger.error(f'\n +---------------------------------------------------------------------------------+ \
                    \n |                                   🤖🤖🤖                                        | \
                    \n | The directory {os.environ.get("JINA_WORKSPACE")} already exists. Please remove it before indexing again. | \
                    \n |                                   🤖🤖🤖                                        | \
                    \n +---------------------------------------------------------------------------------+')
            sys.exit(1)
        index(num_docs)
    elif task == 'query':
        query(top_k)

if __name__ == '__main__':
    main()
